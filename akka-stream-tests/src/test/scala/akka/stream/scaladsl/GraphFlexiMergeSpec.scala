package akka.stream.scaladsl

import akka.stream.FlowMaterializer
import akka.stream.scaladsl.FlexiMerge.{ Read, ReadAll, ReadAllInputs, ReadAny }
import akka.stream.scaladsl.FlowGraph.FlowGraphBuilder
import akka.stream.scaladsl.FlowGraph.Implicits._
import akka.stream.scaladsl.Graphs.InPort
import akka.stream.testkit.AkkaSpec
import akka.stream.testkit.StreamTestKit.{ PublisherProbe, AutoPublisher, OnNext, SubscriberProbe }
import org.reactivestreams.Publisher

import scala.util.control.NoStackTrace

object GraphFlexiMergeSpec {

  object Fair {

    final case class Ports[T]() extends FlexiPorts[T] {
      override val namePrefix = "Fair"

      val input1 = createInputPort[T]()
      val input2 = createInputPort[T]()
    }

    final case class Logic[T](override val ports: Ports[T]) extends FlexiMerge[T, Ports[T]](ports) {
      def initialState = State[T](ReadAny(ports.input1, ports.input2)) { (ctx, input, element) ⇒
        ctx.emit(element)
        SameState
      }
    }

    def apply[T]()(implicit b: FlowGraphBuilder) = FlexiMerge[T, Ports[T]](Ports())(Logic.apply)
  }

  object StrictRoundRobin {

    final case class Ports[T]() extends FlexiPorts[T] {
      val input1 = createInputPort[T]()
      val input2 = createInputPort[T]()
    }

    /**
     * It never skips an input while cycling but waits on it instead (closed inputs are skipped though).
     * The fair merge above is a non-strict round-robin (skips currently unavailable inputs).
     */
    final case class Logic[T](override val ports: Ports[T]) extends FlexiMerge[T, Ports[T]](ports) {
      import akka.stream.scaladsl.FlexiMerge._
      import ports._

      val emitOtherOnClose = CompletionHandling(
        onComplete = { (ctx, input) ⇒
          ctx.changeCompletionHandling(defaultCompletionHandling)
          readRemaining(other(input))
        },
        onError = { (ctx, _, cause) ⇒
          ctx.error(cause)
          SameState
        })

      def other(input: InPort[_]): InPort[_] = if (input eq input1) input2 else input1

      val read1: State[T] = State[T](Read(input1)) { (ctx, input, element) ⇒
        ctx.emit(element)
        read2
      }

      val read2 = State[T](Read(input2)) { (ctx, input, element) ⇒
        ctx.emit(element)
        read1
      }

      def readRemaining(input: InPort[_]) = State[T](Read(input)) { (ctx, input, element) ⇒
        ctx.emit(element)
        SameState
      }

      override def initialState = read1

      override def initialCompletionHandling = emitOtherOnClose
    }

    def apply[T]()(implicit b: FlowGraphBuilder): Ports[T] = FlexiMerge[T, Ports[T]](Ports())(Logic.apply)

  }

  object MyZip {

    final case class Ports[A, B]() extends FlexiPorts[(A, B)] {
      val input1 = createInputPort[A]()
      val input2 = createInputPort[B]()
    }

    final case class Logic[A, B](override val ports: Ports[A, B]) extends FlexiMerge[(A, B), Ports[A, B]](ports) {
      import ports._

      var lastInA: A = _

      override def inputHandles(inputCount: Int) = {
        require(inputCount == 2, s"Zip must have two connected inputs, was $inputCount")
        Vector(input1, input2)
      }

      val readA: State[A] = State[A](Read(input1)) { (ctx, input, element) ⇒
        lastInA = element
        readB
      }

      val readB: State[B] = State[B](Read(input2)) { (ctx, input, element) ⇒
        ctx.emit((lastInA, element))
        readA
      }

      override def initialCompletionHandling = eagerClose

      override def initialState: State[_] = readA
    }

    def apply[A, B]()(implicit b: FlowGraphBuilder): Ports[A, B] = FlexiMerge[(A, B), Ports[A, B]](Ports())(Logic.apply)

  }

  object TripleCancellingZip {

    final case class Ports[A, B, C]() extends FlexiPorts[(A, B, C)] {
      val soonCancelledInput = createInputPort[A]()
      val stableInput1 = createInputPort[B]()
      val stableInput2 = createInputPort[C]()
    }

    final case class Logic[A, B, C](override val ports: Ports[A, B, C], var cancelAfter: Int = Int.MaxValue) extends FlexiMerge[(A, B, C), Ports[A, B, C]](ports) {
      import ports._

      override def inputHandles(inputCount: Int) = {
        require(inputCount == 3, s"TripleZip must have 3 connected inputs, was $inputCount")
        Vector(soonCancelledInput, stableInput1, stableInput2)
      }

      override def initialState = State[ReadAllInputs](ReadAll(soonCancelledInput, stableInput1, stableInput2)) {
        case (ctx, input, inputs) ⇒
          val a = inputs.getOrElse(soonCancelledInput, null)
          val b = inputs.getOrElse(stableInput1, null)
          val c = inputs.getOrElse(stableInput2, null)

          ctx.emit((a, b, c))
          if (cancelAfter == 0)
            ctx.cancel(soonCancelledInput)
          cancelAfter -= 1

          SameState
      }

      override def initialCompletionHandling = eagerClose
    }

    def apply[A, B, C](cancelAfter: Int = Int.MaxValue)(implicit b: FlowGraphBuilder): Ports[A, B, C] =
      FlexiMerge[(A, B, C), Ports[A, B, C]](Ports())(Logic(_, cancelAfter))

  }

  object OrderedMerge {

    final case class Ports() extends FlexiPorts {
      val input1 = createInputPort[Int]()
      val input2 = createInputPort[Int]()
    }

    final case class Logic(override val ports: Ports) extends FlexiMerge[Int, Ports](ports) {
      import akka.stream.scaladsl.FlexiMerge._
      import ports._

      private var reference = 0

      val emitOtherOnClose = CompletionHandling(
        onComplete = { (ctx, input) ⇒
          ctx.changeCompletionHandling(emitLast)
          readRemaining(other(input))
        },
        onError = { (ctx, input, cause) ⇒
          ctx.error(cause)
          SameState
        })

      def other(input: InPort[_]): InPort[_] = if (input eq input1) input2 else input1

      def getFirstElement = State[Int](ReadAny(input1, input2)) { (ctx, input, element) ⇒
        reference = element
        ctx.changeCompletionHandling(emitOtherOnClose)
        readUntilLarger(other(input))
      }

      def readUntilLarger(input: InPort[_]): State[Int] = State[Int](Read(input)) {
        (ctx, input, element) ⇒
          if (element <= reference) {
            ctx.emit(element)
            SameState
          } else {
            ctx.emit(reference)
            reference = element
            readUntilLarger(other(input))
          }
      }

      def readRemaining(input: InPort[_]) = State[Int](Read(input)) {
        (ctx, input, element) ⇒
          if (element <= reference)
            ctx.emit(element)
          else {
            ctx.emit(reference)
            reference = element
          }
          SameState
      }

      val emitLast = CompletionHandling(
        onComplete = { (ctx, input) ⇒
          if (ctx.isDemandAvailable)
            ctx.emit(reference)
          SameState
        },
        onError = { (ctx, input, cause) ⇒
          ctx.error(cause)
          SameState
        })

      override def initialState = getFirstElement
    }

    def apply()(implicit b: FlowGraphBuilder): Ports = FlexiMerge[Int, Ports](Ports())(Logic.apply)
  }

  object PreferringMerge {

    final case class Ports() extends FlexiPorts[Int] {
      val preferred = createInputPort[Int]()
      val secondary1 = createInputPort[Int]()
      val secondary2 = createInputPort[Int]()
    }

    final case class Logic(override val ports: Ports) extends FlexiMerge[Int, Ports](ports) {
      import akka.stream.scaladsl.FlexiMerge._
      import ports._

      override def initialState = State[Int](ReadPreferred(preferred)(secondary1, secondary2)) {
        (ctx, input, element) ⇒
          ctx.emit(element)
          SameState
      }
    }

    def apply()(implicit b: FlowGraphBuilder): Ports = FlexiMerge[Int, Ports](Ports())(Logic.apply)
  }

  object TestMerge {

    final case class Ports() extends FlexiPorts[String] {
      override val namePrefix = "TestMerge"
      val input1 = createInputPort[String]()
      val input2 = createInputPort[String]()
      val input3 = createInputPort[String]()
    }

    final case class Logic(override val ports: Ports) extends FlexiMerge[String, Ports](ports) {

      import akka.stream.scaladsl.FlexiMerge._

      var throwFromOnComplete = false

      override def initialState = State[String](ReadAny(ports.inlets)) {
        (ctx, input, element) ⇒
          if (element == "cancel")
            ctx.cancel(input)
          else if (element == "err")
            ctx.error(new RuntimeException("err") with NoStackTrace)
          else if (element == "exc")
            throw new RuntimeException("exc") with NoStackTrace
          else if (element == "complete")
            ctx.complete()
          else if (element == "onComplete-exc")
            throwFromOnComplete = true
          else
            ctx.emit("onInput: " + element)

          SameState
      }

      override def initialCompletionHandling = CompletionHandling(
        onComplete = { (ctx, input) ⇒
          if (throwFromOnComplete)
            throw new RuntimeException("onComplete-exc") with NoStackTrace
          if (ctx.isDemandAvailable)
            ctx.emit("onComplete: " + input)
          SameState
        },
        onError = { (ctx, input, cause) ⇒
          cause match {
            case _: IllegalArgumentException ⇒ // swallow
            case _                           ⇒ ctx.error(cause)
          }
          SameState
        })
    }

    def apply()(implicit b: FlowGraphBuilder): Ports = FlexiMerge[String, Ports](Ports())(Logic.apply)
  }

}

class GraphFlexiMergeSpec extends AkkaSpec {
  import akka.stream.scaladsl.GraphFlexiMergeSpec._

  implicit val materializer = FlowMaterializer()

  val in1 = Source(List("a", "b", "c", "d"))
  val in2 = Source(List("e", "f"))

  val out = Sink.publisher[String]

  "FlexiMerge" must {

    "build simple fair merge" in {
      val p = FlowGraph[Publisher[String]](out) { implicit b ⇒
        o ⇒
          val merge = Fair[String]()

          in1 ~> merge.input1
          in2 ~> merge.input2
          merge.out ~> o.inlet
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(10)
      (s.probe.receiveN(6).map { case OnNext(elem) ⇒ elem }).toSet should be(
        Set("a", "b", "c", "d", "e", "f"))
      s.expectComplete()
    }

    "be able to have two fleximerges in a graph" in {
      val p = FlowGraph(in1, in2, out)((i1, i2, o) ⇒ o) { implicit b ⇒
        (in1, in2, o) ⇒
          val m1 = Fair[String]() // TODO how to improve inference here?
          val m2 = Fair[String]()

          // format: OFF
          in1.outlet ~> m1.input1 // TODO in1 ~> m1.input would be nice to have
          in2.outlet ~> m1.input2

          Source(List("A", "B", "C", "D", "E", "F")) ~> m2.input1
                                              m1.out ~> m2.input2
                                                        m2.out ~> o.inlet
        // format: ON
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(20)
      (s.probe.receiveN(12).map { case OnNext(elem) ⇒ elem }).toSet should be(
        Set("a", "b", "c", "d", "e", "f", "A", "B", "C", "D", "E", "F"))
      s.expectComplete()
    }

    "allow reuse" in {
      val flow = Flow() { implicit b ⇒
        val merge = Fair[String]()

        Source(() ⇒ Iterator.continually("+")) ~> merge.input1

        merge.input2 → merge.out
      }

      val g = FlowGraph(out) { implicit b ⇒
        o ⇒
          val zip = Zip[String, String]
          in1 ~> flow ~> Flow[String].map { of ⇒ of } ~> zip.left
          in2 ~> flow ~> Flow[String].map { tf ⇒ tf } ~> zip.right
          zip.out.map { x ⇒ x.toString } ~> o.inlet
      }

      val p = g.run()
      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(3)
      val received = (s.probe.receiveN(3).map { case OnNext(elem) ⇒ elem }).toSet
      received should contain("(a,e)")
      received should contain("(c,+)")
      received should contain("(b,f)")
      s.expectComplete()
    }

    "allow zip reuse" in {
      val flow = Flow() { implicit b ⇒
        val merge = MyZip[String, String]()

        Source(() ⇒ Iterator.continually("+")) ~> merge.input1

        (merge.input2, merge.out)
      }

      val g = FlowGraph(out) { implicit b ⇒
        o ⇒
          val zip = Zip[String, String]

          in1 ~> flow ~> Flow[(String, String)].map(_.toString()) ~> zip.left
          in2 ~> zip.right

          zip.out.map(_.toString()) ~> o.inlet
      }

      val p = g.run()
      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(100)
      (s.probe.receiveN(2).map { case OnNext(elem) ⇒ elem }).toSet should be(Set("((+,b),f)", "((+,a),e)"))
      s.expectComplete()
    }

    "build simple round robin merge" in {
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = StrictRoundRobin[String]()
          in1 ~> merge.input1
          in2 ~> merge.input2
          merge.out ~> o.inlet
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext("a")
      s.expectNext("e")
      s.expectNext("b")
      s.expectNext("f")
      s.expectNext("c")
      s.expectNext("d")
      s.expectComplete()
    }

    "build simple zip merge" in {
      val p = FlowGraph(Sink.publisher[(Int, String)]) { implicit b ⇒
        o ⇒
          val merge = MyZip[Int, String]()
          Source(List(1, 2, 3, 4)) ~> merge.input1
          Source(List("a", "b", "c")) ~> merge.input2
          merge.out ~> o.inlet
      }.run()

      val s = SubscriberProbe[(Int, String)]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext(1 -> "a")
      s.expectNext(2 -> "b")
      s.expectNext(3 -> "c")
      s.expectComplete()
    }
    "build simple triple-zip merge using ReadAll" in {
      val p = FlowGraph(Sink.publisher[(Long, Int, String)]) { implicit b ⇒
        o ⇒
          val merge = TripleCancellingZip[Long, Int, String]()
        // format: OFF
        Source(List(1L,   2L       )) ~> merge.soonCancelledInput
        Source(List(1,    2,   3, 4)) ~> merge.stableInput1
        Source(List("a", "b", "c"  )) ~> merge.stableInput2
        merge.out ~> o.inlet
        // format: ON
      }.run()

      val s = SubscriberProbe[(Long, Int, String)]
      p.subscribe(s)
      val sub = s.expectSubscription()

      sub.request(10)
      s.expectNext((1L, 1, "a"))
      s.expectNext((2L, 2, "b"))
      s.expectComplete()
    }
    "build simple triple-zip merge using ReadAll, and continue with provided value for cancelled input" in {
      val p = FlowGraph(Sink.publisher[(Long, Int, String)]) { implicit b ⇒
        o ⇒
          val merge = TripleCancellingZip[Long, Int, String]()
        // format: OFF
        Source(List(1L,   2L,  3L,  4L, 5L)) ~> merge.soonCancelledInput
        Source(List(1,    2,   3,   4     )) ~> merge.stableInput1
        Source(List("a", "b", "c"         )) ~> merge.stableInput2
        merge.out ~> o.inlet
        // format: ON
      }.run()

      val s = SubscriberProbe[(Long, Int, String)]
      p.subscribe(s)
      val sub = s.expectSubscription()

      sub.request(10)
      s.expectNext((1L, 1, "a"))
      s.expectNext((2L, 2, "b"))
      // soonCancelledInput is now cancelled and continues with default (null) value
      s.expectNext((null.asInstanceOf[Long], 3, "c"))
      s.expectComplete()
    }

    "build simple ordered merge 1" in {
      val p = FlowGraph(Sink.publisher[Int]) { implicit b ⇒
        o ⇒
          val merge = OrderedMerge()
          Source(List(3, 5, 6, 7, 8)) ~> merge.input1
          Source(List(1, 2, 4, 9)) ~> merge.input2
          merge.out ~> o.inlet
      }.run()

      val s = SubscriberProbe[Int]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(100)
      for (n ← 1 to 9) {
        s.expectNext(n)
      }
      s.expectComplete()
    }

    "build simple ordered merge 2" in {
      val output = Sink.publisher[Int]

      val p = FlowGraph(output) { implicit b ⇒
        o ⇒
          val merge = OrderedMerge()
          Source(List(3, 5, 6, 7, 8)) ~> merge.input1
          Source(List(3, 5, 6, 7, 8, 10)) ~> merge.input2
          merge.out ~> o.inlet
      }.run()

      val s = SubscriberProbe[Int]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(100)
      s.expectNext(3)
      s.expectNext(3)
      s.expectNext(5)
      s.expectNext(5)
      s.expectNext(6)
      s.expectNext(6)
      s.expectNext(7)
      s.expectNext(7)
      s.expectNext(8)
      s.expectNext(8)
      s.expectNext(10)
      s.expectComplete()
    }

    "build perferring merge" in {
      val output = Sink.publisher[Int]
      val p = FlowGraph(output) { implicit b ⇒
        o ⇒
          val merge = PreferringMerge.Ports()
          Source(List(1, 2, 3)) ~> merge.preferred
          Source(List(11, 12, 13)) ~> merge.secondary1
          Source(List(14, 15, 16)) ~> merge.secondary2
          merge.out ~> output
      }.run()

      val s = SubscriberProbe[Int]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(100)
      s.expectNext(1)
      s.expectNext(2)
      s.expectNext(3)
      val secondaries = s.expectNext() ::
        s.expectNext() ::
        s.expectNext() ::
        s.expectNext() ::
        s.expectNext() ::
        s.expectNext() :: Nil

      secondaries.toSet should equal(Set(11, 12, 13, 14, 15, 16))
      s.expectComplete()
    }
    "build perferring merge, manually driven" in {
      val output = Sink.publisher[Int]
      val preferredDriver = PublisherProbe[Int]()
      val otherDriver1 = PublisherProbe[Int]()
      val otherDriver2 = PublisherProbe[Int]()

      val p = FlowGraph(output) { implicit b ⇒
        o ⇒
          val merge = PreferringMerge()
          Source(preferredDriver) ~> merge.preferred
          Source(otherDriver1) ~> merge.secondary1
          Source(otherDriver2) ~> merge.secondary2
          merge.out ~> output
      }.run()

      val s = SubscriberProbe[Int]
      p.subscribe(s)

      val sub = s.expectSubscription()
      val p1 = preferredDriver.expectSubscription()
      val s1 = otherDriver1.expectSubscription()
      val s2 = otherDriver2.expectSubscription()

      // just consume the preferred
      p1.sendNext(1)
      sub.request(1)
      s.expectNext(1)

      // pick preferred over any of the secondaries
      p1.sendNext(2)
      s1.sendNext(10)
      s2.sendNext(20)
      sub.request(1)
      s.expectNext(2)

      sub.request(2)
      s.expectNext(10)
      s.expectNext(20)

      p1.sendComplete()

      // continue with just secondaries when preferred has completed
      s1.sendNext(11)
      s2.sendNext(21)
      sub.request(2)
      val d1 = s.expectNext()
      val d2 = s.expectNext()
      Set(d1, d2) should equal(Set(11, 21))

      // continue with just one secondary
      s1.sendComplete()
      s2.sendNext(4)
      sub.request(1)
      s.expectNext(4)
      s2.sendComplete()

      // complete when all inputs have completed
      s.expectComplete()
    }

    "support cancel of input" in {
      val publisher = PublisherProbe[String]
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source(publisher) ~> merge.input1
          Source(List("b", "c", "d")) ~> merge.input2
          Source(List("e", "f")) ~> merge.input3
          merge.out ~> o.inlet
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)

      val autoPublisher = new AutoPublisher(publisher)
      autoPublisher.sendNext("a")
      autoPublisher.sendNext("cancel")

      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext("onInput: a")
      s.expectNext("onInput: b")
      s.expectNext("onInput: e")
      s.expectNext("onInput: c")
      s.expectNext("onInput: f")
      s.expectNext("onComplete: 2")
      s.expectNext("onInput: d")
      s.expectNext("onComplete: 1")

      autoPublisher.sendNext("x")

      s.expectComplete()
    }

    "complete when all inputs cancelled" in {
      val publisher1 = PublisherProbe[String]
      val publisher2 = PublisherProbe[String]
      val publisher3 = PublisherProbe[String]
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source(publisher1) ~> merge.input1
          Source(publisher2) ~> merge.input2
          Source(publisher3) ~> merge.input3
          merge.out ~> o.inlet
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)

      val autoPublisher1 = new AutoPublisher(publisher1)
      autoPublisher1.sendNext("a")
      autoPublisher1.sendNext("cancel")

      val autoPublisher2 = new AutoPublisher(publisher2)
      autoPublisher2.sendNext("b")
      autoPublisher2.sendNext("cancel")

      val autoPublisher3 = new AutoPublisher(publisher3)
      autoPublisher3.sendNext("c")
      autoPublisher3.sendNext("cancel")

      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext("onInput: a")
      s.expectNext("onInput: b")
      s.expectNext("onInput: c")
      s.expectComplete()
    }

    "handle error" in {
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source.failed[String](new IllegalArgumentException("ERROR") with NoStackTrace) ~> merge.input1
          Source(List("a", "b")) ~> merge.input2
          Source(List("c")) ~> merge.input3
          merge.out ~> out
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(10)
      // IllegalArgumentException is swallowed by the CompletionHandler
      s.expectNext("onInput: a")
      s.expectNext("onInput: c")
      s.expectNext("onComplete: 2")
      s.expectNext("onInput: b")
      s.expectNext("onComplete: 1")
      s.expectComplete()
    }

    "propagate error" in {
      val publisher = PublisherProbe[String]
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source(publisher) ~> merge.input1
          Source.failed[String](new IllegalStateException("ERROR") with NoStackTrace) ~> merge.input2
          Source.empty[String] ~> merge.input3
          merge.out ~> out
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      s.expectErrorOrSubscriptionFollowedByError().getMessage should be("ERROR")
    }

    "emit error" in {
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source(List("a", "err")) ~> merge.input1
          Source(List("b", "c")) ~> merge.input2
          Source.empty[String] ~> merge.input3
          merge.out ~> out
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext("onInput: a")
      s.expectNext("onInput: b")
      s.expectError().getMessage should be("err")
    }

    "emit error for user thrown exception" in {
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source(List("a", "exc")) ~> merge.input1
          Source(List("b", "c")) ~> merge.input2
          Source.empty[String] ~> merge.input3
          merge.out ~> out
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext("onInput: a")
      s.expectNext("onInput: b")
      s.expectError().getMessage should be("exc")
    }

    "emit error for user thrown exception in onComplete" in {
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source(List("a", "onComplete-exc")) ~> merge.input1
          Source(List("b", "c")) ~> merge.input2
          Source.empty[String] ~> merge.input3
          merge.out ~> out
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext("onInput: a")
      s.expectNext("onInput: b")
      s.expectError().getMessage should be("onComplete-exc")
    }

    "emit error for user thrown exception in onComplete 2" in {
      val publisher = PublisherProbe[String]
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source.empty[String] ~> merge.input1
          Source(publisher) ~> merge.input2
          Source.empty[String] ~> merge.input3
          merge.out ~> out
      }.run()

      val autoPublisher = new AutoPublisher(publisher)
      autoPublisher.sendNext("onComplete-exc")
      autoPublisher.sendNext("a")

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(1)
      s.expectNext("onInput: a")

      autoPublisher.sendComplete()
      s.expectError().getMessage should be("onComplete-exc")
    }

    "support complete from onInput" in {
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source(List("a", "complete")) ~> merge.input1
          Source(List("b", "c")) ~> merge.input2
          Source.empty[String] ~> merge.input3
          merge.out ~> out
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext("onInput: a")
      s.expectNext("onInput: b")
      s.expectComplete()
    }

    "support unconnected inputs" in {
      val p = FlowGraph(out) { implicit b ⇒
        o ⇒
          val merge = TestMerge()
          Source(List("a")) ~> merge.input1
          Source(List("b", "c")) ~> merge.input2
          // input3 not connected
          merge.out ~> o.inlet // TODO I'd rather `merge.out ~> o`
      }.run()

      val s = SubscriberProbe[String]
      p.subscribe(s)
      val sub = s.expectSubscription()
      sub.request(10)
      s.expectNext("onInput: a")
      s.expectNext("onComplete: 0")
      s.expectNext("onInput: b")
      s.expectNext("onInput: c")
      s.expectNext("onComplete: 1")
      s.expectComplete()
    }

  }
}
