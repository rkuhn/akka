package akka.stream.scaladsl

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._

import akka.stream.{ OverflowStrategy, MaterializerSettings }
import akka.stream.FlowMaterializer
import akka.stream.testkit.{ StreamTestKit, AkkaSpec }

class GraphBroadcastSpec extends AkkaSpec {

  val settings = MaterializerSettings(system)
    .withInputBuffer(initialSize = 2, maxSize = 16)

  implicit val materializer = FlowMaterializer(settings)

  "A broadcast" must {
    import FlowGraph.Implicits._

    "broadcast to other subscriber" in {
      val c1 = StreamTestKit.SubscriberProbe[Int]()
      val c2 = StreamTestKit.SubscriberProbe[Int]()

      FlowGraph { implicit b ⇒
        val bcast = Broadcast[Int](2)
        Source(List(1, 2, 3)) ~> bcast.in
        bcast.out(0) ~> Flow[Int].buffer(16, OverflowStrategy.backpressure) ~> Sink(c1)
        bcast.out(1) ~> Flow[Int].buffer(16, OverflowStrategy.backpressure) ~> Sink(c2)
      }.run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub1.request(1)
      sub2.request(2)
      c1.expectNext(1)
      c1.expectNoMsg(100.millis)
      c2.expectNext(1)
      c2.expectNext(2)
      c2.expectNoMsg(100.millis)
      sub1.request(3)
      c1.expectNext(2)
      c1.expectNext(3)
      c1.expectComplete()
      sub2.request(3)
      c2.expectNext(3)
      c2.expectComplete()
    }

    "work with n-way broadcast" in {
      val headSink = Sink.head[Seq[Int]]

      import system.dispatcher
      val result = FlowGraph(
        headSink,
        headSink,
        headSink,
        headSink,
        headSink)(
          (fut1, fut2, fut3, fut4, fut5) ⇒ Future.sequence(List(fut1, fut2, fut3, fut4, fut5))) { implicit b ⇒
            (p1, p2, p3, p4, p5) ⇒
              val bcast = Broadcast[Int](5)
              Source(List(1, 2, 3)) ~> bcast.in
              bcast.out(0).grouped(5) ~> p1.inlet
              bcast.out(1).grouped(5) ~> p2.inlet
              bcast.out(2).grouped(5) ~> p3.inlet
              bcast.out(3).grouped(5) ~> p4.inlet
              bcast.out(4).grouped(5) ~> p5.inlet
          }.run()

      Await.result(result, 3.seconds) should be(List.fill(5)(List(1, 2, 3)))
    }

    "produce to other even though downstream cancels" in {
      val c1 = StreamTestKit.SubscriberProbe[Int]()
      val c2 = StreamTestKit.SubscriberProbe[Int]()

      FlowGraph { implicit b ⇒
        val bcast = Broadcast[Int](2)
        Source(List(1, 2, 3)) ~> bcast.in
        bcast.out(0) ~> Flow[Int] ~> Sink(c1)
        bcast.out(1) ~> Flow[Int] ~> Sink(c2)
      }.run()

      val sub1 = c1.expectSubscription()
      sub1.cancel()
      val sub2 = c2.expectSubscription()
      sub2.request(3)
      c2.expectNext(1)
      c2.expectNext(2)
      c2.expectNext(3)
      c2.expectComplete()
    }

    "produce to downstream even though other cancels" in {
      val c1 = StreamTestKit.SubscriberProbe[Int]()
      val c2 = StreamTestKit.SubscriberProbe[Int]()

      FlowGraph { implicit b ⇒
        val bcast = Broadcast[Int](2)
        Source(List(1, 2, 3)) ~> bcast.in
        bcast.out(0) ~> Flow[Int] ~> Sink(c1)
        bcast.out(1) ~> Flow[Int] ~> Sink(c2)
      }.run()

      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub2.cancel()
      sub1.request(3)
      c1.expectNext(1)
      c1.expectNext(2)
      c1.expectNext(3)
      c1.expectComplete()
    }

    "cancel upstream when downstreams cancel" in {
      val p1 = StreamTestKit.PublisherProbe[Int]()
      val c1 = StreamTestKit.SubscriberProbe[Int]()
      val c2 = StreamTestKit.SubscriberProbe[Int]()

      FlowGraph { implicit b ⇒
        val bcast = Broadcast[Int](2)
        Source(p1.getPublisher) ~> bcast.in
        bcast.out(0) ~> Flow[Int] ~> Sink(c1)
        bcast.out(1) ~> Flow[Int] ~> Sink(c2)
      }.run()

      val bsub = p1.expectSubscription()
      val sub1 = c1.expectSubscription()
      val sub2 = c2.expectSubscription()
      sub1.request(3)
      sub2.request(3)
      p1.expectRequest(bsub, 16)
      bsub.sendNext(1)
      c1.expectNext(1)
      c2.expectNext(1)
      bsub.sendNext(2)
      c1.expectNext(2)
      c2.expectNext(2)
      sub1.cancel()
      sub2.cancel()
      bsub.expectCancellation()
    }

  }

}
