package akka.stream.scaladsl

import akka.stream.{ FlowMaterializer, MaterializerSettings }
import akka.stream.scaladsl.Graphs.FlowPorts
import akka.stream.testkit.AkkaSpec

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class GraphPartialSpec extends AkkaSpec {

  val settings = MaterializerSettings(system)
    .withInputBuffer(initialSize = 2, maxSize = 16)

  implicit val materializer = FlowMaterializer(settings)

  "FlowGraph.partial" must {
    import FlowGraph.Implicits._

    "be able to build and reuse simple partial graphs" in {
      val doubler = FlowGraph.partial { implicit b ⇒
        val bcast = Broadcast[Int](2)
        val zip = ZipWith2[Int, Int, Int]((a, b) ⇒ a + b)

        bcast.out(0) ~> zip.in1
        bcast.out(1) ~> zip.in2
        FlowPorts(bcast.in, zip.out)
      }

      val (_, _, result) = FlowGraph(doubler, doubler, Sink.head[Seq[Int]])(Tuple3.apply) { implicit b ⇒
        (d1, d2, sink) ⇒
          Source(List(1, 2, 3)) ~> d1.inlet
          d1.outlet ~> d2.inlet
          d2.outlet.grouped(100) ~> sink.inlet
      }.run()

      Await.result(result, 3.seconds) should be(List(4, 8, 12))
    }

    "be able to build and reuse simple materializing partial graphs" in {
      val doubler = FlowGraph.partial(Sink.head[Seq[Int]]) { implicit b ⇒
        sink ⇒
          val bcast = Broadcast[Int](3)
          val zip = ZipWith2[Int, Int, Int]((a, b) ⇒ a + b)

          bcast.out(0) ~> zip.in1
          bcast.out(1) ~> zip.in2
          bcast.out(2).grouped(100) ~> sink.inlet
          FlowPorts(bcast.in, zip.out)
      }

      val (sub1, sub2, result) = FlowGraph(doubler, doubler, Sink.head[Seq[Int]])(Tuple3.apply) { implicit b ⇒
        (d1, d2, sink) ⇒
          Source(List(1, 2, 3)) ~> d1.inlet
          d1.outlet ~> d2.inlet
          d2.outlet.grouped(100) ~> sink.inlet
      }.run()

      Await.result(result, 3.seconds) should be(List(4, 8, 12))
      Await.result(sub1, 3.seconds) should be(List(1, 2, 3))
      Await.result(sub2, 3.seconds) should be(List(2, 4, 6))
    }

    "be able to build and reuse complex materializing partial graphs" in {
      val summer = Sink.fold[Int, Int](0)(_ + _)

      val doubler = FlowGraph.partial(summer, summer)(Tuple2.apply) { implicit b ⇒
        (s1, s2) ⇒
          val bcast = Broadcast[Int](3)
          val bcast2 = Broadcast[Int](2)
          val zip = ZipWith2[Int, Int, Int]((a, b) ⇒ a + b)

          bcast.out(0) ~> zip.in1
          bcast.out(1) ~> zip.in2
          bcast.out(2) ~> s1.inlet

          zip.out ~> bcast2.in
          bcast2.out(0) ~> s2.inlet

          FlowPorts(bcast.in, bcast2.out(1))
      }

      val (sub1, sub2, result) = FlowGraph(doubler, doubler, Sink.head[Seq[Int]])(Tuple3.apply) { implicit b ⇒
        (d1, d2, sink) ⇒
          Source(List(1, 2, 3)) ~> d1.inlet
          d1.outlet ~> d2.inlet
          d2.outlet.grouped(100) ~> sink.inlet
      }.run()

      Await.result(result, 3.seconds) should be(List(4, 8, 12))
      Await.result(sub1._1, 3.seconds) should be(6)
      Await.result(sub1._2, 3.seconds) should be(12)
      Await.result(sub2._1, 3.seconds) should be(12)
      Await.result(sub2._2, 3.seconds) should be(24)
    }
  }

}
