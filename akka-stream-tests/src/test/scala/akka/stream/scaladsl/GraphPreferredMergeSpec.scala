/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream.scaladsl.FlowGraph.FlowGraphBuilder
import akka.stream.scaladsl.Graphs.{ InPort, OutPort }
import akka.stream.testkit.TwoStreamsSetup

import scala.concurrent.Await
import scala.concurrent.duration._

class GraphPreferredMergeSpec extends TwoStreamsSetup {

  override type Outputs = Int

  override def fixture(b: FlowGraphBuilder): Fixture = new Fixture(b: FlowGraphBuilder) {
    val merge = MergePreferred[Outputs](1)(b)

    override def left: InPort[Outputs] = merge.preferred
    override def right: InPort[Outputs] = merge.in(0)
    override def out: OutPort[Outputs] = merge.out

  }

  "preferred merge" must {
    import akka.stream.scaladsl.FlowGraph.Implicits._

    commonTests()

    "prefer selected input more than others" in {
      val numElements = 10000

      val preferred = Source(Stream.fill(numElements)(1))
      val aux = Source(Stream.fill(numElements)(2))

      val result = FlowGraph(Sink.head[Seq[Int]]) { implicit b ⇒
        sink ⇒
          val merge = MergePreferred[Int](3)
          preferred ~> merge.preferred

          merge.out.grouped(numElements * 2) ~> sink.inlet
          aux ~> merge.in(0)
          aux ~> merge.in(1)
          aux ~> merge.in(2)
      }.run()

      Await.result(result, 3.seconds).filter(_ == 1).size should be(numElements)
    }

    //    "disallow multiple preferred inputs" in {
    //      val s = Source(0 to 3)
    //
    //      (the[IllegalArgumentException] thrownBy {
    //        val g = FlowGraph { implicit b ⇒
    //          val merge = MergePreferred[Int](1)
    //
    //          s ~> merge.preferred
    //          s ~> merge.preferred
    //          s ~> merge.in(0)
    //
    //          merge.out ~> Sink.head[Int]
    //        }
    //      }).getMessage should include("must have at most one preferred edge")
    //    }

  }

}
