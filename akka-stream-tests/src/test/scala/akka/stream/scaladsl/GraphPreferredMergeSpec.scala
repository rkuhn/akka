/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream.scaladsl.FlowGraph.FlowGraphBuilder
import akka.stream.scaladsl.Graphs.{ OutPort, InPort }

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream.scaladsl.FlowGraph.Implicits._
import akka.stream.testkit.TwoStreamsSetup

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

    //    "prefer selected input more than others" in {
    //      val numElements = 10000
    //
    //      val preferred = Source(Stream.fill(numElements)(1))
    //      val aux1, aux2, aux3 = Source(Stream.fill(numElements)(2))
    //      val sink = Sink.head[Seq[Int]]
    //
    //      val g = FlowGraph { implicit b ⇒
    //        val merge = MergePreferred[Int](3)
    //        preferred ~> merge.preferred
    //
    //        merge.out.grouped(numElements * 2) ~> sink
    //        aux1 ~> merge.in(0)
    //        aux2 ~> merge.in(1)
    //        aux3 ~> merge.in(2)
    //      }.run()
    //
    //      Await.result(g.get(sink), 3.seconds).filter(_ == 1).size should be(numElements)
    //    }
    //
    //    "disallow multiple preferred inputs" in {
    //      val s1, s2, s3 = Source(0 to 3)
    //
    //      (the[IllegalArgumentException] thrownBy {
    //        val g = FlowGraph { implicit b ⇒
    //          val merge = MergePreferred[Int]
    //
    //          s1 ~> merge.preferred ~> Sink.head[Int]
    //          s2 ~> merge.preferred
    //          s3 ~> merge
    //        }
    //      }).getMessage should include("must have at most one preferred edge")
    //    }

  }

}
