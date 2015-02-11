/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream.FlowMaterializer

import akka.stream.testkit.AkkaSpec
import org.scalatest.concurrent.ScalaFutures._

class PublisherSinkSpec extends AkkaSpec {

  implicit val materializer = FlowMaterializer()

  "A PublisherSink" must {

    "be unique when created twice" in {
      val p1 = Sink.publisher[Int]
      val p2 = Sink.publisher[Int]

      val (pub1, pub2) = FlowGraph(Sink.publisher[Int], Sink.publisher[Int])(Pair.apply) { implicit b ⇒
        (p1, p2) ⇒
          import FlowGraph.Implicits._

          val bcast = Broadcast[Int](2)

          Source(0 to 5) ~> bcast.in
          bcast.out(0).map(_ * 2) ~> p1.inlet
          bcast.out(1) ~> p2.inlet
      }.run()

      Source(pub1).map(identity).runFold(0)(_ + _) should be(30)
      Source(pub2).map(identity).runFold(0)(_ + _) should be(15)

    }
  }

}
