package akka.stream.scaladsl

import akka.stream.testkit.AkkaSpec

class FlowToStringSpec extends AkkaSpec {

  "Linear layouts" must {

    "have a nice toString for simple flows" in {
      Flow[Int].map(x ⇒ x).filter(_ != 0).toString should be(
        "Flow[~> map ~> filter ~>]")
    }

    "have a nice toString for flows with sections" in {
      Flow[Int].map(x ⇒ x).section(OperationAttributes.name("mySection"))(
        _.map(_ + 1).grouped(500).mapConcat(identity)).filter(_ != 0).toString should be(
          "Flow[~> map ~> [mySection] ~> filter ~>]")
    }

    "have a nice toString for flows with unnamed sections" in {
      Flow[Int].map(x ⇒ x).section(OperationAttributes.none)(
        _.map(_ + 1).grouped(500).mapConcat(identity)).filter(_ != 0).toString should be(
          "Flow[~> map ~> [map ~> grouped ~> mapConcat] ~> filter ~>]")
    }

    "have a nice toString for simple sources" in {
      Source(0 :: Nil).map(x ⇒ x).filter(_ != 0).toString should be(
        "Source[[iterable] ~> map ~> filter ~>]")
    }

    "have a nice toString for sources with sections" in {
      Source(0 :: Nil).map(x ⇒ x).section(OperationAttributes.name("mySection"))(
        _.map(_ + 1).grouped(500).mapConcat(identity)).filter(_ != 0).toString should be(
          "Source[[iterable] ~> map ~> [mySection] ~> filter ~>]")
    }

    "have a nice toString for sources with unnamed sections" in {
      Source(0 :: Nil).map(x ⇒ x).section(OperationAttributes.none)(
        _.map(_ + 1).grouped(500).mapConcat(identity)).filter(_ != 0).toString should be(
          "Source[[iterable] ~> map ~> [map ~> grouped ~> mapConcat] ~> filter ~>]")
    }

    "have a nice toString for simple sinks" in {
      Flow[Int].map(x ⇒ x).filter(_ != 0).to(Sink.foreach(println)).toString should be(
        "Sink[~> map ~> filter ~> [foreach]]")
    }

    "have a nice toString for sinks with sections" in {
      Flow[Int].map(x ⇒ x).section(OperationAttributes.name("mySection"))(
        _.map(_ + 1).grouped(500).mapConcat(identity)).filter(_ != 0).to(Sink.fold(0)(_ + _)).toString should be(
          "Sink[~> map ~> [mySection] ~> filter ~> [fold]]")
    }

    "have a nice toString for sinks with unnamed sections" in {
      Flow[Int].map(x ⇒ x).section(OperationAttributes.none)(
        _.map(_ + 1).grouped(500).mapConcat(identity)).filter(_ != 0).to(Sink.onComplete(println)).toString should be(
          "Sink[~> map ~> [map ~> grouped ~> mapConcat] ~> filter ~> [onComplete]]")
    }

    "have a nice toString for simple runnable flows" in {
      Source(0 :: Nil).map(x ⇒ x).filter(_ != 0).to(Sink.foreach(println)).toString should be(
        "RunnableFlow[[iterable] ~> map ~> filter ~> [foreach]]")
    }

    "have a nice toString for runnable flows with sections" in {
      Source(0 :: Nil).map(x ⇒ x).section(OperationAttributes.name("mySection"))(
        _.map(_ + 1).grouped(500).mapConcat(identity)).filter(_ != 0).to(Sink.fold(0)(_ + _)).toString should be(
          "RunnableFlow[[iterable] ~> map ~> [mySection] ~> filter ~> [fold]]")
    }

    "have a nice toString for runnable flows with unnamed sections" in {
      Source(0 :: Nil).map(x ⇒ x).section(OperationAttributes.none)(
        _.map(_ + 1).grouped(500).mapConcat(identity)).filter(_ != 0).to(Sink.onComplete(println)).toString should be(
          "RunnableFlow[[iterable] ~> map ~> [map ~> grouped ~> mapConcat] ~> filter ~> [onComplete]]")
    }

  }

}
