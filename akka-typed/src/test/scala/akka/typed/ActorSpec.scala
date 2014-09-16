package akka.typed

import Behavior._
import Ops._
import akka.testkit.EventFilter

class ActorSpec extends TypedSpec {

  object `An Actor` {
    def `should receive PreStart and PostStop`() {
      val i = Inbox.async[Signal]
      val b = Full[Any] {
        case (_, Left(PreStart)) ⇒
          i.ref ! PreStart; Stopped
        case (_, Left(PostStop)) ⇒
          i.ref ! PostStop; Same
      }
      val r = system.spawn(Props(b))
      i.watch(r)
      await(i.receiveMsg()) should be(PreStart)
      await(i.receiveMsg()) should be(PostStop)
      await(i.receiveTerminated()).ref should be(r)
    }

    def `should receive PreRestart and PostRestart`() {
      val i = Inbox.async[Signal]
      val b = Full[Exception] {
        case (_, Right(ex)) ⇒
          throw ex; Same
        case (_, Left(s: PreRestart)) ⇒
          i.ref ! s; Same
        case (_, Left(s: PostRestart)) ⇒
          i.ref ! s; Stopped
      }
      val r = system.spawn(Props(b))
      i.watch(r)
      val ex = new Exception("BUH")
      EventFilter[Exception](message = "BUH", occurrences = 1) intercept {
        r ! ex
      }
      await(i.receiveMsg()) should be(PreRestart(ex))
      await(i.receiveMsg()) should be(PostRestart(ex))
      await(i.receiveTerminated()).ref should be(r)
    }
  }

}