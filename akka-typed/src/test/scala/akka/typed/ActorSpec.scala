/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import Behavior._
import Ops._
import akka.testkit.EventFilter
import scala.concurrent.duration._

class ActorSpec extends TypedSpec {

  /*
   * thoughts:
   * 
   *  - move test code fully into actors, with a helper that sends RunTest to the system
   *  - move Inbox registration down to actors as well
   *  - make new single-shot behavior that stops after receiving the first message
   */

  object `An Actor` {
    def `01 should receive PreStart and PostStop`(): Unit = sync(runTest("01")(Full[Unit] {
      case (ctx, Left(PreStart)) ⇒
        val i = Inbox.async[Signal](ctx, "inbox")
        val b = Full[Any] {
          case (_, Left(PreStart)) ⇒
            i.ref ! PreStart
            Stopped(() ⇒ i.ref ! PostStop)
        }
        val r = ctx.spawn(Props(b), "testee")
        i.watch(r)
        await(i.receiveMsg()) should be(PreStart)
        await(i.receiveMsg()) should be(PostStop)
        await(i.receiveTerminated()).ref should be(r)
        Stopped
    }))

    def `02 should receive PreRestart and PostRestart`(): Unit = sync(runTest("02")(
      StepWise[Signal] { (ctx, startWith) ⇒
        val b = Full[Exception] {
          case (_, Right(ex)) ⇒
            throw ex
          case (_, Left(s: PreRestart)) ⇒
            ctx.self ! s; Same
          case (_, Left(s: PostRestart)) ⇒
            ctx.self ! s; Stopped
        }

        startWith {
          val r = ctx.spawn(Props(b), "testee")
          ctx.watch(r)
          val ex = new Exception("BUH")
          r ! ex
          (r, ex)
        }.expectFailure(100.millis) {
          case (Failed(failure, child), (r, ex)) ⇒
            failure should be(ex)
            Failed.Restart -> ((r, ex))
        }.expectMessage(100.millis) {
          case (msg, (r, ex)) ⇒
            msg should be(PreRestart(ex))
            (r, ex)
        }.expectMessage(100.millis) {
          case (msg, (r, ex)) ⇒
            msg should be(PostRestart(ex))
            r
        }.expectTermination(100.millis) {
          case (Terminated(ref), r) ⇒
            ref should be(r)
        }
      }))
  }

}