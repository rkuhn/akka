package akka.typed

import scala.concurrent.duration._
import scala.concurrent.Future
import org.scalautils.ConversionCheckedTripleEquals

object ActorContextSpec {
  import Behavior._

  sealed trait Command
  sealed trait Event

  // TODO add DeadLetterSuppression
  case class GotSignal(signal: Signal) extends Event

  case class Ping(replyTo: ActorRef[Pong.type]) extends Command
  case object Pong extends Event

  case class Miss(replyTo: ActorRef[Missed.type]) extends Command
  case object Missed extends Event

  case class Renew(replyTo: ActorRef[Renewed.type]) extends Command
  case object Renewed extends Event

  case class Throw(ex: Exception) extends Command

  case class MkChild(name: Option[String], monitor: ActorRef[GotSignal], replyTo: ActorRef[Created]) extends Command
  case class Created(ref: ActorRef[Command]) extends Event

  case class SetTimeout(duration: FiniteDuration, replyTo: ActorRef[TimeoutSet.type]) extends Command
  case object TimeoutSet extends Event

  case class Schedule[T](delay: FiniteDuration, target: ActorRef[T], msg: T, replyTo: ActorRef[Scheduled.type]) extends Command
  case object Scheduled extends Event

  case object Stop extends Command

  case class Kill(name: String, replyTo: ActorRef[Killed.type]) extends Command
  case object Killed extends Event

  case class Watch(ref: ActorRef[Nothing], replyTo: ActorRef[Watched.type]) extends Command
  case object Watched extends Event

  case class Unwatch(ref: ActorRef[Nothing], replyTo: ActorRef[Unwatched.type]) extends Command
  case object Unwatched extends Event

  case class GetInfo(replyTo: ActorRef[Info]) extends Command
  case class Info(self: ActorRef[Command], props: Props[Command], system: ActorSystem[Nothing])

  case class GetChild(name: String, replyTo: ActorRef[Child]) extends Command
  case class Child(c: Option[ActorRef[Nothing]]) extends Event

  case class GetChildren(replyTo: ActorRef[Children]) extends Command
  case class Children(c: Set[ActorRef[Nothing]]) extends Event

  case class ChildEvent(event: Event) extends Event

  def subject(monitor: ActorRef[GotSignal]): Behavior[Command] =
    FullTotal((ctx, msg) ⇒ msg match {
      case Left(signal) ⇒
        monitor ! GotSignal(signal)
        if (signal.isInstanceOf[Failed]) ctx.setFailureResponse(Failed.Restart)
        Same
      case Right(message) ⇒ message match {
        case Ping(replyTo) ⇒
          replyTo ! Pong
          Same
        case Miss(replyTo) ⇒
          replyTo ! Missed
          Unhandled
        case Renew(replyTo) ⇒
          replyTo ! Renewed
          subject(monitor)
        case Throw(ex) ⇒
          throw ex
        case MkChild(name, mon, replyTo) ⇒
          val child = name match {
            case None    ⇒ ctx.spawn(Props(subject(mon)))
            case Some(n) ⇒ ctx.spawn(Props(subject(mon)), n)
          }
          replyTo ! Created(child)
          Same
        case SetTimeout(d, replyTo) ⇒
          ctx.setReceiveTimeout(d)
          replyTo ! TimeoutSet
          Same
        case Schedule(delay, target, msg, replyTo) ⇒
          ctx.schedule(delay, target, msg)
          replyTo ! Scheduled
          Same
        case Stop ⇒ Stopped
        case Kill(name, replyTo) ⇒
          ctx.stop(name)
          replyTo ! Killed
          Same
        case Watch(ref, replyTo) ⇒
          ctx.watch[Nothing](ref)
          replyTo ! Watched
          Same
        case Unwatch(ref, replyTo) ⇒
          ctx.unwatch[Nothing](ref)
          replyTo ! Unwatched
          Same
        case GetInfo(replyTo) ⇒
          replyTo ! Info(ctx.self, ctx.props, ctx.system)
          Same
        case GetChild(name, replyTo) ⇒
          replyTo ! Child(ctx.child(name))
          Same
        case GetChildren(replyTo) ⇒
          replyTo ! Children(ctx.children.toSet)
          Same
      }
    })
}

class ActorContextSpec extends TypedSpec with ConversionCheckedTripleEquals {
  import ActorContextSpec._

  def setup(name: String)(proc: (ActorContext[Event], StepWise.Steps[Event, ActorRef[Command]]) ⇒ StepWise.Steps[Event, _]): Future[TypedSpec.Status] =
    runTest(name)(StepWise[Event] { (ctx, startWith) ⇒
      val steps =
        startWith.withKeepTraces(true)(ctx.spawn(Props(subject(ctx.self)), "subject"))
          .expectMessage(500.millis) { (msg, ref) ⇒
            msg should ===(GotSignal(PreStart))
            ref
          }
      proc(ctx, steps)
    })

  object `An ActorCell` {
    def `01 must canonicalize behaviors`(): Unit =
      sync(setup("cell01") { (ctx, startWith) ⇒
        val self = ctx.self
        startWith.keep { subj ⇒
          subj ! Ping(self)
        }.expectMessageKeep(500.millis) { (msg, subj) ⇒
          msg should ===(Pong)
          subj ! Miss(self)
        }.expectMessageKeep(500.millis) { (msg, subj) ⇒
          msg should ===(Missed)
          subj ! Renew(self)
        }.expectMessage(500.millis) { (msg, subj) ⇒
          msg should ===(Renewed)
          subj ! Ping(self)
        }.expectMessage(500.millis) { (msg, _) ⇒
          msg should ===(Pong)
        }
      })
  }

  object `An ActorContext` {
    def `01 must correctly wire the lifecycle hooks`(): Unit = sync(setup("ctx01") { (ctx, startWith) ⇒
      val self = ctx.self
      val ex = new Exception("KABOOM")
      startWith { subj ⇒
        val log = muteExpectedException[Exception]("KABOOM", occurrences = 1)
        subj ! Throw(ex)
        (subj, log)
      }.expectFailureKeep(500.millis) {
        case (f, (subj, _)) ⇒
          f.cause should ===(ex)
          f.child should ===(subj)
          Failed.Restart
      }.expectMessage(500.millis) {
        case (msg, (subj, log)) ⇒
          msg should ===(GotSignal(PreRestart(ex)))
          log.assertDone(500.millis)
          subj
      }.expectMessage(500.millis) { (msg, subj) ⇒
        msg should ===(GotSignal(PostRestart(ex)))
        ctx.stop("subject")
      }.expectMessage(500.millis) { (msg, _) ⇒
        msg should ===(GotSignal(PostStop))
      }
    })
    
    def `02 must not signal PostStop after voluntary termination`(): Unit = sync(setup("ctx02") { (ctx, startWith) ⇒
      startWith.keep { subj ⇒
        ctx.watch(subj)
        subj ! Stop
      }.expectTermination(500.millis) { (t, subj) ⇒
        t.ref should ===(subj)
      }
    })
    
    def `03 must restart and stop a child actor`(): Unit = sync(setup("ctx03") { (ctx, startWith) ⇒
      val self = ctx.self
      val ex = new Exception("KABOOM")
      startWith.keep { subj ⇒
        subj ! MkChild(None, ctx.createWrapper { (e: Event) ⇒ ChildEvent(e) }, self)
      }.expectMultipleMessages(500.millis, 2) { (msgs, subj) ⇒
        val child = msgs match {
          case Created(child) :: ChildEvent(GotSignal(PreStart)) :: Nil ⇒ child
          case ChildEvent(GotSignal(PreStart)) :: Created(child) :: Nil ⇒ child
        }
        val log = muteExpectedException[Exception]("KABOOM", occurrences = 1)
        child ! Throw(ex)
        (subj, child, log)
      }.expectMultipleMessages(500.millis, 3) {
        case (msgs, (subj, child, log)) ⇒
          msgs should ===(
            GotSignal(Failed(`ex`, `child`)) ::
              ChildEvent(GotSignal(PreRestart(`ex`))) ::
              ChildEvent(GotSignal(PostRestart(`ex`))) :: Nil)
          log.assertDone(500.millis)
          subj ! Stop
          ctx.watch(child)
          ctx.watch(subj)
          (subj, child)
      }.expectMessageKeep(500.millis) { (msg, _) ⇒
        msg should ===(ChildEvent(GotSignal(PostStop)))
      }.expectTermination(500.millis) {
        case (t, (subj, child)) ⇒
          t.ref should ===(child)
          subj
      }.expectTermination(500.millis) {
        case (t, subj) ⇒
          t.ref should ===(subj)
      }
    })
  }

}