package akka.typed

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import akka.actor.OneForOneStrategy
import scala.annotation.tailrec

trait Actor[T] {

  type Behavior = akka.typed.Behavior[T]

  def initialBehavior: Behavior

}

sealed trait Signal
case object PreStart extends Signal
case class PreRestart(failure: Throwable) extends Signal
case class PostRestart(failure: Throwable) extends Signal
case object PostStop extends Signal
case class Failure(cause: Throwable, child: ActorRef[Nothing]) extends Signal
case object ReceiveTimeout extends Signal
case class Terminated(ref: ActorRef[Nothing]) extends Signal

object Failure {
  case class Resume[T](behavior: Behavior[T]) extends Behavior[T] {
    def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = behavior.management(ctx, msg)
    def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior.message(ctx, msg)
  }
  case class Restart[T](behavior: Behavior[T]) extends Behavior[T] {
    def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = behavior.management(ctx, msg)
    def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior.message(ctx, msg)
  }
  case class Stop[T](behavior: Behavior[T]) extends Behavior[T] {
    def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = behavior.management(ctx, msg)
    def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior.message(ctx, msg)
  }
  case class Escalate[T](behavior: Behavior[T]) extends Behavior[T] {
    def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = behavior.management(ctx, msg)
    def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior.message(ctx, msg)
  }

  @tailrec def unwrap[T](b: Behavior[T]): Behavior[T] = b match {
    case Resume(b)   => unwrap(b)
    case Restart(b)  => unwrap(b)
    case Stop(b)     => unwrap(b)
    case Escalate(b) => unwrap(b)
    case _           => b
  }
}

abstract class Behavior[T] {
  def management(ctx: ActorContext[T], msg: Signal): Behavior[T]
  def message(ctx: ActorContext[T], msg: T): Behavior[T]
}

object Behavior {

  case class Full[T](behavior: PartialFunction[(ActorContext[T], Either[Signal, T]), Behavior[T]]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = {
      lazy val fallback: ((ActorContext[T], Either[Signal, T])) => Behavior[T] = _ =>
        msg match {
          case PreRestart(_) =>
            ctx.children foreach { child =>
              ctx.unwatch(child.ref)
              ctx.stop(child.path.name)
            }
            behavior.applyOrElse((ctx, Left(PostStop)), fallback)
          case PostRestart(_) => behavior.applyOrElse((ctx, Left(PreStart)), fallback)
          case _              => Same
        }
      behavior.applyOrElse((ctx, Left(msg)), fallback)
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = {
      behavior.applyOrElse((ctx, Right(msg)), (_: (ActorContext[T], Either[Signal, T])) => Same)
    }
  }

  case class Simple[T](behavior: T => Behavior[T]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = msg match {
      case _ => Same
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior(msg)
  }

  case class Contextual[T](behavior: (ActorContext[T], T) => Behavior[T]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = msg match {
      case _ => Same
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior(ctx, msg)
  }

  case class Composite[T](mgmt: PartialFunction[(ActorContext[T], Signal), Behavior[T]], behavior: (ActorContext[T], T) => Behavior[T]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = {
      lazy val fallback: ((ActorContext[T], Signal)) => Behavior[T] = _ =>
        msg match {
          case PreRestart(_) =>
            ctx.children foreach { child =>
              ctx.unwatch(child.ref)
              ctx.stop(child.path.name)
            }
            mgmt.applyOrElse((ctx, PostStop), fallback)
          case PostRestart(_) => mgmt.applyOrElse((ctx, PreStart), fallback)
          case _              => Same
        }
      mgmt.applyOrElse((ctx, msg), fallback)
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior(ctx, msg)
  }

  case class Selective[T](timeout: FiniteDuration, selector: PartialFunction[T, Behavior[T]], onTimeout: () => Behavior[T]) // TODO

  def Same[T]: Behavior[T] = sameBehavior.asInstanceOf[Behavior[T]]
  def Stopped[T]: Behavior[T] = stoppedBehavior.asInstanceOf[Behavior[T]]

  private[akka] val sameBehavior, stoppedBehavior = new Behavior[Nothing] {
    override def management(ctx: ActorContext[Nothing], msg: Signal): Behavior[Nothing] = ???
    override def message(ctx: ActorContext[Nothing], msg: Nothing): Behavior[Nothing] = ???
  }

}

private[typed] class ActorAdapter[T: ClassTag](val a: () => Actor[T]) extends akka.actor.Actor {
  import Behavior._

  val clazz = implicitly[ClassTag[T]].runtimeClass

  val actor = a()
  var behavior = actor.initialBehavior
  val ctx = new ActorContextAdapter[T](context)

  def receive = {
    case akka.actor.Terminated(ref)   => next(behavior.management(ctx, Terminated(new ActorRef(ref))))
    case akka.actor.ReceiveTimeout    => next(behavior.management(ctx, ReceiveTimeout))
    case msg if clazz.isInstance(msg) => next(behavior.message(ctx, msg.asInstanceOf[T]))
  }

  private def next(b: Behavior[T]): Unit =
    if (b == sameBehavior) {}
    else if (b == stoppedBehavior) {
      context.stop(self)
    } else behavior = b

  override val supervisorStrategy = OneForOneStrategy() {
    case ex =>
      import Failure._
      import akka.actor.{ SupervisorStrategy => s }
      val b = behavior.management(ctx, Failure(ex, new ActorRef(sender())))
      next(unwrap(b))
      b match {
        case Resume(_)  => s.Resume
        case Restart(_) => s.Restart
        case Stop(_)    => s.Stop
        case _          => s.Escalate
      }
  }

  override def preStart(): Unit =
    next(behavior.management(ctx, PreStart))
  override def preRestart(reason: Throwable, message: Option[Any]): Unit =
    next(behavior.management(ctx, PreRestart(reason)))
  override def postRestart(reason: Throwable): Unit =
    next(behavior.management(ctx, PreRestart(reason)))
  override def postStop(): Unit =
    next(behavior.management(ctx, PostStop))
}
