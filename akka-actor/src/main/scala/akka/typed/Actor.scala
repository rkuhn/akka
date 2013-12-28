package akka.typed

sealed trait Management
case object PreStart extends Management
case class PreRestart(failure: Throwable) extends Management
case class PostRestart(failure: Throwable) extends Management
case object PostStop extends Management
case class Failure(cause: Throwable, child: ActorRef[Nothing]) extends Management

abstract class Behavior[T] {
  def management(ctx: ActorContext[T], msg: Management): Behavior[T]
  def message(ctx: ActorContext[T], msg: T): Behavior[T]
}

object Behavior {

  case class Full[T](behavior: PartialFunction[(ActorContext[T], Either[Management, T]), Behavior[T]]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Management): Behavior[T] = {
      lazy val fallback: ((ActorContext[T], Either[Management, T])) => Behavior[T] = _ =>
        msg match {
          case PreStart => Same
          case PostStop => Same
          case PreRestart(_) =>
            ctx.children foreach { child =>
              ctx.unwatch(child)
              ctx.stop(child.path.name)
            }
            behavior.applyOrElse((ctx, Left(PostStop)), fallback)
          case PostRestart(_)    => behavior.applyOrElse((ctx, Left(PreStart)), fallback)
          case Failure(cause, _) => throw cause
        }
      behavior.applyOrElse((ctx, Left(msg)), fallback)
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = {
      behavior.applyOrElse((ctx, Right(msg)), (_: (ActorContext[T], Either[Management, T])) => Same)
    }
  }

  case class Simple[T](behavior: T => Behavior[T]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Management): Behavior[T] = msg match {
      case Failure(cause, _) => throw cause
      case _                 => Same
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior(msg)
  }

  case class Contextual[T](behavior: (ActorContext[T], T) => Behavior[T]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Management): Behavior[T] = msg match {
      case Failure(cause, _) => throw cause
      case _                 => Same
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior(ctx, msg)
  }

  case class Composite[T](mgmt: PartialFunction[(ActorContext[T], Management), Behavior[T]], behavior: (ActorContext[T], T) => Behavior[T]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Management): Behavior[T] = {
      lazy val fallback: ((ActorContext[T], Management)) => Behavior[T] = _ =>
        msg match {
          case PreStart => Same
          case PostStop => Same
          case PreRestart(_) =>
            ctx.children foreach { child =>
              ctx.unwatch(child)
              ctx.stop(child.path.name)
            }
            mgmt.applyOrElse((ctx, PostStop), fallback)
          case PostRestart(_)    => mgmt.applyOrElse((ctx, PreStart), fallback)
          case Failure(cause, _) => throw cause
        }
      mgmt.applyOrElse((ctx, msg), fallback)
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior(ctx, msg)
  }

  def Same[T]: Behavior[T] = sameBehavior.asInstanceOf
  def Stopped[T]: Behavior[T] = stoppedBehavior.asInstanceOf

  private[akka] val sameBehavior, stoppedBehavior = new Behavior[Nothing] {
    override def management(ctx: ActorContext[Nothing], msg: Management): Behavior[Nothing] = ???
    override def message(ctx: ActorContext[Nothing], msg: Nothing): Behavior[Nothing] = ???
  }

}

trait Actor[T] {

  def initialBehavior: Behavior[T]

}