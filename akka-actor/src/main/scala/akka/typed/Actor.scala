package akka.typed

sealed trait Management
case object PreStart extends Management
case class PreRestart(failure: Throwable) extends Management
case class PostRestart(failure: Throwable) extends Management
case object PostStop extends Management
case class Failure(cause: Throwable, child: ActorRef[Nothing]) extends Management

trait Actor[T] {

  sealed trait Behavior {
    def management(ctx: ActorContext[T], msg: Management): Behavior
    def message(ctx: ActorContext[T], msg: T): Behavior
  }

  case class FullBehavior(behavior: PartialFunction[(ActorContext[T], Either[Management, T]), Behavior]) extends Behavior {
    override def management(ctx: ActorContext[T], msg: Management): Behavior = {
      lazy val fallback: ((ActorContext[T], Either[Management, T])) => Behavior = _ =>
        msg match {
          case PreStart => SameBehavior
          case PostStop => SameBehavior
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
    override def message(ctx: ActorContext[T], msg: T): Behavior = {
      behavior.applyOrElse((ctx, Right(msg)), (_: (ActorContext[T], Either[Management, T])) => SameBehavior)
    }
  }

  case class SimpleBehavior(behavior: T => Behavior) extends Behavior {
    override def management(ctx: ActorContext[T], msg: Management): Behavior = msg match {
      case Failure(cause, _) => throw cause
      case _                 => SameBehavior
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior = behavior(msg)
  }

  case class ContextualBehavior(behavior: (ActorContext[T], T) => Behavior) extends Behavior {
    override def management(ctx: ActorContext[T], msg: Management): Behavior = msg match {
      case Failure(cause, _) => throw cause
      case _                 => SameBehavior
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior = behavior(ctx, msg)
  }

  case class CompositeBehavior(mgmt: PartialFunction[(ActorContext[T], Management), Behavior], behavior: (ActorContext[T], T) => Behavior) extends Behavior {
    override def management(ctx: ActorContext[T], msg: Management): Behavior = {
      lazy val fallback: ((ActorContext[T], Management)) => Behavior = _ =>
        msg match {
          case PreStart => SameBehavior
          case PostStop => SameBehavior
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
    override def message(ctx: ActorContext[T], msg: T): Behavior = behavior(ctx, msg)
  }

  case object SameBehavior extends Behavior {
    override def management(ctx: ActorContext[T], msg: Management): Behavior = ???
    override def message(ctx: ActorContext[T], msg: T): Behavior = ???
  }
  
  case object StoppedBehavior extends Behavior {
    override def management(ctx: ActorContext[T], msg: Management): Behavior = ???
    override def message(ctx: ActorContext[T], msg: T): Behavior = ???
  }

  def initialBehavior: Behavior

}