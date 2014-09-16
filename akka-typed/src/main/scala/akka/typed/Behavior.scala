/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import akka.actor.OneForOneStrategy
import scala.annotation.tailrec

abstract class Behavior[T] {
  def management(ctx: ActorContext[T], msg: Signal): Behavior[T]
  def message(ctx: ActorContext[T], msg: T): Behavior[T]
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
    case Resume(b)   ⇒ unwrap(b)
    case Restart(b)  ⇒ unwrap(b)
    case Stop(b)     ⇒ unwrap(b)
    case Escalate(b) ⇒ unwrap(b)
    case _           ⇒ b
  }
}

object Behavior {

  case class Full[T](behavior: PartialFunction[(ActorContext[T], Either[Signal, T]), Behavior[T]]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = {
      lazy val fallback: ((ActorContext[T], Either[Signal, T])) ⇒ Behavior[T] = _ ⇒
        msg match {
          case PreRestart(_) ⇒
            ctx.children foreach { child ⇒
              ctx.unwatch(child.ref)
              ctx.stop(child.path.name)
            }
            behavior.applyOrElse((ctx, Left(PostStop)), fallback)
          case PostRestart(_) ⇒ behavior.applyOrElse((ctx, Left(PreStart)), fallback)
          case _              ⇒ Same
        }
      behavior.applyOrElse((ctx, Left(msg)), fallback)
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = {
      behavior.applyOrElse((ctx, Right(msg)), (_: (ActorContext[T], Either[Signal, T])) ⇒ Same)
    }
  }

  case class Simple[T](behavior: T ⇒ Behavior[T]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = msg match {
      case _ ⇒ Same
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior(msg)
  }

  case class Static[T](behavior: T ⇒ Unit) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = msg match {
      case _ ⇒ Same
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = {
      behavior(msg)
      this
    }
  }

  case class Contextual[T](behavior: (ActorContext[T], T) ⇒ Behavior[T]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = msg match {
      case _ ⇒ Same
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior(ctx, msg)
  }

  case class Composite[T](mgmt: PartialFunction[(ActorContext[T], Signal), Behavior[T]], behavior: (ActorContext[T], T) ⇒ Behavior[T]) extends Behavior[T] {
    override def management(ctx: ActorContext[T], msg: Signal): Behavior[T] = {
      lazy val fallback: ((ActorContext[T], Signal)) ⇒ Behavior[T] = _ ⇒
        msg match {
          case PreRestart(_) ⇒
            ctx.children foreach { child ⇒
              ctx.unwatch(child.ref)
              ctx.stop(child.path.name)
            }
            mgmt.applyOrElse((ctx, PostStop), fallback)
          case PostRestart(_) ⇒ mgmt.applyOrElse((ctx, PreStart), fallback)
          case _              ⇒ Same
        }
      mgmt.applyOrElse((ctx, msg), fallback)
    }
    override def message(ctx: ActorContext[T], msg: T): Behavior[T] = behavior(ctx, msg)
  }

  case class Selective[T](timeout: FiniteDuration, selector: PartialFunction[T, Behavior[T]], onTimeout: () ⇒ Behavior[T]) // TODO

  def Same[T]: Behavior[T] = sameBehavior.asInstanceOf[Behavior[T]]
  def Stopped[T]: Behavior[T] = stoppedBehavior.asInstanceOf[Behavior[T]]

  private[akka] object sameBehavior extends Behavior[Nothing] {
    override def management(ctx: ActorContext[Nothing], msg: Signal): Behavior[Nothing] = ???
    override def message(ctx: ActorContext[Nothing], msg: Nothing): Behavior[Nothing] = ???
  }

  private[akka] object stoppedBehavior extends Behavior[Nothing] {
    override def management(ctx: ActorContext[Nothing], msg: Signal): Behavior[Nothing] = ???
    override def message(ctx: ActorContext[Nothing], msg: Nothing): Behavior[Nothing] = ???
  }

}

