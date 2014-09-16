package akka.typed

import akka.actor.ActorSystem
import language.higherKinds
import scala.concurrent.duration.Duration
import scala.collection.immutable

object Pure {
  import akka.typed.{ ActorContext ⇒ AC }

  trait Monadic[T, U] {
    def flatMap[V](f: U ⇒ Monadic[T, V]): Monadic[T, V]
    def map[V](f: U ⇒ V): Monadic[T, V]
    def run(ctx: AC[T]): (immutable.Seq[Effect], U)
  }

  /**
   * Exploring going fully monadic on the effects; some of these (like self/props/system)
   * might be better modeled directly, not sure yet.
   */
  trait ActorContext[T] extends Any {

    def self: Monadic[T, ActorRef[T]]
    def props: Monadic[T, Props[T]]
    def system: Monadic[T, ActorSystem]
    def children: Monadic[T, Iterable[ActorRef[Nothing]]]
    def child(name: String): Monadic[T, Option[ActorRef[Nothing]]]
    def spawn[U](props: Props[U]): Monadic[T, ActorRef[U]]
    def spawn[U](props: Props[U], name: String): Monadic[T, Either[ActorRef[Nothing], ActorRef[U]]]
    def stop(childName: String): Monadic[T, Unit]
    def watch[U](other: ActorRef[U]): Monadic[T, ActorRef[U]]
    def unwatch[U](other: ActorRef[U]): Monadic[T, ActorRef[U]]
    def setReceiveTimeout(d: Duration): Monadic[T, Unit]

    def bind[U](v: AC[T] ⇒ U): Monadic[T, U]
    def unit[U](v: U): Monadic[T, U]
  }

  sealed trait Effect
  case class Spawned(childName: String) extends Effect
  case class Stopped(childName: String) extends Effect
  case class Watched[T](other: ActorRef[T]) extends Effect
  case class Unwatched[T](other: ActorRef[T]) extends Effect
  case class ReceiveTimeoutSet(d: Duration) extends Effect

  private class ActorContextImpl[T] extends ActorContext[T] {

    def self = bind(_.self)
    def props = bind(_.props)
    def system = bind(_.system)
    def children = bind(_.children)
    def child(name: String) = bind(_.child(name))
    def spawn[U](props: Props[U]) = bind(identity).flatMap { ctx ⇒
      val ref = ctx.spawn(props)
      bind(Spawned(ref.ref.path.name), _ ⇒ ref)
    }
    def spawn[U](props: Props[U], name: String) = bind(Spawned(name),
      ctx ⇒ ctx.child(name) match {
        case None      ⇒ Right(ctx.spawn(props, name))
        case Some(ref) ⇒ Left(ref)
      })
    def stop(childName: String) = bind(Stopped(childName), _.stop(childName))
    def watch[U](other: ActorRef[U]) = bind(Watched(other), _.watch(other))
    def unwatch[U](other: ActorRef[U]) = bind(Unwatched(other), _.unwatch(other))
    def setReceiveTimeout(d: Duration) = bind(ReceiveTimeoutSet(d), _.setReceiveTimeout(d))

    def unit[U](v: U): Monadic[T, U] = new MonadicImpl(Nil, _ ⇒ v)
    def bind[U](v: AC[T] ⇒ U): Monadic[T, U] = new MonadicImpl(Nil, v)
    def bind[U](effect: Effect, v: AC[T] ⇒ U): Monadic[T, U] = new MonadicImpl(effect :: Nil, v)
  }

  private class MonadicImpl[C, T](effects: immutable.Seq[Effect], value: AC[C] ⇒ T) extends Monadic[C, T] { self ⇒
    def flatMap[U](f: T ⇒ Monadic[C, U]): Monadic[C, U] = new DeferredMonadicImpl(this, f)
    def map[U](f: T ⇒ U) = new MonadicImpl(effects, value andThen f)
    def run(ctx: AC[C]): (immutable.Seq[Effect], T) = effects -> value(ctx)
  }

  private class DeferredMonadicImpl[C, T, U](predecessor: Monadic[C, T], f: T ⇒ Monadic[C, U]) extends Monadic[C, U] { self ⇒
    def flatMap[V](ff: U ⇒ Monadic[C, V]): Monadic[C, V] = new DeferredMonadicImpl(this, ff)
    def map[V](ff: U ⇒ V): Monadic[C, V] = new DeferredMonadicImpl(this, (t: U) ⇒ new MonadicImpl(Nil, _ ⇒ ff(t)))
    def run(ctx: AC[C]): (immutable.Seq[Effect], U) = predecessor.run(ctx) match {
      case (effects1, value1) ⇒ f(value1).run(ctx) match {
        case (effects2, value2) ⇒ (effects1 ++ effects2) -> value2
      }
    }
  }

  case class Behavior[T](f: (ActorContext[T], Either[Signal, T]) ⇒ Monadic[T, akka.typed.Behavior[T]]) extends akka.typed.Behavior[T] {
    private val ctxImpl = new ActorContextImpl[T]

    def management(ctx: AC[T], msg: Signal): akka.typed.Behavior[T] = execute(ctx, f(ctxImpl, Left(msg)))._2
    def message(ctx: AC[T], msg: T): akka.typed.Behavior[T] = execute(ctx, f(ctxImpl, Right(msg)))._2

    def run(ctx: AC[T], msg: Either[Signal, T]): (immutable.Seq[Effect], akka.typed.Behavior[T]) = execute(ctx, f(ctxImpl, msg))
    private def execute(ctx: AC[T], m: Monadic[T, akka.typed.Behavior[T]]): (immutable.Seq[Effect], akka.typed.Behavior[T]) =
      m.run(ctx) match {
        case (effects, behv) if behv == akka.typed.Behavior.sameBehavior ⇒ effects -> this
        case x ⇒ x
      }
  }

}
