/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import akka.actor.ActorPath
import scala.annotation.unchecked.uncheckedVariance
import language.implicitConversions

/**
 * An ActorRef is the identity or address of an Actor instance. It is valid
 * only during the Actor’s lifetime and allows messages to be sent to that
 * Actor instance. Sending a message to an Actor that has terminated before
 * receiving the message will lead to that message being discarded; such
 * messages are delivered to the [[akka.actor.DeadLetter]] channel of the
 * [[akka.actor.ActorSystem!.eventStream EventStream]] on a best effort basis
 * (i.e. this delivery is not reliable).
 */
abstract class ActorRef[-T] extends java.lang.Comparable[ActorRef[_]] { this: ScalaActorRef[T] ⇒
  /**
   * Implementation detail. The underlying untyped [[akka.actor.ActorRef]]
   * of this typed ActorRef.
   */
  def ref: akka.actor.ActorRef

  /**
   * Send a message to the Actor referenced by this ActorRef using *at-most-once*
   * messaging semantics.
   */
  def tell(msg: T): Unit = ref ! msg

  /**
   * Unsafe utility method for widening the type accepted by this ActorRef;
   * provided to avoid having to use `asInstanceOf` on the full reference type,
   * which would unfortunately also work on non-ActorRefs.
   */
  def upcast[U >: T @uncheckedVariance]: ActorRef[U] = this.asInstanceOf[ActorRef[U]]

  /**
   * The hierarchical path name of the referenced Actor. The lifecycle of the
   * ActorRef is fully contained within the lifecycle of the [[akka.actor.ActorPath]]
   * and more than one Actor instance can exist with the same path at different
   * points in time, but not concurrently.
   */
  def path: ActorPath = ref.path

  override def toString = ref.toString
  override def equals(other: Any) = other match {
    case a: ActorRef[_] ⇒ a.ref == ref
    case _              ⇒ false
  }
  override def hashCode = ref.hashCode
  override def compareTo(other: ActorRef[_]) = ref.compareTo(other.ref)
}

/**
 * This trait is used to hide the `!` method from Java code.
 */
trait ScalaActorRef[-T] { this: ActorRef[T] ⇒
  def !(msg: T): Unit = tell(msg)
}

object ActorRef {
  private class Combined[T](val ref: akka.actor.ActorRef) extends ActorRef[T] with ScalaActorRef[T]

  implicit def toScalaActorRef[T](ref: ActorRef[T]): ScalaActorRef[T] = ref.asInstanceOf[ScalaActorRef[T]]

  /**
   * Construct a typed ActorRef from an untyped one and a protocol definition
   * (i.e. a recipient message type). This can be used to properly represent
   * untyped Actors within the typed world, given that they implement the assumed
   * protocol.
   */
  def apply[T](ref: akka.actor.ActorRef): ActorRef[T] = new Combined[T](ref)
}
