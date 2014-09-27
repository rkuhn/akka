/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import akka.actor.ActorPath
import scala.annotation.unchecked.uncheckedVariance
import language.implicitConversions

class ActorRef[-T] private (val ref: akka.actor.ActorRef) extends java.lang.Comparable[ActorRef[_]] { this: ScalaActorRef[T] ⇒
  def tell(msg: T): Unit = ref ! msg

  def upcast[U >: T @uncheckedVariance]: ActorRef[U] = this.asInstanceOf[ActorRef[U]]

  def path: ActorPath = ref.path

  override def toString = ref.toString
  override def equals(other: Any) = other match {
    case a: ActorRef[_] ⇒ a.ref == ref
    case _              ⇒ false
  }
  override def hashCode = ref.hashCode
  override def compareTo(other: ActorRef[_]) = ref.compareTo(other.ref)
}

sealed trait ScalaActorRef[-T] { this: ActorRef[T] ⇒
  def !(msg: T): Unit = tell(msg)
}

object ActorRef {
  private class Combined[T](_ref: akka.actor.ActorRef) extends ActorRef[T](_ref) with ScalaActorRef[T]

  implicit def toScalaActorRef[T](ref: ActorRef[T]): ScalaActorRef[T] = ref.asInstanceOf[ScalaActorRef[T]]

  def apply[T](ref: akka.actor.ActorRef): ActorRef[T] = new Combined[T](ref)
}
