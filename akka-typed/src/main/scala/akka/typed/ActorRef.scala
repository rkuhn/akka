/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import akka.actor.ActorPath
import scala.annotation.unchecked.uncheckedVariance
import language.implicitConversions

abstract class ActorRef[-T] extends java.lang.Comparable[ActorRef[_]] { this: ScalaActorRef[T] ⇒
  def ref: akka.actor.ActorRef

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

trait ScalaActorRef[-T] { this: ActorRef[T] ⇒
  def !(msg: T): Unit = tell(msg)
}

object ActorRef {
  private class Combined[T](val ref: akka.actor.ActorRef) extends ActorRef[T] with ScalaActorRef[T]

  implicit def toScalaActorRef[T](ref: ActorRef[T]): ScalaActorRef[T] = ref.asInstanceOf[ScalaActorRef[T]]

  def apply[T](ref: akka.actor.ActorRef): ActorRef[T] = new Combined[T](ref)
}
