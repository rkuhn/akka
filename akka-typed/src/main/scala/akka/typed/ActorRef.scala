/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import akka.actor.ActorPath
import scala.annotation.unchecked.uncheckedVariance

class ActorRef[-T](val ref: akka.actor.ActorRef) extends java.lang.Comparable[ActorRef[_]] {
  def !(msg: T): Unit = ref ! msg
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
