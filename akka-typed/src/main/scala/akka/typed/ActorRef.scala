/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import akka.actor.ActorPath

class ActorRef[-T](val ref: akka.actor.ActorRef) {
  def !(msg: T): Unit = ref ! msg
  def tell(msg: T): Unit = ref ! msg

  def path: ActorPath = ref.path

  override def toString = ref.toString
  override def equals(other: Any) = other match {
    case a: ActorRef[_] ⇒ a.ref == ref
    case _              ⇒ false
  }
  override def hashCode = ref.hashCode
}