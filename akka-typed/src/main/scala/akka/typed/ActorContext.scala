/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import scala.reflect.ClassTag
import akka.actor.ActorSystem
import scala.concurrent.duration.Duration

trait ActorContext[T] {

  def self: ActorRef[T]

  def props: Props[T]

  def system: ActorSystem

  def children: Iterable[ActorRef[Nothing]]

  def child(name: String): Option[ActorRef[Nothing]]

  def spawn[U](props: Props[U]): ActorRef[U]

  def spawn[U](props: Props[U], name: String): ActorRef[U]

  def stop(childName: String): Unit

  def watch[U](other: ActorRef[U]): ActorRef[U]

  def watch(other: akka.actor.ActorRef): other.type

  def unwatch[U](other: ActorRef[U]): ActorRef[U]

  def unwatch(other: akka.actor.ActorRef): other.type

  def setReceiveTimeout(d: Duration): Unit

  /**
   * Create a child actor that will wrap messages such that other Actor’s
   * protocols can be ingested by this Actor. You are strongly advised to cache
   * these ActorRefs or to stop them when no longer needed.
   */
  def createWrapper[U](f: U ⇒ T): ActorRef[U]
}

