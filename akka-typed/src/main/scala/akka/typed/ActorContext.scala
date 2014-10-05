/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import scala.concurrent.duration.Duration
import scala.collection.immutable
import scala.collection.immutable.TreeSet
import scala.collection.immutable.TreeMap
import akka.util.Helpers
import akka.{ actor ⇒ a }
import scala.reflect.ClassTag
import scala.reflect.classTag
import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContextExecutor
import akka.typed.Behavior.stoppedBehavior

trait ActorContext[T] {

  def self: ActorRef[T]

  def props: Props[T]

  def system: ActorSystem[Nothing]

  def children: Iterable[ActorRef[Nothing]]

  def child(name: String): Option[ActorRef[Nothing]]

  def spawn[U](props: Props[U]): ActorRef[U]

  def spawn[U](props: Props[U], name: String): ActorRef[U]

  def actorOf(props: a.Props): a.ActorRef

  def actorOf(props: a.Props, name: String): a.ActorRef

  def stop(childName: String): Unit

  def watch[U](other: ActorRef[U]): ActorRef[U]

  def watch(other: akka.actor.ActorRef): akka.actor.ActorRef

  def unwatch[U](other: ActorRef[U]): ActorRef[U]

  def unwatch(other: akka.actor.ActorRef): akka.actor.ActorRef

  def setReceiveTimeout(d: Duration): Unit

  def schedule[T](delay: FiniteDuration, target: ActorRef[T], msg: T): a.Cancellable

  implicit def executionContext: ExecutionContextExecutor

  /**
   * Create a child actor that will wrap messages such that other Actor’s
   * protocols can be ingested by this Actor. You are strongly advised to cache
   * these ActorRefs or to stop them when no longer needed.
   */
  def createWrapper[U](f: U ⇒ T): ActorRef[U]
}

class DummyActorContext[T](
  val name: String,
  override val props: Props[T],
  override implicit val system: ActorSystem[Nothing]) extends ActorContext[T] {

  val inbox = Inbox.sync[T](name)
  override val self = inbox.ref

  private var _children = TreeMap.empty[String, Inbox.SyncInbox[_]]
  private val childName = Iterator from 1 map (Helpers.base64(_))

  override def children: Iterable[ActorRef[Nothing]] = _children.values map (_.ref)
  override def child(name: String): Option[ActorRef[Nothing]] = _children get name map (_.ref)
  override def spawn[U](props: Props[U]): ActorRef[U] = {
    val i = Inbox.sync[U](childName.next())
    _children += i.ref.ref.path.name -> i
    i.ref
  }
  override def spawn[U](props: Props[U], name: String): ActorRef[U] =
    _children get name match {
      case Some(_) ⇒ throw new a.InvalidActorNameException(s"actor name $name is already taken")
      case None ⇒
        val i = Inbox.sync[U](name)
        _children += name -> i
        i.ref
    }
  override def actorOf(props: a.Props): a.ActorRef = {
    val i = Inbox.sync[Any](childName.next())
    _children += i.ref.ref.path.name -> i
    i.ref.ref
  }
  override def actorOf(props: a.Props, name: String): a.ActorRef =
    _children get name match {
      case Some(_) ⇒ throw new a.InvalidActorNameException(s"actor name $name is already taken")
      case None ⇒
        val i = Inbox.sync[Any](name)
        _children += name -> i
        i.ref.ref
    }
  override def stop(childName: String): Unit = () // removal is asynchronous
  def watch[U](other: ActorRef[U]): ActorRef[U] = other
  def watch(other: akka.actor.ActorRef): other.type = other
  def unwatch[U](other: ActorRef[U]): ActorRef[U] = other
  def unwatch(other: akka.actor.ActorRef): other.type = other
  def setReceiveTimeout(d: Duration): Unit = ()
  def schedule[T](delay: FiniteDuration, target: ActorRef[T], msg: T): a.Cancellable = new a.Cancellable {
    def cancel() = false
    def isCancelled = true
  }
  implicit def executionContext: ExecutionContextExecutor = system.untyped.dispatcher
  def createWrapper[U](f: U ⇒ T): ActorRef[U] = ???

  def getInbox[U](name: String): Inbox.SyncInbox[U] = _children(name).asInstanceOf[Inbox.SyncInbox[U]]
  def removeInbox(name: String): Unit = _children -= name
}

class EffectfulActorContext[T](_name: String, _props: Props[T], _system: ActorSystem[Nothing])
  extends DummyActorContext[T](_name, _props, _system) {
  import Pure._

  private val eq = new ConcurrentLinkedQueue[Effect]
  def getEffect(): Effect = eq.poll() match {
    case null ⇒ throw new NoSuchElementException(s"polling on an empty effect queue: $name")
    case x    ⇒ x
  }
  def getAllEffects(): immutable.Seq[Effect] = {
    @tailrec def rec(acc: List[Effect]): List[Effect] = eq.poll() match {
      case null ⇒ acc.reverse
      case x    ⇒ rec(x :: acc)
    }
    rec(Nil)
  }
  def hasEffects: Boolean = eq.peek() != null

  private var current = props.creator()
  private var lastWrapper = Option.empty[Failed.Wrapper[T]]
  def run(msg: T): Unit = current = unwrap(current.message(this, msg))
  def signal(signal: Signal): Unit = current = unwrap(current.management(this, signal))

  def isStopped = current.isInstanceOf[stoppedBehavior]

  private def unwrap(b: Behavior[T]): Behavior[T] = {
    b match {
      case w: Failed.Wrapper[t] ⇒ lastWrapper = Some(w)
      case _                    ⇒ lastWrapper = None
    }
    Behavior.unwrap(b, current)
  }

  override def spawn[U](props: Props[U]): ActorRef[U] = {
    val ref = super.spawn(props)
    eq.offer(Spawned(ref.ref.path.name))
    ref
  }
  override def spawn[U](props: Props[U], name: String): ActorRef[U] = {
    eq.offer(Spawned(name))
    super.spawn(props, name)
  }
  override def actorOf(props: a.Props): a.ActorRef = {
    val ref = super.actorOf(props)
    eq.offer(Spawned(ref.path.name))
    ref
  }
  override def actorOf(props: a.Props, name: String): a.ActorRef = {
    eq.offer(Spawned(name))
    super.actorOf(props, name)
  }
  override def stop(childName: String): Unit = {
    eq.offer(Stopped(childName))
    super.stop(childName)
  }
  override def watch[U](other: ActorRef[U]): ActorRef[U] = {
    eq.offer(Watched(other))
    super.watch(other)
  }
  override def unwatch[U](other: ActorRef[U]): ActorRef[U] = {
    eq.offer(Unwatched(other))
    super.unwatch(other)
  }
  override def watch(other: akka.actor.ActorRef): other.type = {
    eq.offer(Watched(ActorRef[Any](other)))
    super.watch(other)
  }
  override def unwatch(other: akka.actor.ActorRef): other.type = {
    eq.offer(Unwatched(ActorRef[Any](other)))
    super.unwatch(other)
  }
  override def setReceiveTimeout(d: Duration): Unit = {
    eq.offer(ReceiveTimeoutSet(d))
    super.setReceiveTimeout(d)
  }
}
