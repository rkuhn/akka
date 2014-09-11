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
  def createWrapper[U](f: U => T): ActorRef[U]
}

private[typed] class ActorContextAdapter[T](ctx: akka.actor.ActorContext) extends ActorContext[T] {
  import Ops._
  def self = new ActorRef(ctx.self)
  def props = Props(ctx.props)
  def system = ctx.system
  def children = ctx.children.map(new ActorRef(_))
  def child(name: String) = ctx.child(name).map(new ActorRef(_))
  def spawn[U](props: Props[U]) = ctx.spawn(props)
  def spawn[U](props: Props[U], name: String) = ctx.spawn(props, name)
  def stop(name: String) = ctx.child(name) foreach (ctx.stop)
  def watch[U](other: ActorRef[U]) = { ctx.watch(other.ref); other }
  def watch(other: akka.actor.ActorRef) = { ctx.watch(other); other }
  def unwatch[U](other: ActorRef[U]) = { ctx.unwatch(other.ref); other }
  def unwatch(other: akka.actor.ActorRef) = { ctx.unwatch(other); other }
  def setReceiveTimeout(d: Duration) = ctx.setReceiveTimeout(d)
  def createWrapper[U](f: U => T) = new ActorRef[U](ctx.actorOf(akka.actor.Props(classOf[MessageWrapper], f)))
}

private[typed] class MessageWrapper(f: Any => Any) extends akka.actor.Actor {
  def receive = {
    case msg => context.parent ! f(msg)
  }
}
