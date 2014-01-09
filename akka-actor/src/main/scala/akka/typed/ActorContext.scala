package akka.typed

import scala.reflect.ClassTag

trait ActorContext[T] {

  def self: ActorRef[T]
  
  def props: Props[T]
  
  def system: ActorSystem[Nothing]
  
  def children: Iterable[ActorRef[Nothing]]
  
  def child(name: String): Option[ActorRef[Nothing]]
  
  def actorOf(props: Props[T], name: String): ActorRef[T]
  
  def stop(childName: String): Unit
  
  def watch[U](other: ActorRef[U]): other.type
  
  def unwatch[U](other: ActorRef[U]): other.type

  def registerWrapper[T](foreign: Class[T], local: Class[Holder[_ <: T]]): Unit // TODO: remove
  
  def selfRef[T: ClassTag]: ActorRef[T] // TODO: remove
}
