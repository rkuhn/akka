package akka.typed

trait ActorContext[T] {

  def self: ActorRef[T]
  
  def system: ActorSystem[Nothing]
  
  def children: Iterable[ActorRef[Nothing]]
  
  def child(name: String): Option[ActorRef[Nothing]]
  
  def actorOf(props: Props[T], name: String): ActorRef[T]
  
  def stop(childName: String): Unit
  
  def watch[T](other: ActorRef[T]): other.type
  
  def unwatch[T](other: ActorRef[T]): other.type

  // TODO: add method for registering automatic wrapping of given message type into Holder
}