package akka.typed

class ActorSystem[-T] extends ActorRef[T] {
  def path = ???
  def !(msg: T) = ???
}

object ActorSystem {
  def apply[T](props: Props[T], name: String): ActorSystem[T] = ???
}