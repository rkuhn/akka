package akka.typed

abstract class Props[T] {
  def actorClass: Class[_ <: Actor[_]]
  private[akka] def newActor(): Actor[T]
}

object Props {
  def apply[T](block: => Actor[T]): Props[T] = ???
}
