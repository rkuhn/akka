package akka.typed

class Props[T] {
  
}

object Props {
  def apply[T](block: => Actor[T]): Props[T] = ???
}
