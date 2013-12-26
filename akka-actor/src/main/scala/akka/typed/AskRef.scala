package akka.typed

import scala.concurrent.Future

trait AskRef[T] extends ActorRef[T] {
  def future: Future[T]
}

object AskRef {
  def apply[T](): AskRef[T] = ???
}