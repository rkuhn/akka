package akka.typed

import scala.concurrent.Future
import akka.util.Timeout
import akka.actor.InternalActorRef
import akka.pattern.AskTimeoutException
import akka.pattern.PromiseActorRef

object AskRef {
  implicit class Askable[T](val ref: ActorRef[T]) extends AnyVal {
    def ?[U](f: ActorRef[U] => T)(implicit timeout: Timeout): Future[U] = ask(ref, timeout, f)
  }

  private[typed] def ask[T, U](actorRef: ActorRef[T], timeout: Timeout, f: ActorRef[U] => T): Future[U] = actorRef.ref match {
    case ref: InternalActorRef if ref.isTerminated ⇒
      actorRef ! f(null)
      Future.failed[U](new AskTimeoutException(s"Recipient[$actorRef] had already been terminated."))
    case ref: InternalActorRef ⇒
      if (timeout.duration.length <= 0)
        Future.failed[U](new IllegalArgumentException(s"Timeout length must not be negative, question not sent to [$actorRef]"))
      else {
        val a = PromiseActorRef(ref.provider, timeout, targetName = actorRef.toString)
        val b = new ActorRef[U](a)
        actorRef ! f(b)
        a.result.future.asInstanceOf[Future[U]]
      }
    case _ ⇒ Future.failed[U](new IllegalArgumentException(s"Unsupported recipient ActorRef type, question not sent to [$actorRef]"))
  }
}