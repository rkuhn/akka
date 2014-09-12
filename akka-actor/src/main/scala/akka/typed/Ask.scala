package akka.typed

import scala.concurrent.Future
import akka.util.Timeout
import akka.actor.InternalActorRef
import akka.pattern.AskTimeoutException
import akka.pattern.PromiseActorRef
import java.lang.IllegalArgumentException

object AskPattern {
  implicit class Askable[T](val ref: ActorRef[T]) extends AnyVal {
    def ?[U](f: ActorRef[U] => T)(implicit timeout: Timeout): Future[U] = ask(ref, timeout, f)
  }

  class PromiseRef[U](actorRef: ActorRef[_], timeout: Timeout) {
    val (ref: ActorRef[U], future: Future[U]) = actorRef.ref match {
      case ref: InternalActorRef if ref.isTerminated ⇒
        (ref.provider.deadLetters,
          Future.failed[U](new AskTimeoutException(s"Recipient[$actorRef] had already been terminated.")))
      case ref: InternalActorRef ⇒
        if (timeout.duration.length <= 0)
          (ref.provider.deadLetters,
            Future.failed[U](new IllegalArgumentException(s"Timeout length must not be negative, question not sent to [$actorRef]")))
        else {
          val a = PromiseActorRef(ref.provider, timeout, targetName = actorRef.toString)
          val b = new ActorRef[U](a)
          (b, a.result.future.asInstanceOf[Future[U]])
        }
      case _ ⇒ throw new IllegalArgumentException(s"cannot create PromiseRef for non-Akka ActorRef (${actorRef.getClass})")
    }
  }
  
  object PromiseRef {
    def apply[U](actorRef: ActorRef[_])(implicit timeout: Timeout) = new PromiseRef[U](actorRef, timeout)
  }

  private[typed] def ask[T, U](actorRef: ActorRef[T], timeout: Timeout, f: ActorRef[U] => T): Future[U] = {
    val p = PromiseRef[U](actorRef)(timeout)
    actorRef ! f(p.ref)
    p.future
  }

}