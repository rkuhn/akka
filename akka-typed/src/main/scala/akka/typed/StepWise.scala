package akka.typed

import scala.concurrent.duration.FiniteDuration
import scala.annotation.tailrec
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.Deadline

object StepWise {
  import Behavior._

  sealed trait AST
  private case class Thunk(f: () ⇒ Any) extends AST
  private case class Message(timeout: FiniteDuration, f: (Any, Any) ⇒ Any) extends AST
  private case class MultiMessage(timeout: FiniteDuration, count: Int, f: (Seq[Any], Any) ⇒ Any) extends AST
  private case class Failure(timeout: FiniteDuration, f: (Failed, Any) ⇒ (Failed.Decision, Any)) extends AST
  private case class Termination(timeout: FiniteDuration, f: (Terminated, Any) ⇒ Any) extends AST

  case class Steps[T, U](ops: List[AST]) {

    def expectMessage[V](timeout: FiniteDuration)(f: (T, U) ⇒ V): Steps[T, V] =
      Steps(Message(timeout, f.asInstanceOf[(Any, Any) ⇒ Any]) :: ops)

    def expectMultipleMessages[V](timeout: FiniteDuration, count: Int)(f: (Seq[T], U) ⇒ V): Steps[T, V] =
      Steps(MultiMessage(timeout, count, f.asInstanceOf[(Seq[Any], Any) ⇒ Any]) :: ops)

    def expectFailure[V](timeout: FiniteDuration)(f: (Failed, U) ⇒ (Failed.Decision, V)): Steps[T, V] =
      Steps(Failure(timeout, f.asInstanceOf[(Failed, Any) ⇒ (Failed.Decision, Any)]) :: ops)

    def expectTermination[V](timeout: FiniteDuration)(f: (Terminated, U) ⇒ V): Steps[T, V] =
      Steps(Termination(timeout, f.asInstanceOf[(Terminated, Any) ⇒ Any]) :: ops)
  }

  class StartWith[T] {
    def apply[U](thunk: ⇒ U): Steps[T, U] = Steps(Thunk(() ⇒ thunk) :: Nil)
  }

  def apply[T](f: (ActorContext[T], StartWith[T]) ⇒ Steps[T, _]): Behavior[T] =
    Full {
      case (ctx, Left(PreStart)) ⇒ run(ctx, f(ctx, new StartWith).ops.reverse, ())
    }

  private def run[T](ctx: ActorContext[T], ops: List[AST], value: Any): Behavior[T] =
    ops match {
      case Thunk(f) :: tail ⇒ run(ctx, tail, f())
      case Message(t, f) :: tail ⇒
        ctx.setReceiveTimeout(t)
        Full {
          case (_, Left(ReceiveTimeout)) ⇒ throw new TimeoutException(s"timeout of $t expired while waiting for a message")
          case (_, Right(msg))           ⇒ run(ctx, tail, f(msg, value))
          case (_, other)                ⇒ throw new IllegalStateException(s"unexpected $other while waiting for a message")
        }
      case MultiMessage(t, c, f) :: tail ⇒
        val deadline = Deadline.now + t
        def behavior(count: Int, acc: List[Any]): Behavior[T] = {
          ctx.setReceiveTimeout(deadline.timeLeft)
          Full {
            case (_, Left(ReceiveTimeout)) ⇒ throw new TimeoutException(s"timeout of $t expired while waiting for $c messages (got only $count)")
            case (_, Right(msg)) ⇒
              val nextCount = count + 1
              if (nextCount == c) {
                run(ctx, tail, f((msg :: acc).reverse, value))
              } else behavior(nextCount, msg :: acc)
            case (_, other) ⇒ throw new IllegalStateException(s"unexpected $other while waiting for $c messages (got $count valid ones)")
          }
        }
        behavior(0, Nil)
      case Failure(t, f) :: tail ⇒
        ctx.setReceiveTimeout(t)
        Full {
          case (_, Left(ReceiveTimeout)) ⇒ throw new TimeoutException(s"timeout of $t expired while waiting for a failure")
          case (_, Left(failure: Failed)) ⇒
            val (response, v) = f(failure, value)
            val next = run(ctx, tail, v)
            import Failed._
            response match {
              case Resume   ⇒ Resume(next)
              case Restart  ⇒ Restart(next)
              case Stop     ⇒ Stop(next)
              case Escalate ⇒ Escalate(next)
            }
          case (_, other) ⇒ throw new IllegalStateException(s"unexpected $other while waiting for a message")
        }
      case Termination(t, f) :: tail ⇒
        ctx.setReceiveTimeout(t)
        Full {
          case (_, Left(ReceiveTimeout)) ⇒ throw new TimeoutException(s"timeout of $t expired while waiting for termination")
          case (_, Left(t: Terminated))  ⇒ run(ctx, tail, f(t, value))
          case (_, other)                ⇒ throw new IllegalStateException(s"unexpected $other while waiting for termination")
        }
      case Nil ⇒ Stopped
    }
}
