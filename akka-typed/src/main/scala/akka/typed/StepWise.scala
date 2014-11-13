/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import scala.concurrent.duration.FiniteDuration
import scala.annotation.tailrec
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.Deadline
import scala.util.control.NonFatal

/**
 * This object contains tools for building step-wise behaviors for formulating
 * a linearly progressing protocol.
 *
 * Example:
 * {{{
 * import scala.concurrent.duration._
 *
 * StepWise[Command] { (ctx, startWith) =>
 *   startWith {
 *     val child = ctx.spawn(...)
 *     child ! msg
 *     child
 *   }.expectMessage(100.millis) { (reply, child) =>
 *     target ! GotReply(reply)
 *   }
 * }
 * }}}
 *
 * State can be passed from one step to the next by returning it as is
 * demonstrated with the `child` ActorRef in the example.
 *
 * This way of writing Actors can be very useful when writing Actor-based
 * test procedures for actor systems, hence also the possibility to expect
 * failures (see [[StepWise$.Steps!.expectFailure]]).
 */
object StepWise {
  import Behavior._

  sealed trait AST
  private case class Thunk(f: () ⇒ Any) extends AST
  private case class Message(timeout: FiniteDuration, f: (Any, Any) ⇒ Any, trace: Trace) extends AST
  private case class MultiMessage(timeout: FiniteDuration, count: Int, f: (Seq[Any], Any) ⇒ Any, trace: Trace) extends AST
  private case class Failure(timeout: FiniteDuration, f: (Failed, Any) ⇒ (Failed.Decision, Any), trace: Trace) extends AST
  private case class Termination(timeout: FiniteDuration, f: (Terminated, Any) ⇒ Any, trace: Trace) extends AST

  private sealed trait Trace {
    def getStackTrace: Array[StackTraceElement]
  }
  private class WithTrace extends Trace {
    private val trace = Thread.currentThread.getStackTrace
    def getStackTrace = trace
  }
  private object WithoutTrace extends Trace {
    def getStackTrace = Thread.currentThread.getStackTrace
  }

  case class Steps[T, U](ops: List[AST], keepTraces: Boolean) {
    private def getTrace(): Trace =
      if (keepTraces) new WithTrace
      else WithoutTrace

    def expectMessage[V](timeout: FiniteDuration)(f: (T, U) ⇒ V): Steps[T, V] =
      copy(ops = Message(timeout, f.asInstanceOf[(Any, Any) ⇒ Any], getTrace()) :: ops)

    def expectMultipleMessages[V](timeout: FiniteDuration, count: Int)(f: (Seq[T], U) ⇒ V): Steps[T, V] =
      copy(ops = MultiMessage(timeout, count, f.asInstanceOf[(Seq[Any], Any) ⇒ Any], getTrace()) :: ops)

    def expectFailure[V](timeout: FiniteDuration)(f: (Failed, U) ⇒ (Failed.Decision, V)): Steps[T, V] =
      copy(ops = Failure(timeout, f.asInstanceOf[(Failed, Any) ⇒ (Failed.Decision, Any)], getTrace()) :: ops)

    def expectTermination[V](timeout: FiniteDuration)(f: (Terminated, U) ⇒ V): Steps[T, V] =
      copy(ops = Termination(timeout, f.asInstanceOf[(Terminated, Any) ⇒ Any], getTrace()) :: ops)

    def withKeepTraces(b: Boolean): Steps[T, U] = copy(keepTraces = b)
  }

  class StartWith[T](keepTraces: Boolean) {
    def apply[U](thunk: ⇒ U): Steps[T, U] = Steps(Thunk(() ⇒ thunk) :: Nil, keepTraces)
    def withKeepTraces(b: Boolean): StartWith[T] = new StartWith(b)
  }

  def apply[T](f: (ActorContext[T], StartWith[T]) ⇒ Steps[T, _]): Behavior[T] =
    Full {
      case (ctx, Left(PreStart)) ⇒ run(ctx, f(ctx, new StartWith(keepTraces = false)).ops.reverse, ())
    }

  private def throwTimeout(trace: Trace, message: String): Nothing =
    throw new TimeoutException(message) {
      override def fillInStackTrace(): Throwable = {
        setStackTrace(trace.getStackTrace)
        this
      }
    }

  private def throwIllegalState(trace: Trace, message: String): Nothing =
    throw new IllegalStateException(message) {
      override def fillInStackTrace(): Throwable = {
        setStackTrace(trace.getStackTrace)
        this
      }
    }

  private def run[T](ctx: ActorContext[T], ops: List[AST], value: Any): Behavior[T] =
    ops match {
      case Thunk(f) :: tail ⇒ run(ctx, tail, f())
      case Message(t, f, trace) :: tail ⇒
        ctx.setReceiveTimeout(t)
        Full {
          case (_, Left(ReceiveTimeout)) ⇒ throwTimeout(trace, s"timeout of $t expired while waiting for a message")
          case (_, Right(msg))           ⇒ run(ctx, tail, f(msg, value))
          case (_, other)                ⇒ throwIllegalState(trace, s"unexpected $other while waiting for a message")
        }
      case MultiMessage(t, c, f, trace) :: tail ⇒
        val deadline = Deadline.now + t
        def behavior(count: Int, acc: List[Any]): Behavior[T] = {
          ctx.setReceiveTimeout(deadline.timeLeft)
          Full {
            case (_, Left(ReceiveTimeout)) ⇒ throwTimeout(trace, s"timeout of $t expired while waiting for $c messages (got only $count)")
            case (_, Right(msg)) ⇒
              val nextCount = count + 1
              if (nextCount == c) {
                run(ctx, tail, f((msg :: acc).reverse, value))
              } else behavior(nextCount, msg :: acc)
            case (_, other) ⇒ throwIllegalState(trace, s"unexpected $other while waiting for $c messages (got $count valid ones)")
          }
        }
        behavior(0, Nil)
      case Failure(t, f, trace) :: tail ⇒
        ctx.setReceiveTimeout(t)
        Full {
          case (_, Left(ReceiveTimeout)) ⇒ throwTimeout(trace, s"timeout of $t expired while waiting for a failure")
          case (_, Left(failure: Failed)) ⇒
            val (response, v) = f(failure, value)
            ctx.setFailureResponse(response)
            run(ctx, tail, v)
          case (_, other) ⇒ throwIllegalState(trace, s"unexpected $other while waiting for a message")
        }
      case Termination(t, f, trace) :: tail ⇒
        ctx.setReceiveTimeout(t)
        Full {
          case (_, Left(ReceiveTimeout)) ⇒ throwTimeout(trace, s"timeout of $t expired while waiting for termination")
          case (_, Left(t: Terminated))  ⇒ run(ctx, tail, f(t, value))
          case (_, other)                ⇒ throwIllegalState(trace, s"unexpected $other while waiting for termination")
        }
      case Nil ⇒ Stopped
    }
}
