/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.io

import java.lang.{ Iterable ⇒ JIterable }
import scala.annotation.tailrec
import scala.util.{ Try, Success, Failure }
import java.nio.ByteOrder
import akka.util.ByteString

/**
 * Scala API: A pair of pipes, one for commands and one for events. Commands travel from
 * top to bottom, events from bottom to top.
 *
 * Java base classes are provided in the form of [[AbstractPipePair]]
 * and [[AbstractSymmetricPipePair]] since the Scala function types can be
 * awkward to handle in Java.
 *
 * @see [[PipelineStage]]
 * @see [[AbstractPipePair]]
 * @see [[AbstractSymmetricPipePair]]
 * @see [[PipePairFactory]]
 */
trait PipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow] {

  /**
   * The command pipeline transforms injected commands from the upper stage
   * into commands for the stage below, but it can also emit events for the
   * upper stage. Any number of each can be generated.
   */
  def commandPipeline: CmdAbove ⇒ Iterable[Either[EvtAbove, CmdBelow]]

  /**
   * The event pipeline transforms injected event from the lower stage
   * into event for the stage above, but it can also emit commands for the
   * stage below. Any number of each can be generated.
   */
  def eventPipeline: EvtBelow ⇒ Iterable[Either[EvtAbove, CmdBelow]]
}

/**
 * A convenience type for expressing a [[PipePair]] which has the same types
 * for commands and events.
 */
trait SymmetricPipePair[Above, Below] extends PipePair[Above, Below, Above, Below]

/**
 * Java API: A pair of pipes, one for commands and one for events. Commands travel from
 * top to bottom, events from bottom to top.
 *
 * @see [[PipelineStage]]
 * @see [[AbstractSymmetricPipePair]]
 * @see [[PipePairFactory]]
 */
abstract class AbstractPipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow] {

  /**
   * Commands reaching this pipe pair are transformed into a sequence of
   * commands for the next or events for the previous stage.
   *
   * Throwing exceptions within this method will abort processing of the whole
   * pipeline which this pipe pair is part of.
   *
   * @param cmd the incoming command
   * @return an Iterable of elements which are either events or commands
   *
   * @see [[#makeCommand]]
   * @see [[#makeEvent]]
   */
  def onCommand(cmd: CmdAbove): JIterable[Either[EvtAbove, CmdBelow]]

  /**
   * Events reaching this pipe pair are transformed into a sequence of
   * commands for the next or events for the previous stage.
   *
   * Throwing exceptions within this method will abort processing of the whole
   * pipeline which this pipe pair is part of.
   *
   * @param cmd the incoming command
   * @return an Iterable of elements which are either events or commands
   *
   * @see [[#makeCommand]]
   * @see [[#makeEvent]]
   */
  def onEvent(event: EvtBelow): JIterable[Either[EvtAbove, CmdBelow]]

  /**
   * Helper method for wrapping a command which shall be emitted.
   */
  def makeCommand(cmd: CmdBelow): Either[EvtAbove, CmdBelow] = Right(cmd)

  /**
   * Helper method for wrapping an event which shall be emitted.
   */
  def makeEvent(event: EvtAbove): Either[EvtAbove, CmdBelow] = Left(event)
}

/**
 * A convenience type for expressing a [[AbstractPipePair]] which has the same types
 * for commands and events.
 */
abstract class AbstractSymmetricPipePair[Above, Below] extends AbstractPipePair[Above, Below, Above, Below]

/**
 * This class contains static factory methods which produce [[PipePair]]
 * instances; those are needed within the implementation of [[PipelineStage#apply]].
 */
object PipePairFactory {

  /**
   * Scala API: construct a [[PipePair]] from the two given functions; useful for not capturing `$outer` references.
   */
  def apply[CmdAbove, CmdBelow, EvtAbove, EvtBelow] //
  (commandPL: CmdAbove ⇒ Iterable[Either[EvtAbove, CmdBelow]], eventPL: EvtBelow ⇒ Iterable[Either[EvtAbove, CmdBelow]]) =
    new PipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow] {
      override def commandPipeline = commandPL
      override def eventPipeline = eventPL
    }

  /**
   * Java API: construct a [[PipePair]] from the given [[AbstractPipePair]].
   */
  def create[CmdAbove, CmdBelow, EvtAbove, EvtBelow] //
  (ap: AbstractPipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow]) =
    new PipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow] {
      import scala.collection.JavaConverters._
      override def commandPipeline = (cmd) ⇒ ap.onCommand(cmd).asScala
      override def eventPipeline = (evt) ⇒ ap.onEvent(evt).asScala
    }

  /**
   * Java API: construct a [[PipePair]] from the given [[AbstractSymmetricPipePair]].
   */
  def create[Above, Below] //
  (ap: AbstractSymmetricPipePair[Above, Below]): SymmetricPipePair[Above, Below] =
    new SymmetricPipePair[Above, Below] {
      import scala.collection.JavaConverters._
      override def commandPipeline = (cmd) ⇒ ap.onCommand(cmd).asScala
      override def eventPipeline = (evt) ⇒ ap.onEvent(evt).asScala
    }
}

/**
 * This class contains static factory methods which turn a pipeline context
 * and a [[PipelineStage]] into readily usable pipelines.
 */
object PipelineFactory {

  /**
   * Scala API: build the pipeline and return the “naked” [[PipePair]] which
   * holds the command and event pipeline functions. Exceptions thrown by the
   * pipeline stages will not be caught.
   *
   * @param ctx The context object for this pipeline
   * @param stage The (composite) pipeline stage from which to build the pipeline
   * @return the [[PipePair]] built from the pipeline stage
   */
  def buildPipePair[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow] //
  (ctx: Ctx, stage: PipelineStage[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow]) //
  : PipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow] =
    stage apply ctx

  /**
   * Scala API: build the pipeline and return a pair of functions representing
   * the command and event pipelines. Each function returns the commands and
   * events resulting from running the pipeline on the given input, where the
   * the sequence of events is the first element of the returned pair and the
   * sequence of commands the second element.
   *
   * Exceptions thrown by the pipeline stages will not be caught.
   *
   * @param ctx The context object for this pipeline
   * @param stage The (composite) pipeline stage from whcih to build the pipeline
   * @return a pair of command and event pipeline functions
   */
  def buildFunctionPair[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow] //
  (ctx: Ctx, stage: PipelineStage[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow]) //
  : (CmdAbove ⇒ (Iterable[EvtAbove], Iterable[CmdBelow]), EvtBelow ⇒ (Iterable[EvtAbove], Iterable[CmdBelow])) = {
    val pp = stage apply ctx
    val split = (in: Iterable[Either[EvtAbove, CmdBelow]]) ⇒ {
      val cmds = Vector.newBuilder[CmdBelow]
      val evts = Vector.newBuilder[EvtAbove]
      in foreach {
        case Right(cmd) ⇒ cmds += cmd
        case Left(evt)  ⇒ evts += evt
      }
      (evts.result, cmds.result)
    }
    (pp.commandPipeline andThen split, pp.eventPipeline andThen split)
  }

  /**
   * Scala API: build the pipeline attaching the given command and event sinks
   * to its outputs. Exceptions thrown within the pipeline stages will abort
   * processing (i.e. will not be processed in following stages) but will be
   * caught and passed as [[scala.util.Failure]] into the respective sink.
   *
   * @param ctx The context object for this pipeline
   * @param stage The (composite) pipeline stage from whcih to build the pipeline
   * @param commandSink The function to invoke for commands or command failures
   * @param eventSink The function to invoke for events or event failures
   * @return a handle for injecting events or commands into the pipeline
   */
  def buildWithSinkFunctions[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow] //
  (ctx: Ctx,
   stage: PipelineStage[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow],
   commandSink: Try[CmdBelow] ⇒ Unit,
   eventSink: Try[EvtAbove] ⇒ Unit): PipelineInjector[CmdAbove, EvtBelow] =
    new PipelineInjector[CmdAbove, EvtBelow] {
      val pl = stage(ctx)
      override def injectCommand(cmd: CmdAbove): Unit = {
        Try(pl.commandPipeline(cmd)) match {
          case f: Failure[_] ⇒ commandSink(f.asInstanceOf[Try[CmdBelow]])
          case Success(out) ⇒
            out foreach {
              case Right(cmd) ⇒ commandSink(Success(cmd))
              case Left(evt)  ⇒ eventSink(Success(evt))
            }
        }
      }
      override def injectEvent(evt: EvtBelow): Unit = {
        Try(pl.eventPipeline(evt)) match {
          case f: Failure[_] ⇒ eventSink(f.asInstanceOf[Try[EvtAbove]])
          case Success(out) ⇒
            out foreach {
              case Right(cmd) ⇒ commandSink(Success(cmd))
              case Left(evt)  ⇒ eventSink(Success(evt))
            }
        }
      }
    }

  /**
   * Java API: build the pipeline attaching the given callback object to its
   * outputs. Exceptions thrown within the pipeline stages will abort
   * processing (i.e. will not be processed in following stages) but will be
   * caught and passed as [[scala.util.Failure]] into the respective sink.
   *
   * @param ctx The context object for this pipeline
   * @param stage The (composite) pipeline stage from whcih to build the pipeline
   * @param callback The [[PipelineSink]] to attach to the built pipeline
   * @return a handle for injecting events or commands into the pipeline
   */
  def buildWithSink[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow] //
  (ctx: Ctx,
   stage: PipelineStage[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow],
   callback: PipelineSink[CmdBelow, EvtAbove]): PipelineInjector[CmdAbove, EvtBelow] =
    buildWithSinkFunctions[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow](ctx, stage, {
      case Failure(thr) ⇒ callback.onCommandFailure(thr)
      case Success(cmd) ⇒ callback.onCommand(cmd)
    }, {
      case Failure(thr) ⇒ callback.onEventFailure(thr)
      case Success(evt) ⇒ callback.onEvent(evt)
    })
}

/**
 * A handle for injecting commands and events into a pipeline. Commands travel
 * down (or to the right) through the stages, events travel in the opposite
 * direction.
 *
 * @see [[PipelineFactory#buildWithSinkFunctions]]
 * @see [[PipelineFactory#buildWithSink]]
 */
trait PipelineInjector[Cmd, Evt] {

  /**
   * Inject the given command into the connected pipeline.
   */
  def injectCommand(cmd: Cmd): Unit

  /**
   * Inject the given event into the connected pipeline.
   */
  def injectEvent(event: Evt): Unit
}

/**
 * A sink which can be attached by [[PipelineFactory#buildWithSink]] to a
 * pipeline when it is being built. The methods are called when commands,
 * events or their failures occur during evaluation of the pipeline (i.e.
 * when injection is triggered using the associated [[PipelineInjector]]).
 */
abstract class PipelineSink[Cmd, Evt] {

  /**
   * This callback is invoked for every command generated by the pipeline.
   */
  def onCommand(cmd: Cmd): Unit

  /**
   * This callback is invoked if an exception occurred while processing an
   * injected command. If this callback is invoked that no other callbacks will
   * be invoked for the same injection.
   */
  def onCommandFailure(thr: Throwable): Unit

  /**
   * This callback is invoked for every event generated by the pipeline.
   */
  def onEvent(event: Evt): Unit

  /**
   * This callback is invoked if an exception occurred while processing an
   * injected event. If this callback is invoked that no other callbacks will
   * be invoked for the same injection.
   */
  def onEventFailure(thr: Throwable): Unit
}

object PipelineStage {

  /**
   * Java API: attach the two given stages such that the command output of the
   * first is fed into the command input of the second, and the event output of
   * the second is fed into the event input of the first. In other words:
   * sequence the stages such that the left one is on top of the right one.
   *
   * @param left the left or upper pipeline stage
   * @param right the right or lower pipeline stage
   * @return a pipeline stage representing the sequence of the two stages
   */
  def sequence[Ctx, CmdAbove, CmdBelow, CmdBelowBelow, EvtAbove, EvtBelow, EvtBelowBelow] //
  (left: PipelineStage[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow],
   right: PipelineStage[Ctx, CmdBelow, CmdBelowBelow, EvtBelow, EvtBelowBelow]) //
   : PipelineStage[Ctx, CmdAbove, CmdBelowBelow, EvtAbove, EvtBelowBelow] =
    left >> right

  /**
   * Java API: combine the two stages such that the command pipeline of the
   * left stage is used and the event pipeline of the right, discarding the
   * other two sub-pipelines.
   *
   * @param left the command pipeline
   * @param right the event pipeline
   * @return a pipeline stage using the left command pipeline and the right event pipeline
   */
  def combine[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow] //
  (left: PipelineStage[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow],
   right: PipelineStage[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow]) //
   : PipelineStage[Ctx, CmdAbove, CmdBelow, EvtAbove, EvtBelow] =
    left | right
}

/**
 * A [[PipelineStage]] which is symmetric in command and event types, i.e. it only
 * has one command and event type above and one below.
 */
abstract class SymmetricPipelineStage[Context, Above, Below] extends PipelineStage[Context, Above, Below, Above, Below]

/**
 * A pipeline stage which can be combined with other stages to build a
 * protocol stack. The main function of this class is to serve as a factory
 * for the actual [[PipePair]] generated by the [[#apply]] method so that a
 * context object can be passed in.
 *
 * @see [[PipelineFactory]]
 */
abstract class PipelineStage[Context, CmdAbove, CmdBelow, EvtAbove, EvtBelow] { left ⇒

  /**
   * Implement this method to generate this stage’s pair of command and event
   * functions.
   *
   * @see [[AbstractPipePair]]
   * @see [[AbstractSymmetricPipePair]]
   */
  protected[io] def apply(ctx: Context): PipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow]

  /**
   * Scala API: attach the two given stages such that the command output of the
   * first is fed into the command input of the second, and the event output of
   * the second is fed into the event input of the first. In other words:
   * sequence the stages such that the left one is on top of the right one.
   *
   * @param right the right or lower pipeline stage
   * @return a pipeline stage representing the sequence of the two stages
   */
  def >>[CmdBelowBelow, EvtBelowBelow, BelowContext <: Context] //
  (right: PipelineStage[_ >: BelowContext, CmdBelow, CmdBelowBelow, EvtBelow, EvtBelowBelow]) //
  : PipelineStage[BelowContext, CmdAbove, CmdBelowBelow, EvtAbove, EvtBelowBelow] =
    new PipelineStage[BelowContext, CmdAbove, CmdBelowBelow, EvtAbove, EvtBelowBelow] {
      protected[io] override def apply(ctx: BelowContext): PipePair[CmdAbove, CmdBelowBelow, EvtAbove, EvtBelowBelow] = {
        val leftPL = left(ctx)
        val rightPL = right(ctx)
        new PipePair[CmdAbove, CmdBelowBelow, EvtAbove, EvtBelowBelow] {
          override val commandPipeline = { a: CmdAbove ⇒
            val output = Vector.newBuilder[Either[EvtAbove, CmdBelowBelow]]
            def rec(input: Iterable[Either[EvtAbove, CmdBelow]]): Unit = {
              input foreach {
                case Right(cmd) ⇒
                  rightPL.commandPipeline(cmd) foreach {
                    case r @ Right(_) ⇒ output += r.asInstanceOf[Right[EvtAbove, CmdBelowBelow]]
                    case Left(evt)    ⇒ rec(leftPL.eventPipeline(evt))
                  }
                case l @ Left(_) ⇒ output += l.asInstanceOf[Left[EvtAbove, CmdBelowBelow]]
              }
            }
            rec(leftPL.commandPipeline(a))
            output.result
          }
          override val eventPipeline = { b: EvtBelowBelow ⇒
            val output = Vector.newBuilder[Either[EvtAbove, CmdBelowBelow]]
            def rec(input: Iterable[Either[EvtBelow, CmdBelowBelow]]): Unit = {
              input foreach {
                case r @ Right(_) ⇒ output += r.asInstanceOf[Right[EvtAbove, CmdBelowBelow]]
                case Left(evt) ⇒
                  leftPL.eventPipeline(evt) foreach {
                    case Right(cmd)  ⇒ rec(rightPL.commandPipeline(cmd))
                    case l @ Left(_) ⇒ output += l.asInstanceOf[Left[EvtAbove, CmdBelowBelow]]
                  }
              }
            }
            rec(rightPL.eventPipeline(b))
            output.result
          }
        }
      }
    }

  /**
   * Scala API: combine the two stages such that the command pipeline of the
   * left stage is used and the event pipeline of the right, discarding the
   * other two sub-pipelines.
   *
   * @param right the event pipeline
   * @return a pipeline stage using the left command pipeline and the right event pipeline
   */
  def |[RightContext <: Context] //
  (right: PipelineStage[_ >: RightContext, CmdAbove, CmdBelow, EvtAbove, EvtBelow]) //
  : PipelineStage[RightContext, CmdAbove, CmdBelow, EvtAbove, EvtBelow] =
    new PipelineStage[RightContext, CmdAbove, CmdBelow, EvtAbove, EvtBelow] {
      override def apply(ctx: RightContext): PipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow] =
        new PipePair[CmdAbove, CmdBelow, EvtAbove, EvtBelow] {
          override val commandPipeline = left(ctx).commandPipeline
          override val eventPipeline = right(ctx).eventPipeline
        }
    }
}

/**
 * Pipeline stage for length-field encoded framing. It will prepend a
 * four-byte length header to the message; the header contains the length of
 * the resulting frame including header in big-endian representation.
 *
 * The `maxSize` argument is used to protect the communication channel sanity:
 * larger frames will not be sent (silently dropped) or received (in which case
 * stream decoding would be broken, hence throwing an IllegalArgumentException).
 */
class LengthFieldFrame(maxSize: Int) extends SymmetricPipelineStage[AnyRef, ByteString, ByteString] {
  override def apply(ctx: AnyRef) =
    new SymmetricPipePair[ByteString, ByteString] {
      var buffer = None: Option[ByteString]
      implicit val byteOrder = ByteOrder.BIG_ENDIAN

      @tailrec def extractFrames(bs: ByteString, acc: List[Left[ByteString, ByteString]]): (Option[ByteString], Seq[Left[ByteString, ByteString]]) = {
        if (bs.isEmpty) {
          (None, acc.reverse)
        } else if (bs.length < 4) {
          (Some(bs.compact), acc.reverse)
        } else {
          val length = bs.iterator.getInt
          if (length > maxSize)
            throw new IllegalArgumentException(s"received too large frame of size $length (max = $maxSize)")
          if (bs.length >= length) {
            extractFrames(bs drop length, Left(bs.slice(4, length)) :: acc)
          } else {
            (Some(bs.compact), acc.reverse)
          }
        }
      }

      override def commandPipeline =
        { bs ⇒
          val length = bs.length + 4
          if (length > maxSize) Seq()
          else {
            val bb = java.nio.ByteBuffer.allocate(4)
            bb.order(byteOrder)
            bb.putInt(bs.length + 4).flip
            Seq(Right(ByteString(bb) ++ bs))
          }
        }
      override def eventPipeline =
        { bs ⇒
          val data = if (buffer.isEmpty) bs else buffer.get ++ bs
          extractFrames(data, Nil) match {
            case (nb, result) ⇒ buffer = nb; result
          }
        }
    }
}
