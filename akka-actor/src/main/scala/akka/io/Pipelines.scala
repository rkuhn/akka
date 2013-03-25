/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.io

import java.lang.{ Iterable ⇒ JIterable }
import scala.annotation.tailrec
import scala.util.{ Try, Success, Failure }

/**
 * Events flow from Left/Below to Right/Above.
 * Commands flow from Right/Above to Left/Below.
 */
trait PipePair[CmdBelow, CmdAbove, EvtBelow, EvtAbove] {
  def commandPipeline: CmdAbove ⇒ Iterable[Either[CmdBelow, EvtAbove]]
  def eventPipeline: EvtBelow ⇒ Iterable[Either[CmdBelow, EvtAbove]]
}

trait SymmetricPipePair[Below, Above] extends PipePair[Below, Above, Below, Above]

abstract class AbstractPipePair[CmdBelow, CmdAbove, EvtBelow, EvtAbove] {
  def onCommand(cmd: CmdAbove): JIterable[Either[CmdBelow, EvtAbove]]
  def onEvent(event: EvtBelow): JIterable[Either[CmdBelow, EvtAbove]]
  def makeCommand(cmd: CmdBelow): Either[CmdBelow, EvtAbove] = Left(cmd)
  def makeEvent(event: EvtAbove): Either[CmdBelow, EvtAbove] = Right(event)
}

abstract class AbstractSymmetricPipePair[Below, Above] extends AbstractPipePair[Below, Above, Below, Above]

object PipePairFactory {
  def apply[CmdBelow, CmdAbove, EvtBelow, EvtAbove] //
  (commandPL: CmdAbove ⇒ Iterable[Either[CmdBelow, EvtAbove]], eventPL: EvtBelow ⇒ Iterable[Either[CmdBelow, EvtAbove]]) =
    new PipePair[CmdBelow, CmdAbove, EvtBelow, EvtAbove] {
      override def commandPipeline = commandPL
      override def eventPipeline = eventPL
    }
  def create[CmdBelow, CmdAbove, EvtBelow, EvtAbove] //
  (ap: AbstractPipePair[CmdBelow, CmdAbove, EvtBelow, EvtAbove]) =
    new PipePair[CmdBelow, CmdAbove, EvtBelow, EvtAbove] {
      import scala.collection.JavaConverters._
      override def commandPipeline = (cmd) ⇒ ap.onCommand(cmd).asScala
      override def eventPipeline = (evt) ⇒ ap.onEvent(evt).asScala
    }
}

object PipelineFactory {
  def build[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove] //
  (ctx: Ctx, stage: PipelineStage[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove]) //
  : PipePair[CmdBelow, CmdAbove, EvtBelow, EvtAbove] =
    stage apply ctx

  def buildFunctionPair[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove] //
  (ctx: Ctx, stage: PipelineStage[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove]) //
  : (CmdAbove ⇒ (Iterable[CmdBelow], Iterable[EvtAbove]), EvtBelow ⇒ (Iterable[CmdBelow], Iterable[EvtAbove])) = {
    val pp = stage apply ctx
    val split = (in: Iterable[Either[CmdBelow, EvtAbove]]) ⇒ {
      val cmds = Vector.newBuilder[CmdBelow]
      val evts = Vector.newBuilder[EvtAbove]
      in foreach {
        case Left(cmd)  ⇒ cmds += cmd
        case Right(evt) ⇒ evts += evt
      }
      (cmds.result, evts.result)
    }
    (pp.commandPipeline andThen split, pp.eventPipeline andThen split)
  }

  def buildWithSinks[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove] //
  (ctx: Ctx,
   stage: PipelineStage[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove],
   commandSink: Try[CmdBelow] ⇒ Unit,
   eventSink: Try[EvtAbove] ⇒ Unit): PipelineWithSink[CmdAbove, EvtBelow] =
    new PipelineWithSink[CmdAbove, EvtBelow] {
      val pl = stage(ctx)
      override def injectCommand(cmd: CmdAbove): Unit = {
        Try(pl.commandPipeline(cmd)) match {
          case f: Failure[_] ⇒ commandSink(f.asInstanceOf[Try[CmdBelow]])
          case Success(out) ⇒
            out foreach {
              case Left(cmd)  ⇒ commandSink(Success(cmd))
              case Right(evt) ⇒ eventSink(Success(evt))
            }
        }
      }
      override def injectEvent(evt: EvtBelow): Unit = {
        Try(pl.eventPipeline(evt)) match {
          case f: Failure[_] ⇒ eventSink(f.asInstanceOf[Try[EvtAbove]])
          case Success(out) ⇒
            out foreach {
              case Left(cmd)  ⇒ commandSink(Success(cmd))
              case Right(evt) ⇒ eventSink(Success(evt))
            }
        }
      }
    }
  def buildWithNotifier[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove] //
  (ctx: Ctx,
   stage: PipelineStage[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove],
   notifier: PipelineSink[CmdBelow, EvtAbove]): PipelineWithSink[CmdAbove, EvtBelow] =
    buildWithSinks[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove](ctx, stage, {
      case Failure(thr) ⇒ notifier.onCommandFailure(thr)
      case Success(cmd) ⇒ notifier.onCommand(cmd)
    }, {
      case Failure(thr) ⇒ notifier.onEventFailure(thr)
      case Success(evt) ⇒ notifier.onEvent(evt)
    })
}

trait PipelineWithSink[Cmd, Evt] {
  def injectCommand(cmd: Cmd): Unit
  def injectEvent(event: Evt): Unit
}

abstract class PipelineSink[Cmd, Evt] {
  def onCommand(cmd: Cmd): Unit
  def onCommandFailure(thr: Throwable): Unit
  def onEvent(event: Evt): Unit
  def onEventFailure(thr: Throwable): Unit
}

object PipelineStage {
  def sequence[Ctx, CmdBelow, CmdAbove, CmdAboveAbove, EvtBelow, EvtAbove, EvtAboveAbove] //
  (left: PipelineStage[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove],
   right: PipelineStage[Ctx, CmdAbove, CmdAboveAbove, EvtAbove, EvtAboveAbove]) //
   : PipelineStage[Ctx, CmdBelow, CmdAboveAbove, EvtBelow, EvtAboveAbove] =
    left >> right
  def combine[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove] //
  (left: PipelineStage[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove],
   right: PipelineStage[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove]) //
   : PipelineStage[Ctx, CmdBelow, CmdAbove, EvtBelow, EvtAbove] =
    left | right
}

abstract class SymmetricPipelineStage[Context, Below, Above] extends PipelineStage[Context, Below, Above, Below, Above]

abstract class PipelineStage[Context, CmdBelow, CmdAbove, EvtBelow, EvtAbove] { left ⇒
  protected[io] def apply(ctx: Context): PipePair[CmdBelow, CmdAbove, EvtBelow, EvtAbove]

  def >>[CmdAboveAbove, EvtAboveAbove, AboveContext <: Context] //
  (right: PipelineStage[_ >: AboveContext, CmdAbove, CmdAboveAbove, EvtAbove, EvtAboveAbove]) //
  : PipelineStage[AboveContext, CmdBelow, CmdAboveAbove, EvtBelow, EvtAboveAbove] =
    new PipelineStage[AboveContext, CmdBelow, CmdAboveAbove, EvtBelow, EvtAboveAbove] {
      protected[io] override def apply(ctx: AboveContext): PipePair[CmdBelow, CmdAboveAbove, EvtBelow, EvtAboveAbove] = {
        val leftPL = left(ctx)
        val rightPL = right(ctx)
        new PipePair[CmdBelow, CmdAboveAbove, EvtBelow, EvtAboveAbove] {
          override val commandPipeline = { a: CmdAboveAbove ⇒
            val output = Vector.newBuilder[Either[CmdBelow, EvtAboveAbove]]
            def rec(input: Iterable[Either[CmdAbove, EvtAboveAbove]]): Unit = {
              input foreach {
                case Left(cmd) ⇒
                  leftPL.commandPipeline(cmd) foreach {
                    case l @ Left(_) ⇒ output += l.asInstanceOf[Left[CmdBelow, EvtAboveAbove]]
                    case Right(evt)  ⇒ rec(rightPL.eventPipeline(evt))
                  }
                case r @ Right(_) ⇒ output += r.asInstanceOf[Right[CmdBelow, EvtAboveAbove]]
              }
            }
            rec(rightPL.commandPipeline(a))
            output.result
          }
          override val eventPipeline = { b: EvtBelow ⇒
            val output = Vector.newBuilder[Either[CmdBelow, EvtAboveAbove]]
            def rec(input: Iterable[Either[CmdBelow, EvtAbove]]): Unit = {
              input foreach {
                case l @ Left(_) ⇒ output += l.asInstanceOf[Left[CmdBelow, EvtAboveAbove]]
                case Right(evt) ⇒
                  rightPL.eventPipeline(evt) foreach {
                    case Left(cmd)    ⇒ rec(leftPL.commandPipeline(cmd))
                    case r @ Right(_) ⇒ output += r.asInstanceOf[Right[CmdBelow, EvtAboveAbove]]
                  }
              }
            }
            rec(leftPL.eventPipeline(b))
            output.result
          }
        }
      }
    }

  def |[RightContext <: Context] //
  (right: PipelineStage[_ >: RightContext, CmdBelow, CmdAbove, EvtBelow, EvtAbove]) //
  : PipelineStage[RightContext, CmdBelow, CmdAbove, EvtBelow, EvtAbove] =
    new PipelineStage[RightContext, CmdBelow, CmdAbove, EvtBelow, EvtAbove] {
      override def apply(ctx: RightContext): PipePair[CmdBelow, CmdAbove, EvtBelow, EvtAbove] =
        new PipePair[CmdBelow, CmdAbove, EvtBelow, EvtAbove] {
          override val commandPipeline = left(ctx).commandPipeline
          override val eventPipeline = right(ctx).eventPipeline
        }
    }
}
