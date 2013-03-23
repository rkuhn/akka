/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.io

import akka.japi.Function
import java.lang.{ Iterable ⇒ JIterable }
import scala.annotation.tailrec

/**
 * Events flow from Left/Below to Right/Above.
 * Commands flow from Right/Above to Left/Below.
 */
abstract class Pipelines[Below, Above] {
  def commandPipeline: Above ⇒ Iterable[Either[Below, Above]]
  def eventPipeline: Below ⇒ Iterable[Either[Below, Above]]
}

object Pipelines {
  def apply[Below, Above](commandPL: Above ⇒ Iterable[Either[Below, Above]], eventPL: Below ⇒ Iterable[Either[Below, Above]]) =
    new Pipelines[Below, Above] {
      override def commandPipeline = commandPL
      override def eventPipeline = eventPL
    }
  def create[Below, Above](commandPL: Function[Above, JIterable[Either[Below, Above]]], eventPL: Function[Below, JIterable[Either[Below, Above]]]) =
    new Pipelines[Below, Above] {
      import scala.collection.JavaConverters._
      override def commandPipeline = (a: Above) ⇒ commandPL(a).asScala
      override def eventPipeline = (b: Below) ⇒ eventPL(b).asScala
    }
}

trait PipelineContext {

}

abstract class PipelineStage[Context <: PipelineContext, Below, Above] { left ⇒
  def apply(ctx: Context): Pipelines[Below, Above]

  def >>[AboveAbove, AboveContext <: Context] //
  (right: PipelineStage[_ >: AboveContext, Above, AboveAbove]): PipelineStage[AboveContext, Below, AboveAbove] =
    new PipelineStage[AboveContext, Below, AboveAbove] {
      override def apply(ctx: AboveContext): Pipelines[Below, AboveAbove] = {
        val leftPL = left(ctx)
        val rightPL = right(ctx)
        new Pipelines[Below, AboveAbove] {
          override def commandPipeline = { a: AboveAbove ⇒
            val output = Vector.newBuilder[Either[Below, AboveAbove]]
            def rec(input: Iterable[Either[Above, AboveAbove]]): Unit = {
              input foreach {
                case Left(cmd) ⇒
                  leftPL.commandPipeline(cmd) foreach {
                    case l @ Left(_) ⇒ output += l.asInstanceOf[Left[Below, AboveAbove]]
                    case Right(evt)  ⇒ rec(rightPL.eventPipeline(evt))
                  }
                case r @ Right(_) ⇒ output += r.asInstanceOf[Right[Below, AboveAbove]]
              }
            }
            rec(rightPL.commandPipeline(a))
            output.result
          }
          override def eventPipeline = { b: Below ⇒
            val output = Vector.newBuilder[Either[Below, AboveAbove]]
            def rec(input: Iterable[Either[Below, Above]]): Unit = {
              input foreach {
                case l @ Left(_) ⇒ output += l.asInstanceOf[Left[Below, AboveAbove]]
                case Right(evt) ⇒
                  rightPL.eventPipeline(evt) foreach {
                    case Left(cmd)    ⇒ rec(leftPL.commandPipeline(cmd))
                    case r @ Right(_) ⇒ output += r.asInstanceOf[Right[Below, AboveAbove]]
                  }
              }
            }
            rec(leftPL.eventPipeline(b))
            output.result
          }
        }
      }
    }
}
