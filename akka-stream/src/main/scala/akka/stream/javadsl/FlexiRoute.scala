/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.javadsl

import scala.annotation.varargs
import akka.stream.scaladsl
import scala.collection.immutable
import java.util.{ List ⇒ JList }
import akka.japi.Util.immutableIndexedSeq
import akka.stream._

object FlexiRoute {

  sealed trait DemandCondition[T]

  /**
   * Demand condition for the [[State]] that will be
   * fulfilled when there are requests for elements from one specific downstream
   * output.
   *
   * It is not allowed to use a handle that has been cancelled or
   * has been completed. `IllegalArgumentException` is thrown if
   * that is not obeyed.
   */
  class DemandFrom(val output: OutPort) extends DemandCondition[OutPort]

  /**
   * Demand condition for the [[State]] that will be
   * fulfilled when there are requests for elements from any of the given downstream
   * outputs.
   *
   * Cancelled and completed inputs are not used, i.e. it is allowed
   * to specify them in the list of `outputs`.
   */
  class DemandFromAny(val outputs: JList[OutPort]) extends DemandCondition[OutPort]

  /**
   * Demand condition for the [[State]] that will be
   * fulfilled when there are requests for elements from all of the given downstream
   * outputs.
   *
   * Cancelled and completed outputs are not used, i.e. it is allowed
   * to specify them in the list of `outputs`.
   */
  class DemandFromAll(val outputs: JList[OutPort]) extends DemandCondition[Unit]

  /**
   * Context that is passed to the functions of [[State]] and [[CompletionHandling]].
   * The context provides means for performing side effects, such as emitting elements
   * downstream.
   */
  trait RouteLogicContext {
    /**
     * @return `true` if at least one element has been requested by the given downstream (output).
     */
    def isDemandAvailable(output: OutPort): Boolean

    /**
     * Emit one element downstream. It is only allowed to `emit` when
     * [[#isDemandAvailable]] is `true` for the given `output`, otherwise
     * `IllegalArgumentException` is thrown.
     */
    def emit(output: OutPort, elem: AnyRef): Unit

    /**
     * Complete the given downstream successfully.
     */
    def complete(output: OutPort): Unit

    /**
     * Complete all downstreams successfully and cancel upstream.
     */
    def complete(): Unit

    /**
     * Complete the given downstream with failure.
     */
    def error(output: OutPort, cause: Throwable): Unit

    /**
     * Complete all downstreams with failure and cancel upstream.
     */
    def error(cause: Throwable): Unit

    /**
     * Replace current [[CompletionHandling]].
     */
    def changeCompletionHandling(completion: CompletionHandling): Unit
  }

  /**
   * How to handle completion or error from upstream input and how to
   * handle cancel from downstream output.
   *
   * The `onComplete` method is called the upstream input was completed successfully.
   * It returns next behavior or [[#sameState]] to keep current behavior.
   *
   * The `onError` method is called when the upstream input was completed with failure.
   * It returns next behavior or [[#SameState]] to keep current behavior.
   *
   * The `onCancel` method is called when a downstream output cancels.
   * It returns next behavior or [[#sameState]] to keep current behavior.
   */
  abstract class CompletionHandling {
    def onComplete(ctx: RouteLogicContext): Unit
    def onError(ctx: RouteLogicContext, cause: Throwable): Unit
    def onCancel(ctx: RouteLogicContext, output: OutPort): State[_]
  }

  /**
   * Definition of which outputs that must have requested elements and how to act
   * on the read elements. When an element has been read [[#onInput]] is called and
   * then it is ensured that the specified downstream outputs have requested at least
   * one element, i.e. it is allowed to emit at least one element downstream with
   * [[RouteLogicContext#emit]].
   *
   * The `onInput` method is called when an `element` was read from upstream.
   * The function returns next behavior or [[#sameState]] to keep current behavior.
   */
  abstract class State[T](val condition: DemandCondition[T]) {
    def onInput(ctx: RouteLogicContext, preferredOutput: T, element: AnyRef): State[_]
  }

  /**
   * The possibly stateful logic that reads from the input and enables emitting to downstream
   * via the defined [[State]]. Handles completion, error and cancel via the defined
   * [[CompletionHandling]].
   *
   * Concrete instance is supposed to be created by implementing [[FlexiRoute#createRouteLogic]].
   */
  abstract class RouteLogic {

    def initialState: State[_]
    def initialCompletionHandling: CompletionHandling = defaultCompletionHandling

    /**
     * Return this from [[State]] `onInput` to use same state for next element.
     */
    def sameState[T]: State[T] = FlexiRoute.sameStateInstance.asInstanceOf[State[T]]

    /**
     * Convenience to create a [[DemandFromAny]] condition.
     */
    @varargs def demandFromAny(outputs: OutPort*): DemandFromAny = {
      import scala.collection.JavaConverters._
      new DemandFromAny(outputs.asJava)
    }

    /**
     * Convenience to create a [[DemandFromAll]] condition.
     */
    @varargs def demandFromAll(outputs: OutPort*): DemandFromAll = {
      import scala.collection.JavaConverters._
      new DemandFromAll(outputs.asJava)
    }

    /**
     * Convenience to create a [[DemandFrom]] condition.
     */
    def demandFrom(output: OutPort): DemandFrom = new DemandFrom(output)

    /**
     * When an output cancels it continues with remaining outputs.
     * Error or completion from upstream are immediately propagated.
     */
    def defaultCompletionHandling: CompletionHandling =
      new CompletionHandling {
        override def onComplete(ctx: RouteLogicContext): Unit = ()
        override def onError(ctx: RouteLogicContext, cause: Throwable): Unit = ()
        override def onCancel(ctx: RouteLogicContext, output: OutPort): State[_] =
          sameState
      }

    /**
     * Completes as soon as any output cancels.
     * Error or completion from upstream are immediately propagated.
     */
    def eagerClose[A]: CompletionHandling =
      new CompletionHandling {
        override def onComplete(ctx: RouteLogicContext): Unit = ()
        override def onError(ctx: RouteLogicContext, cause: Throwable): Unit = ()
        override def onCancel(ctx: RouteLogicContext, output: OutPort): State[_] = {
          ctx.complete()
          sameState
        }
      }
  }

  private val sameStateInstance = new State(new DemandFromAny(java.util.Collections.emptyList[OutPort])) {
    override def onInput(ctx: RouteLogicContext, output: OutPort, element: AnyRef): State[_] =
      throw new UnsupportedOperationException("SameState.onInput should not be called")

    override def toString: String = "SameState"
  }

  /**
   * INTERNAL API
   */
  private[akka] object Internal {
    class RouteLogicWrapper(delegate: RouteLogic) extends scaladsl.FlexiRoute.RouteLogic[AnyRef] {

      override def initialState: this.State[_] = wrapState(delegate.initialState)

      override def initialCompletionHandling: this.CompletionHandling =
        wrapCompletionHandling(delegate.initialCompletionHandling)

      private def wrapState[T](delegateState: FlexiRoute.State[T]): State[T] =
        if (sameStateInstance == delegateState)
          SameState
        else
          State(convertDemandCondition(delegateState.condition)) { (ctx, outputHandle, elem) ⇒
            val newDelegateState =
              delegateState.onInput(new RouteLogicContextWrapper(ctx), outputHandle, elem)
            wrapState(newDelegateState)
          }

      private def wrapCompletionHandling[Out](
        delegateCompletionHandling: FlexiRoute.CompletionHandling): CompletionHandling =
        CompletionHandling(
          onComplete = ctx ⇒ {
            delegateCompletionHandling.onComplete(new RouteLogicContextWrapper(ctx))
          },
          onError = (ctx, cause) ⇒ {
            delegateCompletionHandling.onError(new RouteLogicContextWrapper(ctx), cause)
          },
          onCancel = (ctx, outputHandle) ⇒ {
            val newDelegateState = delegateCompletionHandling.onCancel(
              new RouteLogicContextWrapper(ctx), outputHandle)
            wrapState(newDelegateState)
          })

      class RouteLogicContextWrapper(delegate: RouteLogicContext) extends FlexiRoute.RouteLogicContext {
        override def isDemandAvailable(output: OutPort): Boolean = delegate.isDemandAvailable(output)
        override def emit(output: OutPort, elem: AnyRef): Unit = delegate.emit(output.asInstanceOf[Outlet[AnyRef]])(elem)
        override def complete(): Unit = delegate.complete()
        override def complete(output: OutPort): Unit = delegate.complete(output)
        override def error(cause: Throwable): Unit = delegate.error(cause)
        override def error(output: OutPort, cause: Throwable): Unit = delegate.error(output, cause)
        override def changeCompletionHandling(completion: FlexiRoute.CompletionHandling): Unit =
          delegate.changeCompletionHandling(wrapCompletionHandling(completion))
      }

    }

    private def toAnyRefSeq(l: JList[OutPort]) = immutableIndexedSeq(l).asInstanceOf[immutable.Seq[Outlet[AnyRef]]]

    def convertDemandCondition[T](condition: DemandCondition[T]): scaladsl.FlexiRoute.DemandCondition[T] =
      condition match {
        case c: DemandFromAny ⇒ scaladsl.FlexiRoute.DemandFromAny(toAnyRefSeq(c.outputs))
        case c: DemandFromAll ⇒ scaladsl.FlexiRoute.DemandFromAll(toAnyRefSeq(c.outputs))
        case c: DemandFrom    ⇒ scaladsl.FlexiRoute.DemandFrom(c.output.asInstanceOf[Outlet[AnyRef]])
      }

  }
}

/**
 * Base class for implementing custom route junctions.
 * Such a junction always has one [[#in]] port and one or more output ports.
 * The output ports are to be defined in the concrete subclass and are created with
 * [[#createOutputPort]].
 *
 * The concrete subclass must implement [[#createRouteLogic]] to define the [[FlexiRoute#RouteLogic]]
 * that will be used when reading input elements and emitting output elements.
 * The [[FlexiRoute#RouteLogic]] instance may be stateful, but the ``FlexiRoute`` instance
 * must not hold mutable state, since it may be shared across several materialized ``FlowGraph``
 * instances.
 *
 * Note that a `FlexiRoute` instance can only be used at one place in the `FlowGraph` (one vertex).
 *
 * @param attributes optional attributes for this vertex
 */
abstract class FlexiRoute[In, Out](val attributes: OperationAttributes) {
  import FlexiRoute._

  def this() = this(OperationAttributes.none)

  /**
   * Create the stateful logic that will be used when reading input elements
   * and emitting output elements. Create a new instance every time.
   */
  def createRouteLogic(): RouteLogic

  override def toString = attributes.asScala.nameLifted match {
    case Some(n) ⇒ n
    case None    ⇒ super.toString
  }

}
