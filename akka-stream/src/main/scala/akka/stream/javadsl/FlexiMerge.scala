/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.javadsl

import scala.annotation.varargs
import akka.stream.scaladsl
import akka.stream.scaladsl.FlexiMerge.ReadAllInputsBase
import scala.collection.immutable
import java.util.{ List ⇒ JList }
import akka.japi.Util.immutableIndexedSeq
import akka.stream._

object FlexiMerge {

  sealed trait ReadCondition

  /**
   * Read condition for the [[State]] that will be
   * fulfilled when there are elements for one specific upstream
   * input.
   *
   * It is not allowed to use a handle that has been cancelled or
   * has been completed. `IllegalArgumentException` is thrown if
   * that is not obeyed.
   */
  class Read(val input: InPort) extends ReadCondition

  /**
   * Read condition for the [[State]] that will be
   * fulfilled when there are elements for any of the given upstream
   * inputs.
   *
   * Cancelled and completed inputs are not used, i.e. it is allowed
   * to specify them in the list of `inputs`.
   */
  class ReadAny(val inputs: JList[InPort]) extends ReadCondition

  /**
   * Read condition for the [[FlexiMerge#State]] that will be
   * fulfilled when there are elements for any of the given upstream
   * inputs, however it differs from [[ReadAny]] in the case that both
   * the `preferred` and at least one other `secondary` input have demand,
   * the `preferred` input will always be consumed first.
   *
   * Cancelled and completed inputs are not used, i.e. it is allowed
   * to specify them in the list of `inputs`.
   */
  class ReadPreferred(val preferred: InPort, val secondaries: JList[InPort]) extends ReadCondition

  /**
   * Read condition for the [[FlexiMerge#State]] that will be
   * fulfilled when there are elements for *all* of the given upstream
   * inputs.
   *
   * The emited element the will be a [[ReadAllInputs]] object, which contains values for all non-cancelled inputs of this FlexiMerge.
   *
   * Cancelled inputs are not used, i.e. it is allowed to specify them in the list of `inputs`,
   * the resulting [[ReadAllInputs]] will then not contain values for this element, which can be
   * handled via supplying a default value instead of the value from the (now cancelled) input.
   */
  class ReadAll(val inputs: JList[InPort]) extends ReadCondition

  /**
   * Provides typesafe accessors to values from inputs supplied to [[ReadAll]].
   */
  final class ReadAllInputs(map: immutable.Map[InPort, Any]) extends ReadAllInputsBase {
    /** Returns the value for the given [[InputPort]], or `null` if this input was cancelled. */
    def get[T](input: Inlet[T]): T = getOrDefault(input, null)

    /** Returns the value for the given [[InputPort]], or `defaultValue`. */
    def getOrDefault[T, B >: T](input: Inlet[T], defaultValue: B): T = map.getOrElse(input, defaultValue).asInstanceOf[T]
  }

  /**
   * Context that is passed to the methods of [[State]] and [[CompletionHandling]].
   * The context provides means for performing side effects, such as emitting elements
   * downstream.
   */
  trait MergeLogicContext[Out] {
    /**
     * @return `true` if at least one element has been requested by downstream (output).
     */
    def isDemandAvailable: Boolean

    /**
     * Emit one element downstream. It is only allowed to `emit` when
     * [[#isDemandAvailable]] is `true`, otherwise `IllegalArgumentException`
     * is thrown.
     */
    def emit(elem: Out): Unit

    /**
     * Complete this stream succesfully. Upstream subscriptions will be cancelled.
     */
    def complete(): Unit

    /**
     * Complete this stream with failure. Upstream subscriptions will be cancelled.
     */
    def error(cause: Throwable): Unit

    /**
     * Cancel a specific upstream input stream.
     */
    def cancel(input: InPort): Unit

    /**
     * Replace current [[CompletionHandling]].
     */
    def changeCompletionHandling(completion: CompletionHandling[Out]): Unit
  }

  /**
   * How to handle completion or error from upstream input.
   *
   * The `onComplete` method is called when an upstream input was completed sucessfully.
   * It returns next behavior or [[MergeLogic#sameState]] to keep current behavior.
   * A completion can be propagated downstream with [[MergeLogicContext#complete]],
   * or it can be swallowed to continue with remaining inputs.
   *
   * The `onError` method is called when an upstream input was completed sucessfully.
   * It returns next behavior or [[MergeLogic#sameState]] to keep current behavior.
   * An error can be propagated downstream with [[MergeLogicContext#error]],
   * or it can be swallowed to continue with remaining inputs.
   */
  abstract class CompletionHandling[Out] {
    def onComplete(ctx: MergeLogicContext[Out], input: InPort): State[Out]
    def onError(ctx: MergeLogicContext[Out], input: InPort, cause: Throwable): State[Out]
  }

  /**
   * Definition of which inputs to read from and how to act on the read elements.
   * When an element has been read [[#onInput]] is called and then it is ensured
   * that downstream has requested at least one element, i.e. it is allowed to
   * emit at least one element downstream with [[MergeLogicContext#emit]].
   *
   * The `onInput` method is called when an `element` was read from the `input`.
   * The method returns next behavior or [[MergeLogic#sameState]] to keep current behavior.
   */
  abstract class State[Out](val condition: ReadCondition) {
    def onInput(ctx: MergeLogicContext[Out], input: InPort, element: AnyRef): State[Out]
  }

  /**
   * The possibly stateful logic that reads from input via the defined [[State]] and
   * handles completion and error via the defined [[CompletionHandling]].
   *
   * Concrete instance is supposed to be created by implementing [[FlexiMerge#createMergeLogic]].
   */
  abstract class MergeLogic[Out] {
    def initialState: State[Out]
    def initialCompletionHandling: CompletionHandling[Out] = defaultCompletionHandling
    /**
     * Return this from [[State]] `onInput` to use same state for next element.
     */
    def sameState: State[Out] = FlexiMerge.sameStateInstance.asInstanceOf[State[Out]]

    /**
     * Convenience to create a [[Read]] condition.
     */
    def read(input: InPort): Read = new Read(input)

    /**
     * Convenience to create a [[ReadAny]] condition.
     */
    @varargs def readAny(inputs: InPort*): ReadAny = {
      import scala.collection.JavaConverters._
      new ReadAny(inputs.asJava)
    }

    /**
     * Convenience to create a [[ReadPreferred]] condition.
     */
    @varargs def readPreferred(preferred: InPort, secondaries: InPort*): ReadPreferred = {
      import scala.collection.JavaConverters._
      new ReadPreferred(preferred, secondaries.asJava)
    }

    /**
     * Convenience to create a [[ReadAll]] condition.
     */
    @varargs def readAll(inputs: InPort*): ReadAll = {
      import scala.collection.JavaConverters._
      new ReadAll(inputs.asJava)
    }

    /**
     * Will continue to operate until a read becomes unsatisfiable, then it completes.
     * Errors are immediately propagated.
     */
    def defaultCompletionHandling: CompletionHandling[Out] =
      new CompletionHandling[Out] {
        override def onComplete(ctx: MergeLogicContext[Out], input: InPort): State[Out] =
          sameState
        override def onError(ctx: MergeLogicContext[Out], input: InPort, cause: Throwable): State[Out] = {
          ctx.error(cause)
          sameState
        }
      }

    /**
     * Completes as soon as any input completes.
     * Errors are immediately propagated.
     */
    def eagerClose: CompletionHandling[Out] =
      new CompletionHandling[Out] {
        override def onComplete(ctx: MergeLogicContext[Out], input: InPort): State[Out] = {
          ctx.complete()
          sameState
        }
        override def onError(ctx: MergeLogicContext[Out], input: InPort, cause: Throwable): State[Out] = {
          ctx.error(cause)
          sameState
        }
      }
  }

  private val sameStateInstance = new State[Any](new ReadAny(java.util.Collections.emptyList[InPort])) {
    override def onInput(ctx: MergeLogicContext[Any], input: InPort, element: AnyRef): State[Any] =
      throw new UnsupportedOperationException("SameState.onInput should not be called")

    override def toString: String = "SameState"
  }

  /**
   * INTERNAL API
   */
  private[akka] object Internal {
    class MergeLogicWrapper[Out](delegate: MergeLogic[Out]) extends scaladsl.FlexiMerge.MergeLogic[Out] {

      override def initialState: State[_] = wrapState(delegate.initialState)

      override def initialCompletionHandling: this.CompletionHandling =
        wrapCompletionHandling(delegate.initialCompletionHandling)

      private def wrapState(delegateState: FlexiMerge.State[Out]): State[_] =
        if (sameStateInstance == delegateState)
          SameState
        else
          State(convertReadCondition(delegateState.condition)) { (ctx, inputHandle, elem) ⇒
            val newDelegateState =
              delegateState.onInput(new MergeLogicContextWrapper(ctx), inputHandle, elem)
            wrapState(newDelegateState)
          }

      private def wrapCompletionHandling(
        delegateCompletionHandling: FlexiMerge.CompletionHandling[Out]): CompletionHandling =
        CompletionHandling(
          onComplete = (ctx, inputHandle) ⇒ {
            val newDelegateState = delegateCompletionHandling.onComplete(
              new MergeLogicContextWrapper(ctx), inputHandle)
            wrapState(newDelegateState)
          },
          onError = (ctx, inputHandle, cause) ⇒ {
            val newDelegateState = delegateCompletionHandling.onError(
              new MergeLogicContextWrapper(ctx), inputHandle, cause)
            wrapState(newDelegateState)
          })

      class MergeLogicContextWrapper[In](delegate: MergeLogicContext) extends FlexiMerge.MergeLogicContext[Out] {
        override def isDemandAvailable: Boolean = delegate.isDemandAvailable
        override def emit(elem: Out): Unit = delegate.emit(elem)
        override def complete(): Unit = delegate.complete()
        override def error(cause: Throwable): Unit = delegate.error(cause)
        override def cancel(input: InPort): Unit = delegate.cancel(input)
        override def changeCompletionHandling(completion: FlexiMerge.CompletionHandling[Out]): Unit =
          delegate.changeCompletionHandling(wrapCompletionHandling(completion))
      }

    }

    private def toAnyRefSeq(l: JList[InPort]) = immutableIndexedSeq(l).asInstanceOf[immutable.Seq[Inlet[AnyRef]]]

    def convertReadCondition(condition: ReadCondition): scaladsl.FlexiMerge.ReadCondition[AnyRef] = {
      condition match {
        case r: ReadAny       ⇒ scaladsl.FlexiMerge.ReadAny(toAnyRefSeq(r.inputs))
        case r: ReadPreferred ⇒ scaladsl.FlexiMerge.ReadPreferred(r.preferred.asInstanceOf[Inlet[AnyRef]], toAnyRefSeq(r.secondaries))
        case r: Read          ⇒ scaladsl.FlexiMerge.Read(r.input.asInstanceOf[Inlet[AnyRef]])
        case r: ReadAll       ⇒ scaladsl.FlexiMerge.ReadAll(new ReadAllInputs(_), toAnyRefSeq(r.inputs): _*).asInstanceOf[scaladsl.FlexiMerge.ReadCondition[AnyRef]]
      }
    }

  }
}

/**
 * Base class for implementing custom merge junctions.
 * Such a junction always has one [[#out]] port and one or more input ports.
 * The input ports are to be defined in the concrete subclass and are created with
 * [[#createInputPort]].
 *
 * The concrete subclass must implement [[#createMergeLogic]] to define the [[FlexiMerge#MergeLogic]]
 * that will be used when reading input elements and emitting output elements.
 * The [[FlexiMerge#MergeLogic]] instance may be stateful, but the ``FlexiMerge`` instance
 * must not hold mutable state, since it may be shared across several materialized ``FlowGraph``
 * instances.
 *
 * Note that a `FlexiMerge` instance can only be used at one place in the `FlowGraph` (one vertex).
 *
 * @param attributes optional attributes for this vertex
 */
abstract class FlexiMerge[S <: Shape](val attributes: OperationAttributes) {
  import FlexiMerge._

  def this() = this(OperationAttributes.none)

  // FIXME can the type parameter make sense at all?
  def createMergeLogic(): MergeLogic[_]

  override def toString = attributes.asScala.nameLifted match {
    case Some(n) ⇒ n
    case None    ⇒ super.toString
  }
}
