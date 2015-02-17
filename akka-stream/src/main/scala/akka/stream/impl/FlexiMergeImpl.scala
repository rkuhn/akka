/**
 * Copyright (C) 2014-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.scaladsl.FlexiMerge.{ Read, ReadAll, ReadAny, ReadPreferred }
import akka.stream.scaladsl.Graphs.{ Ports, InPort }
import akka.stream.{ MaterializerSettings, scaladsl }

import scala.collection.breakOut
import scala.collection.immutable
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
private[akka] class FlexiMergeImpl[T, P <: Ports](
  _settings: MaterializerSettings,
  ports: P,
  mergeLogic: scaladsl.FlexiMerge.MergeLogic[T]) extends FanIn(_settings, ports.inlets.size) {

  private type StateT = mergeLogic.State[_]
  private type CompletionT = mergeLogic.CompletionHandling
  private type InP = StreamLayout.InPort

  val inputMapping: Map[Int, InP] = ports.inlets.zipWithIndex.map(_.swap)(breakOut)
  val indexOf: Map[InP, Int] = ports.inlets.zipWithIndex.toMap

  private var behavior: StateT = _
  private def anyBehavior = behavior.asInstanceOf[mergeLogic.State[Any]]
  private var completion: CompletionT = _

  override protected val inputBunch = new FanIn.InputBunch(inputCount, settings.maxInputBufferSize, this) {
    override def onError(input: Int, e: Throwable): Unit = {
      changeBehavior(
        try completion.onError(ctx, inputMapping(input), e)
        catch {
          case NonFatal(e) ⇒ fail(e); mergeLogic.SameState
        })
      cancel(input)
    }

    override def onDepleted(input: Int): Unit =
      triggerCompletion(inputMapping(input))
  }

  private val ctx: mergeLogic.MergeLogicContext = new mergeLogic.MergeLogicContext {
    override def isDemandAvailable: Boolean = primaryOutputs.demandAvailable

    override def emit(elem: T): Unit = {
      require(primaryOutputs.demandAvailable, "emit not allowed when no demand available")
      primaryOutputs.enqueueOutputElement(elem)
    }

    override def complete(): Unit = {
      inputBunch.cancel()
      primaryOutputs.complete()
      context.stop(self)
    }

    override def error(cause: Throwable): Unit = fail(cause)

    override def cancel(input: InP): Unit = inputBunch.cancel(indexOf(input))

    override def changeCompletionHandling(newCompletion: CompletionT): Unit =
      FlexiMergeImpl.this.changeCompletionHandling(newCompletion)

  }

  private def markInputs(inputs: Array[InP]): Unit = {
    inputBunch.unmarkAllInputs()
    var i = 0
    while (i < inputs.length) {
      val id = indexOf(inputs(i))
      if (include(id))
        inputBunch.markInput(id)
      i += 1
    }
  }

  private def include(port: InP): Boolean = include(indexOf(port))

  private def include(portIndex: Int): Boolean =
    inputMapping.contains(portIndex) && !inputBunch.isCancelled(portIndex) && !inputBunch.isDepleted(portIndex)

  private def precondition: TransferState = {
    behavior.condition match {
      case _: ReadAny[_] | _: ReadPreferred[_] | _: Read[_] ⇒ inputBunch.AnyOfMarkedInputs && primaryOutputs.NeedsDemand
      case _: ReadAll[_]                                    ⇒ inputBunch.AllOfMarkedInputs && primaryOutputs.NeedsDemand
    }
  }

  private def changeCompletionHandling(newCompletion: CompletionT): Unit = completion = newCompletion

  private def changeBehavior(newBehavior: StateT): Unit =
    if (newBehavior != mergeLogic.SameState && (newBehavior ne behavior)) {
      behavior = newBehavior
      behavior.condition match {
        case read: ReadAny[_] ⇒
          markInputs(read.inputs.toArray)
        case r: ReadPreferred[_] ⇒
          markInputs(r.secondaries.toArray)
          inputBunch.markInput(indexOf(r.preferred))
        case read: ReadAll[_] ⇒
          markInputs(read.inputs.toArray)
        case Read(input) ⇒
          val inputIdx = indexOf(input)
          require(inputMapping.contains(inputIdx), s"Unknown input handle $input")
          require(!inputBunch.isCancelled(inputIdx), s"Read not allowed from cancelled $input")
          require(!inputBunch.isDepleted(inputIdx), s"Read not allowed from depleted $input")
          inputBunch.unmarkAllInputs()
          inputBunch.markInput(inputIdx)
      }
    }

  changeBehavior(mergeLogic.initialState)
  changeCompletionHandling(mergeLogic.initialCompletionHandling)

  nextPhase(TransferPhase(precondition) { () ⇒
    behavior.condition match {
      case read: ReadAny[t] ⇒
        val id = inputBunch.idToDequeue()
        val elem = inputBunch.dequeueAndYield(id)
        val inputHandle = inputMapping(id)
        changeBehavior(anyBehavior.onInput(ctx, inputHandle, elem))
        triggerCompletionAfterRead(inputHandle)
      case r: ReadPreferred[_] ⇒
        val id = indexOf(r.preferred)
        val elem = inputBunch.dequeuePrefering(id)
        val inputHandle = inputMapping(id)
        changeBehavior(anyBehavior.onInput(ctx, inputHandle, elem))
        triggerCompletionAfterRead(inputHandle)
      case Read(input) ⇒
        val elem = inputBunch.dequeue(indexOf(input))
        changeBehavior(anyBehavior.onInput(ctx, input, elem))
        triggerCompletionAfterRead(input)
      case read: ReadAll[t] ⇒
        val inputHandles = read.inputs

        val values = inputHandles.collect {
          case input if include(input) ⇒ input → inputBunch.dequeue(indexOf(input))
        }

        changeBehavior(anyBehavior.onInput(ctx, inputHandles.head, read.mkResult(Map(values: _*))))

        // must be triggered after emitting the accumulated out value
        triggerCompletionAfterRead(inputHandles)
    }

  })

  private def triggerCompletionAfterRead(inputs: Seq[InP]): Unit = {
    var j = 0
    while (j < inputs.length) {
      triggerCompletionAfterRead(inputs(j))
      j += 1
    }
  }

  private def triggerCompletionAfterRead(inputHandle: InP): Unit =
    if (inputBunch.isDepleted(indexOf(inputHandle)))
      triggerCompletion(inputHandle)

  private def triggerCompletion(in: InP): Unit =
    changeBehavior(
      try completion.onComplete(ctx, in)
      catch {
        case NonFatal(e) ⇒ fail(e); mergeLogic.SameState
      })

}
