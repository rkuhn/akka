/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.actor.{ ActorRef, Props }
import akka.stream.impl._
import akka.stream.{ SinkShape, Inlet, Outlet, Graph }
import akka.stream.scaladsl.OperationAttributes._
import akka.stream.stage.{ TerminationDirective, Directive, Context, PushStage }
import org.reactivestreams.{ Publisher, Subscriber }
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.{ Promise, Future }
import scala.util.{ Success, Failure, Try }
import akka.stream.FlowMaterializer

/**
 * A `Sink` is a set of stream processing steps that has one open input and an attached output.
 * Can be used as a `Subscriber`
 */
final class Sink[-In, +Mat](m: StreamLayout.Module, val inlet: Inlet[In])
  extends Graph[SinkShape[In], Mat] {
  private[stream] override val module: StreamLayout.Module = m

  private[akka] def this(module: SinkModule[In @uncheckedVariance, Mat]) = this(module, module.inPort)

  override val shape: SinkShape[In] = SinkShape(inlet)

  private[stream] def carbonCopy(): Sink[In, Mat] = {
    val sinkCopy = module.carbonCopy()
    new Sink(sinkCopy.module, sinkCopy.inPorts(inlet).asInstanceOf[Inlet[In]])
  }

  /**
   * Connect this `Sink` to a `Source` and run it. The returned value is the materialized value
   * of the `Source`, e.g. the `Subscriber` of a [[SubscriberSource]].
   */
  def runWith[Mat2](source: Source[In, Mat2])(implicit materializer: FlowMaterializer): Mat =
    source.to(this).run()

  def mapMaterialized[Mat2](f: Mat ⇒ Mat2): Sink[In, Mat2] = {
    val sinkCopy = module.carbonCopy()
    new Sink(
      sinkCopy.module.transformMaterializedValue(f.asInstanceOf[Any ⇒ Any]),
      sinkCopy.inPorts(inlet).asInstanceOf[Inlet[In]])
  }

  def withAttributes(attr: OperationAttributes): Sink[In, Mat] = {
    val newModule = module.withAttributes(attr)
    new Sink(newModule, newModule.inPorts.head.asInstanceOf[Inlet[In]])
  }

}

object Sink extends SinkApply {

  /**
   * Helper to create [[Sink]] from `Subscriber`.
   */
  def apply[T](subscriber: Subscriber[T]): Sink[T, Unit] = new Sink(new SubscriberSink(subscriber))

  /**
   * Creates a `Sink` that is materialized to an [[akka.actor.ActorRef]] which points to an Actor
   * created according to the passed in [[akka.actor.Props]]. Actor created by the `props` should
   * be [[akka.stream.actor.ActorSubscriber]].
   */
  def apply[T](props: Props): Sink[T, ActorRef] = new Sink(new PropsSink(props))

  /**
   * A `Sink` that immediately cancels its upstream after materialization.
   */
  def cancelled[T]: Sink[T, Unit] = new Sink[Any, Unit](new CancelSink)

  /**
   * A `Sink` that materializes into a `Future` of the first value received.
   */
  def head[T]: Sink[T, Future[T]] = new Sink(HeadSink[T])

  /**
   * A `Sink` that materializes into a [[org.reactivestreams.Publisher]].
   * that can handle one [[org.reactivestreams.Subscriber]].
   */
  def publisher[T]: Sink[T, Publisher[T]] = new Sink(PublisherSink[T])

  /**
   * A `Sink` that materializes into a [[org.reactivestreams.Publisher]]
   * that can handle more than one [[org.reactivestreams.Subscriber]].
   */
  def fanoutPublisher[T](initialBufferSize: Int, maximumBufferSize: Int): Sink[T, Publisher[T]] =
    new Sink(new FanoutPublisherSink[T](initialBufferSize, maximumBufferSize))

  /**
   * A `Sink` that will consume the stream and discard the elements.
   */
  def ignore: Sink[Any, Unit] = new Sink(new BlackholeSink())

  /**
   * A `Sink` that will invoke the given procedure for each received element. The sink is materialized
   * into a [[scala.concurrent.Future]] will be completed with `Success` when reaching the
   * normal end of the stream, or completed with `Failure` if there is an error is signaled in
   * the stream..
   */
  def foreach[T](f: T ⇒ Unit): Sink[T, Future[Unit]] = {

    def newForeachStage(): (PushStage[T, Unit], Future[Unit]) = {
      val promise = Promise[Unit]()

      val stage = new PushStage[T, Unit] {
        override def onPush(elem: T, ctx: Context[Unit]): Directive = {
          f(elem)
          ctx.pull()
        }
        override def onUpstreamFailure(cause: Throwable, ctx: Context[Unit]): TerminationDirective = {
          promise.failure(cause)
          ctx.fail(cause)
        }
        override def onUpstreamFinish(ctx: Context[Unit]): TerminationDirective = {
          promise.success(())
          ctx.finish()
        }
      }

      (stage, promise.future)
    }

    Flow[T].section(name("foreach")) { section ⇒
      section.transformMaterializing(newForeachStage)
    }.to(Sink.ignore)

  }

  /**
   * A `Sink` that will invoke the given function for every received element, giving it its previous
   * output (or the given `zero` value) and the element as input.
   * The returned [[scala.concurrent.Future]] will be completed with value of the final
   * function evaluation when the input stream ends, or completed with `Failure`
   * if there is an error is signaled in the stream.
   */
  def fold[U, T](zero: U)(f: (U, T) ⇒ U): Sink[T, Future[U]] = {

    def newFoldStage(): (PushStage[T, U], Future[U]) = {
      val promise = Promise[U]()

      val stage = new PushStage[T, U] {
        private var aggregator = zero

        override def onPush(elem: T, ctx: Context[U]): Directive = {
          aggregator = f(aggregator, elem)
          ctx.pull()
        }

        override def onUpstreamFailure(cause: Throwable, ctx: Context[U]): TerminationDirective = {
          promise.failure(cause)
          ctx.fail(cause)
        }

        override def onUpstreamFinish(ctx: Context[U]): TerminationDirective = {
          promise.success(aggregator)
          ctx.finish()
        }
      }

      (stage, promise.future)
    }

    Flow[T].section(name("fold")) { section ⇒
      section.transformMaterializing(newFoldStage)
    }.to(Sink.ignore)

  }

  /**
   * A `Sink` that when the flow is completed, either through an error or normal
   * completion, apply the provided function with [[scala.util.Success]]
   * or [[scala.util.Failure]].
   */
  def onComplete[T](callback: Try[Unit] ⇒ Unit): Sink[T, Unit] = {

    def newOnCompleteStage(): PushStage[T, Unit] = {
      new PushStage[T, Unit] {
        override def onPush(elem: T, ctx: Context[Unit]): Directive = ctx.pull()
        override def onUpstreamFailure(cause: Throwable, ctx: Context[Unit]): TerminationDirective = {
          callback(Failure(cause))
          ctx.fail(cause)
        }
        override def onUpstreamFinish(ctx: Context[Unit]): TerminationDirective = {
          callback(Success[Unit](()))
          ctx.finish()
        }
      }
    }

    Flow[T].section(name("onComplete")) { section ⇒
      section.transform(newOnCompleteStage)
    }.to(Sink.ignore)
  }
}