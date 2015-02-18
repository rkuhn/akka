/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ ActorRef, Props }
import akka.stream.impl.StreamLayout.{ Mapping, Module, OutPort, InPort }
import akka.stream.scaladsl.OperationAttributes._
import akka.stream.scaladsl.{ Graphs, Sink, OperationAttributes, Source }
import akka.stream.stage._
import org.reactivestreams.{ Processor, Publisher, Subscriber, Subscription }

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }

trait SinkModule[-In, Mat] extends StreamLayout.Module {

  /**
   * This method is only used for Sinks that return true from [[#isActive]], which then must
   * implement it.
   */
  def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Subscriber[In] @uncheckedVariance, Mat)

  /**
   * This method indicates whether this Sink can create a Subscriber instead of being
   * attached to a Publisher. This is only used if the Flow does not contain any
   * operations.
   */

  override def subModules: Set[Module] = Set.empty
  override def downstreams: Map[OutPort, InPort] = Map.empty
  override def upstreams: Map[InPort, OutPort] = Map.empty

  val inPort: Graphs.InPort[In] = new Graphs.InPort[In]("Sink.in")
  override def inPorts: Set[InPort] = Set(inPort)
  override def outPorts: Set[OutPort] = Set.empty

  protected def newInstance: SinkModule[In, Mat]
  override def carbonCopy: () ⇒ Mapping = () ⇒ {
    val copy = newInstance
    Mapping(copy, Map(inPort -> copy.inPort), Map.empty)
  }
}

object PublisherSink {
  def apply[T](): PublisherSink[T] = new PublisherSink[T]
  def withFanout[T](initialBufferSize: Int, maximumBufferSize: Int): FanoutPublisherSink[T] =
    new FanoutPublisherSink[T](initialBufferSize, maximumBufferSize)
}

/**
 * Holds the downstream-most [[org.reactivestreams.Publisher]] interface of the materialized flow.
 * The stream will not have any subscribers attached at this point, which means that after prefetching
 * elements to fill the internal buffers it will assert back-pressure until
 * a subscriber connects and creates demand for elements to be emitted.
 */
class PublisherSink[In](val attributes: OperationAttributes = OperationAttributes.none) extends SinkModule[In, Publisher[In]] {

  override def toString: String = "PublisherSink"

  /**
   * This method is only used for Sinks that return true from [[#isActive]], which then must
   * implement it.
   */
  override def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Subscriber[In], Publisher[In]) = {
    val pub = new VirtualPublisher[In]
    val sub = new VirtualSubscriber[In](pub)
    (sub, pub)
  }

  override protected def newInstance: SinkModule[In, Publisher[In]] = new PublisherSink[In](attributes)
  override def withAttributes(attr: OperationAttributes): Module = new PublisherSink[In](attr)
}

final class FanoutPublisherSink[In](
  initialBufferSize: Int,
  maximumBufferSize: Int,
  val attributes: OperationAttributes = OperationAttributes.none)
  extends SinkModule[In, Publisher[In]] {

  override def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Subscriber[In], Publisher[In]) = {
    val fanoutActor = materializer.actorOf(
      Props(new FanoutProcessorImpl(materializer.settings, initialBufferSize, maximumBufferSize)), s"$flowName-fanoutPublisher")
    val fanoutProcessor = ActorProcessorFactory[In, In](fanoutActor)
    (fanoutProcessor, fanoutProcessor)
  }

  override protected def newInstance: SinkModule[In, Publisher[In]] =
    new FanoutPublisherSink[In](initialBufferSize, maximumBufferSize, attributes)

  override def withAttributes(attr: OperationAttributes): Module =
    new FanoutPublisherSink[In](initialBufferSize, maximumBufferSize, attr)
}

object HeadSink {
  def apply[T](): HeadSink[T] = new HeadSink[T]

  /** INTERNAL API */
  private[akka] class HeadSinkSubscriber[In](p: Promise[In]) extends Subscriber[In] {
    private val sub = new AtomicReference[Subscription]
    override def onSubscribe(s: Subscription): Unit =
      if (!sub.compareAndSet(null, s)) s.cancel()
      else s.request(1)

    override def onNext(t: In): Unit = { p.trySuccess(t); sub.get.cancel() }
    override def onError(t: Throwable): Unit = p.tryFailure(t)
    override def onComplete(): Unit = p.tryFailure(new NoSuchElementException("empty stream"))
  }

}

/**
 * Holds a [[scala.concurrent.Future]] that will be fulfilled with the first
 * thing that is signaled to this stream, which can be either an element (after
 * which the upstream subscription is canceled), an error condition (putting
 * the Future into the corresponding failed state) or the end-of-stream
 * (failing the Future with a NoSuchElementException).
 */
class HeadSink[In](val attributes: OperationAttributes = OperationAttributes.none) extends SinkModule[In, Future[In]] {

  override def create(materializer: ActorBasedFlowMaterializer, flowName: String) = {
    val p = Promise[In]()
    val sub = new HeadSink.HeadSinkSubscriber[In](p)
    (sub, p.future)
  }

  override protected def newInstance: SinkModule[In, Future[In]] = new HeadSink[In](attributes)
  override def withAttributes(attr: OperationAttributes): Module = new HeadSink[In](attr)

  override def toString: String = "HeadSink"
}

/**
 * Attaches a subscriber to this stream which will just discard all received
 * elements.
 */
final class BlackholeSink(val attributes: OperationAttributes = OperationAttributes.none) extends SinkModule[Any, Unit] {

  override def create(materializer: ActorBasedFlowMaterializer, flowName: String) =
    (new BlackholeSubscriber[Any](materializer.settings.maxInputBufferSize), ())

  override protected def newInstance: SinkModule[Any, Unit] = new BlackholeSink(attributes)
  override def withAttributes(attr: OperationAttributes): Module = new BlackholeSink(attr)
}

/**
 * Attaches a subscriber to this stream.
 */
final class SubscriberSink[In](subscriber: Subscriber[In], val attributes: OperationAttributes = OperationAttributes.none) extends SinkModule[In, Unit] {

  override def create(materializer: ActorBasedFlowMaterializer, flowName: String) = (subscriber, ())

  override protected def newInstance: SinkModule[In, Unit] = new SubscriberSink[In](subscriber, attributes)
  override def withAttributes(attr: OperationAttributes): Module = new SubscriberSink[In](subscriber, attr)
}

/**
 * A sink that immediately cancels its upstream upon materialization.
 */
final class CancelSink(val attributes: OperationAttributes = OperationAttributes.none) extends SinkModule[Any, Unit] {

  /**
   * This method is only used for Sinks that return true from [[#isActive]], which then must
   * implement it.
   */
  override def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Subscriber[Any], Unit) = {
    val subscriber = new Subscriber[Any] {
      override def onError(t: Throwable): Unit = ()
      override def onSubscribe(s: Subscription): Unit = s.cancel()
      override def onComplete(): Unit = ()
      override def onNext(t: Any): Unit = ()
    }
    (subscriber, ())
  }

  override protected def newInstance: SinkModule[Any, Unit] = new CancelSink(attributes)
  override def withAttributes(attr: OperationAttributes): Module = new CancelSink(attr)
}

/**
 * Creates and wraps an actor into [[org.reactivestreams.Subscriber]] from the given `props`,
 * which should be [[akka.actor.Props]] for an [[akka.stream.actor.ActorSubscriber]].
 */
final class PropsSink[In](props: Props, val attributes: OperationAttributes = OperationAttributes.none) extends SinkModule[In, ActorRef] {

  override def create(materializer: ActorBasedFlowMaterializer, flowName: String) = {
    val subscriberRef = materializer.actorOf(props, name = s"$flowName-props")
    (akka.stream.actor.ActorSubscriber[In](subscriberRef), subscriberRef)
  }

  override protected def newInstance: SinkModule[In, ActorRef] = new PropsSink[In](props, attributes)
  override def withAttributes(attr: OperationAttributes): Module = new PropsSink[In](props, attr)
}
