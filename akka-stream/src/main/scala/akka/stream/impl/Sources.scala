/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import java.util.concurrent.atomic.AtomicBoolean
import akka.actor.{ PoisonPill, Cancellable, Props, ActorRef }
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.OperationAttributes
import org.reactivestreams._
import scala.annotation.unchecked.uncheckedVariance
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ Promise, ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{ Success, Failure }
import akka.stream.impl.StreamLayout.{ Module, Mapping }
import akka.stream.{ Inlet, Outlet, InPort, OutPort }
import akka.stream.SourceShape

trait SourceModule[+Out, +Mat] extends Module {
  
  val outPort: Outlet[Out] = new Outlet[Out]("Source.out")
  override val shape = new SourceShape[Out](outPort)

  override def subModules: Set[Module] = Set.empty
  override def upstreams: Map[InPort, OutPort] = Map.empty
  override def downstreams: Map[OutPort, InPort] = Map.empty

  /**
   * This method is only used for Sources that return true from [[#isActive]], which then must
   * implement it.
   *
   * FIXME: this is no longer true, right?
   */
  def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Publisher[Out] @uncheckedVariance, Mat)

  protected def newInstance: SourceModule[Out, Mat]

  override def carbonCopy: () ⇒ Mapping = () ⇒ {
    val copy = newInstance
    Mapping(copy, Map.empty, Map(outPort -> copy.outPort))
  }
}

/**
 * Holds a `Subscriber` representing the input side of the flow.
 * The `Subscriber` can later be connected to an upstream `Publisher`.
 */
final class SubscriberSource[Out](val attributes: OperationAttributes = OperationAttributes.none) extends SourceModule[Out, Subscriber[Out]] {

  /**
   * This method is only used for Sources that return true from [[#isActive]], which then must
   * implement it.
   */
  override def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Publisher[Out], Subscriber[Out]) = {
    val processor = new Processor[Out, Out] {
      @volatile private var subscriber: Subscriber[_ >: Out] = null

      override def subscribe(s: Subscriber[_ >: Out]): Unit = subscriber = s

      override def onError(t: Throwable): Unit = subscriber.onError(t)
      override def onSubscribe(s: Subscription): Unit = subscriber.onSubscribe(s)
      override def onComplete(): Unit = subscriber.onComplete()
      override def onNext(t: Out): Unit = subscriber.onNext(t)
    }

    (processor, processor)
  }

  override protected def newInstance: SourceModule[Out, Subscriber[Out]] = new SubscriberSource[Out](attributes)
  override def withAttributes(attr: OperationAttributes): Module = new SubscriberSource[Out](attr)
}

/**
 * Construct a transformation starting with given publisher. The transformation steps
 * are executed by a series of [[org.reactivestreams.Processor]] instances
 * that mediate the flow of elements downstream and the propagation of
 * back-pressure upstream.
 */
final class PublisherSource[Out](p: Publisher[Out], val attributes: OperationAttributes = OperationAttributes.none) extends SourceModule[Out, Unit] {
  override def create(materializer: ActorBasedFlowMaterializer, flowName: String) = (p, ())

  override protected def newInstance: SourceModule[Out, Unit] = new PublisherSource[Out](p, attributes)
  override def withAttributes(attr: OperationAttributes): Module = new PublisherSource[Out](p, attr)
}

/**
 * Start a new `Source` from the given `Future`. The stream will consist of
 * one element when the `Future` is completed with a successful value, which
 * may happen before or after materializing the `Flow`.
 * The stream terminates with an error if the `Future` is completed with a failure.
 */
final class FutureSource[Out](future: Future[Out], val attributes: OperationAttributes = OperationAttributes.none) extends SourceModule[Out, Unit] { // FIXME Why does this have anything to do with Actors?
  override def create(materializer: ActorBasedFlowMaterializer, flowName: String) =
    future.value match {
      case Some(Success(element)) ⇒
        (SynchronousIterablePublisher(List(element), s"$flowName-0-synciterable"), ()) // Option is not Iterable. sigh
      case Some(Failure(t)) ⇒
        (ErrorPublisher(t, s"$flowName-0-error").asInstanceOf[Publisher[Out]], ())
      case None ⇒
        (ActorPublisher[Out](materializer.actorOf(FuturePublisher.props(future, materializer.settings),
          name = s"$flowName-0-future")), ()) // FIXME this does not need to be an actor
    }

  override protected def newInstance: SourceModule[Out, Unit] = new FutureSource(future, attributes)
  override def withAttributes(attr: OperationAttributes): Module = new FutureSource(future, attr)
}

final class LazyEmptySource[Out](val attributes: OperationAttributes = OperationAttributes.none) extends SourceModule[Out, Promise[Unit]] {

  override def create(materializer: ActorBasedFlowMaterializer, flowName: String) = {
    val p = Promise[Unit]()

    // Not TCK verified as RC1 does not allow "empty publishers", 
    // reactive-streams on master now contains support for empty publishers.
    // so we can enable it then, though it will require external completing of the promise
    val pub = new Publisher[Unit] {
      override def subscribe(s: Subscriber[_ >: Unit]) = {
        s.onSubscribe(new Subscription {
          override def request(n: Long): Unit = ()

          override def cancel(): Unit = p.success(())
        })
        p.future.onComplete {
          case Success(_)  ⇒ s.onComplete()
          case Failure(ex) ⇒ s.onError(ex) // due to external signal
        }(materializer.asInstanceOf[ActorBasedFlowMaterializer].executionContext) // TODO: Should it use this EC or something else?
      }
    }

    pub.asInstanceOf[Publisher[Out]] → p
  }

  override protected def newInstance: SourceModule[Out, Promise[Unit]] = new LazyEmptySource[Out](attributes)
  override def withAttributes(attr: OperationAttributes): Module = new LazyEmptySource(attr)
}

/**
 * Elements are emitted periodically with the specified interval.
 * The tick element will be delivered to downstream consumers that has requested any elements.
 * If a consumer has not requested any elements at the point in time when the tick
 * element is produced it will not receive that tick element later. It will
 * receive new tick elements as soon as it has requested more elements.
 */
final class TickSource[Out](initialDelay: FiniteDuration, interval: FiniteDuration, tick: Out, val attributes: OperationAttributes = OperationAttributes.none) extends SourceModule[Out, Cancellable] { // FIXME Why does this have anything to do with Actors?

  override def create(materializer: ActorBasedFlowMaterializer, flowName: String) = {
    val cancelled = new AtomicBoolean(false)
    val ref =
      materializer.actorOf(TickPublisher.props(initialDelay, interval, tick, materializer.settings, cancelled),
        name = s"$flowName-0-tick")
    (ActorPublisher[Out](ref), new Cancellable {
      override def cancel(): Boolean = {
        if (!isCancelled) ref ! PoisonPill
        true
      }
      override def isCancelled: Boolean = cancelled.get()
    })
  }

  override protected def newInstance: SourceModule[Out, Cancellable] = new TickSource[Out](initialDelay, interval, tick, attributes)
  override def withAttributes(attr: OperationAttributes): Module = new TickSource(initialDelay, interval, tick, attr)
}

/**
 * Creates and wraps an actor into [[org.reactivestreams.Publisher]] from the given `props`,
 * which should be [[akka.actor.Props]] for an [[akka.stream.actor.ActorPublisher]].
 */
final class PropsSource[Out](props: Props, val attributes: OperationAttributes = OperationAttributes.none) extends SourceModule[Out, ActorRef] {

  override def create(materializer: ActorBasedFlowMaterializer, flowName: String) = {
    val publisherRef = materializer.actorOf(props, name = s"$flowName-0-props")
    (akka.stream.actor.ActorPublisher[Out](publisherRef), publisherRef)
  }

  override protected def newInstance: SourceModule[Out, ActorRef] = new PropsSource[Out](props, attributes)
  override def withAttributes(attr: OperationAttributes): Module = new PropsSource(props, attr)
}