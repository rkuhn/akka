/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.testkit.AkkaSpec
import org.reactivestreams.{ Subscription, Subscriber, Publisher }

class StreamLayoutSpec extends AkkaSpec {
  import StreamLayout._

  def testAtomic(inPortCount: Int, outPortCount: Int): AtomicModule = AtomicModule(
    List.fill(inPortCount)(new InPort).toSet,
    List.fill(outPortCount)(new OutPort).toSet)

  def testStage(): AtomicModule = testAtomic(1, 1)
  def testSource(): AtomicModule = testAtomic(0, 1)
  def testSink(): AtomicModule = testAtomic(1, 0)

  "StreamLayout" must {

    "be able to model simple linear stages" in {
      val stage1 = testStage()

      stage1.inPorts.size should be(1)
      stage1.outPorts.size should be(1)
      stage1.isRunnable should be(false)
      stage1.isFlow should be(true)
      stage1.isSink should be(false)
      stage1.isSource should be(false)

      val stage2 = testStage()
      val flow12 = stage1.composeConnect(stage1.outPorts.head, stage2, stage2.inPorts.head).module

      flow12.inPorts should be(stage1.inPorts)
      flow12.outPorts should be(stage2.outPorts)
      flow12.isRunnable should be(false)
      flow12.isFlow should be(true)
      flow12.isSink should be(false)
      flow12.isSource should be(false)

      val source0 = testSource()
      source0.inPorts.size should be(0)
      source0.outPorts.size should be(1)
      source0.isRunnable should be(false)
      source0.isFlow should be(false)
      source0.isSink should be(false)
      source0.isSource should be(true)

      val sink3 = testSink()
      sink3.inPorts.size should be(1)
      sink3.outPorts.size should be(0)
      sink3.isRunnable should be(false)
      sink3.isFlow should be(false)
      sink3.isSink should be(true)
      sink3.isSource should be(false)

      val source012 = source0.composeConnect(source0.outPorts.head, flow12, flow12.inPorts.head).module
      source012.inPorts.size should be(0)
      source012.outPorts should be(flow12.outPorts)
      source012.isRunnable should be(false)
      source012.isFlow should be(false)
      source012.isSink should be(false)
      source012.isSource should be(true)

      val sink123 = flow12.composeConnect(flow12.outPorts.head, sink3, sink3.inPorts.head).module
      sink123.inPorts should be(flow12.inPorts)
      sink123.outPorts.size should be(0)
      sink123.isRunnable should be(false)
      sink123.isFlow should be(false)
      sink123.isSink should be(true)
      sink123.isSource should be(false)

      val runnable0123a = source0.composeConnect(source0.outPorts.head, sink123, sink123.inPorts.head).module
      val runnable0123b = source012.composeConnect(source012.outPorts.head, sink3, sink3.inPorts.head).module
      val runnable0123c =
        source0
          .composeConnect(source0.outPorts.head, flow12, flow12.inPorts.head)
          .composeConnect(flow12.outPorts.head, sink3, sink3.inPorts.head)
          .module

      runnable0123a should be(runnable0123b)
      runnable0123a should be(runnable0123c)

      runnable0123a.inPorts.size should be(0)
      runnable0123a.outPorts.size should be(0)
      runnable0123a.isRunnable should be(true)
      runnable0123a.isFlow should be(false)
      runnable0123a.isSink should be(false)
      runnable0123a.isSource should be(false)
    }

    "be able to model hierarchic linear modules" in {
      pending
    }

    "be able to model graph layouts" in {
      pending
    }

    "be able to materialize linear layouts" in {
      val source = testSource()
      val stage1 = testStage()
      val stage2 = testStage()
      val sink = testSink()

      val runnable = source.composeConnect(source.outPorts.head, stage1, stage1.inPorts.head)
        .composeConnect(stage1.outPorts.head, stage2, stage2.inPorts.head)
        .composeConnect(stage2.outPorts.head, sink, sink.inPorts.head)
        .module

      checkMaterialized(runnable)
    }

    "be able to materialize DAG layouts" in {
      pending

    }
    "be able to materialize cyclic layouts" in {
      pending
    }

    "be able to model hierarchic graph modules" in {
      pending
    }

    "be able to model hierarchic attributes" in {
      pending
    }

    "be able to model hierarchic cycle detection" in {
      pending
    }

  }

  case class TestPublisher(owner: Module, port: OutPort) extends Publisher[Any] with Subscription {
    var downstreamModule: Module = _
    var downstreamPort: InPort = _

    override def subscribe(s: Subscriber[_ >: Any]): Unit = s match {
      case TestSubscriber(o, p) ⇒
        downstreamModule = o
        downstreamPort = p
        s.onSubscribe(this)
    }

    override def request(n: Long): Unit = ()
    override def cancel(): Unit = ()
  }

  case class TestSubscriber(owner: Module, port: InPort) extends Subscriber[Any] {
    var upstreamModule: Module = _
    var upstreamPort: OutPort = _

    override def onSubscribe(s: Subscription): Unit = s match {
      case TestPublisher(o, p) ⇒
        upstreamModule = o
        upstreamPort = p
    }

    override def onError(t: Throwable): Unit = ()
    override def onComplete(): Unit = ()
    override def onNext(t: Any): Unit = ()
  }

  class FlatTestMaterializer extends MaterializerSession {
    var publishers = Vector.empty[TestPublisher]
    var subscribers = Vector.empty[TestSubscriber]

    override protected def materializeAtomic(atomic: AtomicModule, topLevel: Module): Unit = {
      for (inPort ← atomic.inPorts) {
        val subscriber = TestSubscriber(atomic, inPort)
        subscribers :+= subscriber
        assignPort(inPort, subscriber, topLevel)
      }
      for (outPort ← atomic.outPorts) {
        val publisher = TestPublisher(atomic, outPort)
        publishers :+= publisher
        assignPort(outPort, publisher, topLevel)
      }
    }
  }

  def checkMaterialized(topLevel: Module): (Set[TestPublisher], Set[TestSubscriber]) = {
    val materializer = new FlatTestMaterializer()
    materializer.materialize(topLevel)
    materializer.publishers.isEmpty should be(false)
    materializer.subscribers.isEmpty should be(false)

    materializer.subscribers.size should be(materializer.publishers.size)

    val inToSubscriber: Map[InPort, TestSubscriber] = materializer.subscribers.map(s ⇒ s.port -> s).toMap
    val outToPublisher: Map[OutPort, TestPublisher] = materializer.publishers.map(s ⇒ s.port -> s).toMap

    for (publisher ← materializer.publishers) {
      publisher.owner.isInstanceOf[AtomicModule] should be(true)
      topLevel.upstreams(publisher.downstreamPort) should be(publisher.port)
    }

    for (subscriber ← materializer.subscribers) {
      subscriber.owner.isInstanceOf[AtomicModule] should be(true)
      topLevel.downstreams(subscriber.upstreamPort) should be(subscriber.port)
    }

    def getAllAtomic(module: Module): Set[AtomicModule] = {
      val (atomics, composites) = module.subModules.partition(_.isInstanceOf[AtomicModule])
      atomics.asInstanceOf[Set[AtomicModule]] ++ composites.map(getAllAtomic).flatten
    }

    val allAtomic = getAllAtomic(topLevel)

    for (atomic ← allAtomic) {
      for (in ← atomic.inPorts; subscriber = inToSubscriber(in)) {
        subscriber.owner should be(atomic)
        subscriber.upstreamPort should be(topLevel.upstreams(in))
        subscriber.upstreamModule.outPorts.exists(outToPublisher(_).downstreamPort == in)
      }
      for (out ← atomic.outPorts; publisher = outToPublisher(out)) {
        publisher.owner should be(atomic)
        publisher.downstreamPort should be(topLevel.downstreams(out))
        publisher.downstreamModule.inPorts.exists(inToSubscriber(_).upstreamPort == out)
      }
    }

    materializer.publishers.distinct.size should be(materializer.publishers.size)
    materializer.subscribers.distinct.size should be(materializer.subscribers.size)

    (materializer.publishers.toSet, materializer.subscribers.toSet)
  }

}
