/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import org.reactivestreams.{ Publisher, Subscriber }

/**
 * INTERNAL API
 */
private[akka] object StreamLayout {

  class InPort
  class OutPort

  // TODO: Materialization order
  // TODO: Attribute support
  // TODO: Modular materialization
  // TODO: Special case linear composites
  // TODO: Cycles
  // TODO: Copy support (don't deep copy, try to add a distinguisher ID)
  // TODO: Benchmark, Optimize

  trait Layout {
    def inPorts: Set[InPort]
    def outPorts: Set[OutPort]

    def isRunnable: Boolean = inPorts.isEmpty && outPorts.isEmpty
    def isSink: Boolean = (inPorts.size == 1) && outPorts.isEmpty
    def isSource: Boolean = (outPorts.size == 1) && inPorts.isEmpty
    def isFlow: Boolean = (inPorts.size == 1) && (outPorts.size == 1)

    def connect(from: OutPort, to: InPort): PartialModule = {
      assert(outPorts(from))
      assert(inPorts(to))

      PartialModule(
        subModules,
        inPorts - to,
        outPorts - from,
        downstreams.updated(from, to),
        upstreams.updated(to, from))
    }

    def compose(that: Module): PartialModule = {
      assert(that ne this)
      assert(!subModules(that))

      PartialModule(
        this.subModules ++ that.subModules,
        this.inPorts ++ that.inPorts,
        this.outPorts ++ that.outPorts,
        this.downstreams ++ that.downstreams,
        this.upstreams ++ that.upstreams)
    }

    def composeConnect(from: OutPort, toStage: Module, to: InPort): PartialModule = {
      assert(outPorts(from))
      assert(!inPorts(to))
      assert(toStage ne this)
      assert(toStage.inPorts(to))

      // TODO: Optimize
      compose(toStage).connect(from, to)
    }

    def subModules: Set[Module]

    def downstreams: Map[OutPort, InPort]
    def upstreams: Map[InPort, OutPort]

  }

  sealed trait Module extends Layout

  final case class AtomicModule(inPorts: Set[InPort], outPorts: Set[OutPort]) extends Module {
    override val downstreams = Map.empty[OutPort, InPort]
    override val upstreams = Map.empty[InPort, OutPort]
    override val subModules = Set[Module](this)
  }

  final case class CompositeModule(
    subModules: Set[Module],
    inPorts: Set[InPort],
    outPorts: Set[OutPort],
    downstreams: Map[OutPort, InPort],
    upstreams: Map[InPort, OutPort]) extends Module

  final case class PartialModule(
    subModules: Set[Module],
    inPorts: Set[InPort],
    outPorts: Set[OutPort],
    downstreams: Map[OutPort, InPort],
    upstreams: Map[InPort, OutPort]) extends Layout {

    def module(): Module = CompositeModule(
      subModules,
      inPorts,
      outPorts,
      downstreams,
      upstreams)
  }

}

abstract class MaterializerSession {
  import StreamLayout._

  private val subscribers = scala.collection.mutable.HashMap[InPort, Subscriber[Any]]().withDefaultValue(null)
  private val publishers = scala.collection.mutable.HashMap[OutPort, Publisher[Any]]().withDefaultValue(null)

  final def materialize(module: Module): Unit = {
    assert(module.isRunnable)
    materializeModule(module, module)
  }

  protected def materializeModule(module: Module, topLevel: Module): Unit = {
    for (submodule ← module.subModules) {
      submodule match {
        case c: CompositeModule ⇒ materializeComposite(c, topLevel)
        case a: AtomicModule    ⇒ materializeAtomic(a, topLevel)
      }
    }
  }

  protected def materializeComposite(composite: CompositeModule, topLevel: Module): Unit = {
    materializeModule(composite, topLevel)
  }

  protected def materializeAtomic(atomic: AtomicModule, topLevel: Module): Unit

  final protected def assignPort(in: InPort, subscriber: Subscriber[Any], topLevel: Module): Unit = {
    subscribers.put(in, subscriber)
    val publisher = publishers(topLevel.upstreams(in))
    if (publisher ne null) publisher.subscribe(subscriber)
  }
  final protected def assignPort(out: OutPort, publisher: Publisher[Any], topLevel: Module): Unit = {
    publishers.put(out, publisher)
    val subscriber = subscribers(topLevel.downstreams(out))
    if (subscriber ne null) publisher.subscribe(subscriber)
  }

}
