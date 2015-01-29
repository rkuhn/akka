/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import org.reactivestreams.{ Publisher, Subscriber }

import scala.annotation.tailrec

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

  // FIXME fix immutability, add a backing vector, pointers replaced by numbers
  class ConnectedComponentNode(var parent: ConnectedComponentNode)
  def ConnectedComponent(): ConnectedComponentNode = {
    val n = new ConnectedComponentNode(null)
    n.parent = n
    n
  }

  // TODO: Path compress
  case class ConnectedComponents(components: Map[AtomicModule, ConnectedComponentNode] = Map.empty) {

    def add(atomic: AtomicModule): ConnectedComponents = {
      assert(!components.isDefinedAt(atomic))
      this.copy(components.updated(atomic, ConnectedComponent()))
    }
    def merge(that: ConnectedComponents): ConnectedComponents = this.copy(this.components ++ that.components)
    def link(from: AtomicModule, to: AtomicModule): ConnectedComponents = {
      val componentFrom = resolve(components(from))
      val componentTo = resolve(components(to))

      if (componentFrom eq componentTo) this
      else {
        val newParent = ConnectedComponent()
        componentFrom.parent = newParent
        componentTo.parent = newParent
        this
      }
    }

    def inSameComponent(atomic1: AtomicModule, atomic2: AtomicModule): Boolean =
      resolve(components(atomic1)) eq resolve(components(atomic2))

    @tailrec private def resolve(node: ConnectedComponentNode): ConnectedComponentNode = {
      if (node.parent == node) node
      else resolve(node.parent)
    }
  }

}

abstract class MaterializerSession(val topLevel: StreamLayout.Module) {
  import StreamLayout._

  private val subscribers = scala.collection.mutable.HashMap[InPort, Subscriber[Any]]().withDefaultValue(null)
  private val publishers = scala.collection.mutable.HashMap[OutPort, Publisher[Any]]().withDefaultValue(null)

  final def materialize(): Unit = {
    assert(topLevel.isRunnable)
    materializeModule(topLevel)
  }

  protected def materializeModule(module: Module): Unit = {
    for (submodule ← module.subModules) {
      submodule match {
        case c: CompositeModule ⇒ materializeComposite(c)
        case a: AtomicModule    ⇒ materializeAtomic(a)
      }
    }
  }

  protected def materializeComposite(composite: CompositeModule): Unit = {
    materializeModule(composite)
  }

  protected def materializeAtomic(atomic: AtomicModule): Unit

  final protected def assignPort(in: InPort, subscriber: Subscriber[Any]): Unit = {
    subscribers.put(in, subscriber)
    val publisher = publishers(topLevel.upstreams(in))
    if (publisher ne null) publisher.subscribe(subscriber)
  }
  final protected def assignPort(out: OutPort, publisher: Publisher[Any]): Unit = {
    publishers.put(out, publisher)
    val subscriber = subscribers(topLevel.downstreams(out))
    if (subscriber ne null) publisher.subscribe(subscriber)
  }

}
