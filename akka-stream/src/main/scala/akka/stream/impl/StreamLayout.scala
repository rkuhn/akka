/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.scaladsl.OperationAttributes
import org.reactivestreams.{ Subscription, Publisher, Subscriber }

/**
 * INTERNAL API
 */
private[akka] object StreamLayout {

  class InPort
  class OutPort

  // TODO: materialized value combiners in DSL
  // TODO: Materialization order
  // TODO: Special case linear composites
  // TODO: Cycles

  sealed trait MaterializedValueNode
  case class Combine(f: (Any, Any) ⇒ Any, dep1: MaterializedValueNode, dep2: MaterializedValueNode) extends MaterializedValueNode
  case class Atomic(module: Module) extends MaterializedValueNode
  case object Ignore extends MaterializedValueNode

  case class Mapping(module: Module, inPorts: Map[InPort, InPort], outPorts: Map[OutPort, OutPort])

  trait Module {
    def inPorts: Set[InPort]
    def outPorts: Set[OutPort]

    def isRunnable: Boolean = inPorts.isEmpty && outPorts.isEmpty
    def isSink: Boolean = (inPorts.size == 1) && outPorts.isEmpty
    def isSource: Boolean = (outPorts.size == 1) && inPorts.isEmpty
    def isFlow: Boolean = (inPorts.size == 1) && (outPorts.size == 1)

    def connect[A, B](from: OutPort, to: InPort): Module = {
      assert(outPorts(from))
      assert(inPorts(to))

      CompositeModule(
        subModules,
        inPorts - to,
        outPorts - from,
        downstreams.updated(from, to),
        upstreams.updated(to, from),
        materializedValueComputation,
        carbonCopy = () ⇒ {
          val mapping = this.carbonCopy()
          mapping.copy(module = mapping.module.connect(mapping.outPorts(from), mapping.inPorts(to)))
        },
        attributes)
    }

    def grow(that: Module): Module = {
      assert(that ne this)
      assert(!subModules(that))

      val modules1 = if (this.isAtomic) Set(this) else this.subModules
      val modules2 = if (that.isAtomic) Set(that) else that.subModules

      CompositeModule(
        modules1 ++ modules2,
        this.inPorts ++ that.inPorts,
        this.outPorts ++ that.outPorts,
        this.downstreams ++ that.downstreams,
        this.upstreams ++ that.upstreams,
        materializedValueComputation,
        carbonCopy = () ⇒ {
          val copy1 = this.carbonCopy()
          val copy2 = that.carbonCopy()
          Mapping(copy1.module.grow(copy2.module), copy1.inPorts ++ copy2.inPorts, copy1.outPorts ++ copy2.outPorts)
        },
        attributes)
    }

    def grow[A, B, C](that: Module, f: (A, B) ⇒ C): Module = {
      assert(that ne this)
      assert(!subModules(that))

      val modules1 = if (this.isAtomic) Set(this) else this.subModules
      val modules2 = if (that.isAtomic) Set(that) else that.subModules

      CompositeModule(
        modules1 ++ modules2,
        this.inPorts ++ that.inPorts,
        this.outPorts ++ that.outPorts,
        this.downstreams ++ that.downstreams,
        this.upstreams ++ that.upstreams,
        Combine(f.asInstanceOf[(Any, Any) ⇒ Any], this.materializedValueComputation, that.materializedValueComputation),
        carbonCopy = () ⇒ {
          val copy1 = this.carbonCopy()
          val copy2 = that.carbonCopy()
          Mapping(copy1.module.grow(copy2.module, f), copy1.inPorts ++ copy2.inPorts, copy1.outPorts ++ copy2.outPorts)
        },
        attributes)
    }

    def wrap(): Module = {
      CompositeModule(
        subModules = Set(this),
        inPorts,
        outPorts,
        downstreams,
        upstreams,
        Atomic(this),
        carbonCopy = () ⇒ {
          val copy = this.carbonCopy()
          copy.copy(module = copy.module.wrap())
        },
        attributes)
    }

    def subModules: Set[Module]
    def isAtomic: Boolean = subModules.isEmpty

    def downstreams: Map[OutPort, InPort]
    def upstreams: Map[InPort, OutPort]

    def materializedValueComputation: MaterializedValueNode = Atomic(this)
    def carbonCopy: () ⇒ Mapping

    def attributes: OperationAttributes
    def withAttributes(attributes: OperationAttributes): Module

    final override def hashCode(): Int = super.hashCode()
    final override def equals(obj: scala.Any): Boolean = super.equals(obj)
  }

  object EmptyModule extends Module {
    override def subModules: Set[Module] = Set.empty

    override def inPorts: Set[InPort] = Set.empty
    override def outPorts: Set[OutPort] = Set.empty

    override def downstreams: Map[OutPort, InPort] = Map.empty
    override def upstreams: Map[InPort, OutPort] = Map.empty

    override def withAttributes(attributes: OperationAttributes): Module = this
    override def attributes: OperationAttributes = OperationAttributes.none

    private val emptyMapping = Mapping(this, Map.empty, Map.empty)
    override val carbonCopy: () ⇒ Mapping = () ⇒ emptyMapping

    override def isRunnable: Boolean = false
    override def isAtomic: Boolean = false
    override def materializedValueComputation: MaterializedValueNode = Ignore

    override def grow(that: Module): Module = that

    override def wrap(): Module = this
  }

  final case class CompositeModule(
    subModules: Set[Module],
    inPorts: Set[InPort],
    outPorts: Set[OutPort],
    downstreams: Map[OutPort, InPort],
    upstreams: Map[InPort, OutPort],
    override val materializedValueComputation: MaterializedValueNode,
    override val carbonCopy: () ⇒ Mapping,
    attributes: OperationAttributes) extends Module {

    override def withAttributes(attributes: OperationAttributes): Module = copy(
      attributes = attributes,
      carbonCopy = () ⇒ {
        val that = this.carbonCopy()
        that.copy(module = that.module.withAttributes(attributes))
      })

  }
}

class VirtualSubscriber[T](val owner: VirtualPublisher[T]) extends Subscriber[T] {
  override def onSubscribe(s: Subscription): Unit = throw new UnsupportedOperationException("This method should not be called")
  override def onError(t: Throwable): Unit = throw new UnsupportedOperationException("This method should not be called")
  override def onComplete(): Unit = throw new UnsupportedOperationException("This method should not be called")
  override def onNext(t: T): Unit = throw new UnsupportedOperationException("This method should not be called")
}

class VirtualPublisher[T]() extends Publisher[T] {
  @volatile var realPublisher: Publisher[T] = null
  override def subscribe(s: Subscriber[_ >: T]): Unit = realPublisher.subscribe(s)
}

abstract class MaterializerSession(val topLevel: StreamLayout.Module) {
  import StreamLayout._

  private val subscribers = collection.mutable.HashMap[InPort, Subscriber[Any]]().withDefaultValue(null)
  private val publishers = collection.mutable.HashMap[OutPort, Publisher[Any]]().withDefaultValue(null)

  final def materialize(): Any = {
    assert(topLevel.isRunnable)
    materializeModule(topLevel, topLevel.attributes)
  }

  protected def mergeAttributes(parent: OperationAttributes, current: OperationAttributes): OperationAttributes =
    parent and current

  protected def materializeModule(module: Module, effectiveAttributes: OperationAttributes): Any = {
    val materializedValues = collection.mutable.HashMap.empty[Module, Any]
    for (submodule ← module.subModules) {
      val subEffectiveAttributes = mergeAttributes(effectiveAttributes, submodule.attributes)
      if (submodule.isAtomic) materializedValues.put(submodule, materializeAtomic(submodule, subEffectiveAttributes))
      else materializedValues.put(submodule, materializeComposite(submodule, subEffectiveAttributes))
    }
    resolveMaterialized(module.materializedValueComputation, materializedValues)
  }

  protected def materializeComposite(composite: Module, effectiveAttributes: OperationAttributes): Any = {
    materializeModule(composite, effectiveAttributes)
  }

  protected def materializeAtomic(atomic: Module, effectiveAttributes: OperationAttributes): Any

  private def resolveMaterialized(matNode: MaterializedValueNode, materializedValues: collection.Map[Module, Any]): Any = matNode match {
    case Atomic(m)          ⇒ materializedValues(m)
    case Combine(f, d1, d2) ⇒ f(resolveMaterialized(d1, materializedValues), resolveMaterialized(d2, materializedValues))
    case Ignore             ⇒ ()
  }

  private def attach(p: Publisher[Any], s: Subscriber[Any]) = s match {
    case v: VirtualSubscriber[Any] ⇒ v.owner.realPublisher = p
    case _                         ⇒ p.subscribe(s)
  }

  final protected def assignPort(in: InPort, subscriber: Subscriber[Any]): Unit = {
    subscribers.put(in, subscriber)
    val publisher = publishers(topLevel.upstreams(in))
    if (publisher ne null) attach(publisher, subscriber)
  }

  final protected def assignPort(out: OutPort, publisher: Publisher[Any]): Unit = {
    publishers.put(out, publisher)
    val subscriber = subscribers(topLevel.downstreams(out))
    if (subscriber ne null) attach(publisher, subscriber)
  }

}
