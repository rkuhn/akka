/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.scaladsl.{ Keep, OperationAttributes }
import akka.stream.{ Inlet, Outlet, InPort, OutPort, Shape, EmptyShape, AmorphousShape }
import org.reactivestreams.{ Subscription, Publisher, Subscriber }

/**
 * INTERNAL API
 */
private[akka] object StreamLayout {

  // TODO: Materialization order
  // TODO: Special case linear composites
  // TODO: Cycles

  sealed trait MaterializedValueNode
  case class Combine(f: (Any, Any) ⇒ Any, dep1: MaterializedValueNode, dep2: MaterializedValueNode) extends MaterializedValueNode
  case class Atomic(module: Module) extends MaterializedValueNode
  case class Transform(f: Any ⇒ Any, dep: MaterializedValueNode) extends MaterializedValueNode
  case object Ignore extends MaterializedValueNode

  trait Module {
    def shape: Shape
    /**
     * Verify that the given Shape has the same ports and return a new module with that shape.
     * Concrete implementations may throw UnsupportedOperationException where applicable.
     */
    def replaceShape(s: Shape): Module

    final lazy val inPorts: Set[InPort] = shape.inlets.toSet
    final lazy val outPorts: Set[OutPort] = shape.outlets.toSet

    def isRunnable: Boolean = inPorts.isEmpty && outPorts.isEmpty
    def isSink: Boolean = (inPorts.size == 1) && outPorts.isEmpty
    def isSource: Boolean = (outPorts.size == 1) && inPorts.isEmpty
    def isFlow: Boolean = (inPorts.size == 1) && (outPorts.size == 1)

    def connect[A, B](from: OutPort, to: InPort): Module = {
      require(outPorts(from), s"The output port [$from] is not part of the underlying graph.")
      require(inPorts(to), s"The input port [$to] is not part of the underlying graph.")

      CompositeModule(
        subModules,
        AmorphousShape(shape.inlets.filterNot(_ == to), shape.outlets.filterNot(_ == from)),
        downstreams.updated(from, to),
        upstreams.updated(to, from),
        materializedValueComputation,
        attributes)
    }

    def transformMaterializedValue(f: Any ⇒ Any): Module = {
      CompositeModule(
        subModules = this.subModules,
        shape,
        downstreams,
        upstreams,
        Transform(f, this.materializedValueComputation),
        attributes)
    }

    def grow(that: Module): Module = grow(that, Keep.left)

    def grow[A, B, C](that: Module, f: (A, B) ⇒ C): Module = {
      require(that ne this, "A module cannot be added to itself. You should pass a separate instance to grow().")
      require(!subModules(that), "An existing submodule cannot be added again. All contained modules must be unique.")

      val modules1 = if (this.isAtomic) Set(this) else this.subModules
      val modules2 = if (that.isAtomic) Set(that) else that.subModules

      CompositeModule(
        modules1 ++ modules2,
        AmorphousShape(shape.inlets ++ that.shape.inlets, shape.outlets ++ that.shape.outlets),
        this.downstreams ++ that.downstreams,
        this.upstreams ++ that.upstreams,
        if (f eq Keep.left) materializedValueComputation
        else if (f eq Keep.right) that.materializedValueComputation
        else Combine(f.asInstanceOf[(Any, Any) ⇒ Any], this.materializedValueComputation, that.materializedValueComputation),
        attributes)
    }

    def wrap(): Module = {
      CompositeModule(
        subModules = Set(this),
        shape,
        downstreams,
        upstreams,
        Atomic(this),
        attributes)
    }

    def subModules: Set[Module]
    def isAtomic: Boolean = subModules.isEmpty

    def downstreams: Map[OutPort, InPort]
    def upstreams: Map[InPort, OutPort]

    def materializedValueComputation: MaterializedValueNode = Atomic(this)
    def carbonCopy: Module

    def attributes: OperationAttributes
    def withAttributes(attributes: OperationAttributes): Module

    final override def hashCode(): Int = super.hashCode()
    final override def equals(obj: scala.Any): Boolean = super.equals(obj)
  }

  case class EmptyModuleWithShape(shape: Shape, attributes: OperationAttributes) extends Module {
    override def replaceShape(s: Shape) = copy(shape.copyFromPorts(s.inlets, s.outlets))

    override def subModules: Set[Module] = Set.empty

    override def downstreams: Map[OutPort, InPort] = Map.empty
    override def upstreams: Map[InPort, OutPort] = Map.empty

    override def withAttributes(attributes: OperationAttributes): Module = copy(attributes = attributes)

    override val carbonCopy: Module = copy(shape.deepCopy())

    override def isRunnable: Boolean = false
    override def isAtomic: Boolean = false
    override def materializedValueComputation: MaterializedValueNode = Ignore

    override def wrap(): Module = this
  }

  object EmptyModule extends EmptyModuleWithShape(EmptyShape, OperationAttributes.none) {
    override def grow(that: Module): Module = that
    override def wrap(): Module = this
  }

  final case class CompositeModule(
    subModules: Set[Module],
    shape: Shape,
    downstreams: Map[OutPort, InPort],
    upstreams: Map[InPort, OutPort],
    override val materializedValueComputation: MaterializedValueNode,
    attributes: OperationAttributes) extends Module {

    override def replaceShape(s: Shape): Module = {
      shape.requireSamePortsAs(s)
      copy(shape = s)
    }

    override def carbonCopy: Module = {
      import scala.collection.mutable
      val out = mutable.Map[OutPort, OutPort]()
      val in = mutable.Map[InPort, InPort]()

      val subs = subModules map { s ⇒
        val n = s.carbonCopy
        out ++= s.shape.outlets.zip(n.shape.outlets)
        in ++= s.shape.inlets.zip(n.shape.inlets)
        n
      }

      val downs = downstreams.map(p ⇒ (out(p._1), in(p._2)))
      val ups = upstreams.map(p ⇒ (in(p._1), out(p._2)))

      val newShape = shape.copyFromPorts(shape.inlets.map(in.asInstanceOf[Inlet[_] ⇒ Inlet[_]]),
        shape.outlets.map(out.asInstanceOf[Outlet[_] ⇒ Outlet[_]]))

      copy(subModules = subs, shape = newShape, downstreams = downs, upstreams = ups)
    }

    override def withAttributes(attributes: OperationAttributes): Module = copy(attributes = attributes)

    override def toString = {
      "\nModules: \n" + subModules.toSeq.map(m ⇒ "   " + m.getClass.getName).mkString("\n") + "\n" +
        "Downstreams: \n" + downstreams.map { case (in, out) ⇒ s"   $in -> $out" }.mkString("\n") + "\n" +
        "Upstreams: \n" + upstreams.map { case (out, in) ⇒ s"   $out -> $in" }.mkString("\n")
    }

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
    require(topLevel ne EmptyModule, "An empty module cannot be materialized (EmptyModule was given)")
    require(
      topLevel.isRunnable,
      s"The top level module cannot be materialized because it has unconnected ports: ${(topLevel.inPorts ++ topLevel.outPorts).mkString(", ")}")
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
    case Transform(f, d)    ⇒ f(resolveMaterialized(d, materializedValues))
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
