/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream

import scala.collection.immutable
import scala.collection.JavaConverters._

sealed abstract class InPort
sealed abstract class OutPort

final class Inlet[-T](override val toString: String) extends InPort
final class Outlet[+T](override val toString: String) extends OutPort

abstract class Shape {
  /**
   * Scala API: get a list of all input ports
   */
  def inlets: immutable.Seq[Inlet[_]]

  /**
   * Scala API: get a list of all output ports
   */
  def outlets: immutable.Seq[Outlet[_]]

  /**
   * Create a copy of this Ports object, returning the same type as the
   * original; this constraint can unfortunately not be expressed in the
   * type system.
   */
  def deepCopy(): Shape

  /**
   * Java API: get a list of all input ports
   */
  def getInlets: java.util.List[Inlet[_]] = inlets.asJava

  /**
   * Java API: get a list of all output ports
   */
  def getOutlets: java.util.List[Outlet[_]] = outlets.asJava
}

/**
 * Java API for creating custom Shape types.
 */
abstract class AbstractShape extends Shape {
  def allInlets: java.util.List[Inlet[_]]
  def allOutlets: java.util.List[Outlet[_]]

  final override lazy val inlets = allInlets.asScala.toList
  final override lazy val outlets = allOutlets.asScala.toList

  final override def getInlets = allInlets
  final override def getOutlets = allOutlets
}

object EmptyShape extends Shape {
  override val inlets = Nil
  override val outlets = Nil
  override def deepCopy() = this

  /**
   * Java API: obtain EmptyShape instance
   */
  def getInstance: Shape = this
}

case class AmorphousShape(inlets: immutable.Seq[Inlet[_]], outlets: immutable.Seq[Outlet[_]]) extends Shape {
  override def deepCopy() = AmorphousShape(
    inlets.map(i ⇒ new Inlet[Any](i.toString)),
    outlets.map(o ⇒ new Outlet[Any](o.toString)))
}

final case class SourceShape[+T](outlet: Outlet[T]) extends Shape {
  override val inlets: immutable.Seq[Inlet[_]] = Nil
  override val outlets: immutable.Seq[Outlet[_]] = List(outlet)

  override def deepCopy(): SourceShape[T] = SourceShape(new Outlet(outlet.toString))
}

final case class FlowShape[-I, +O](inlet: Inlet[I], outlet: Outlet[O]) extends Shape {
  override val inlets: immutable.Seq[Inlet[_]] = List(inlet)
  override val outlets: immutable.Seq[Outlet[_]] = List(outlet)

  override def deepCopy(): FlowShape[I, O] = FlowShape(new Inlet(inlet.toString), new Outlet(outlet.toString))
}

final case class SinkShape[-T](inlet: Inlet[T]) extends Shape {
  override val inlets: immutable.Seq[Inlet[_]] = List(inlet)
  override val outlets: immutable.Seq[Outlet[_]] = Nil

  override def deepCopy(): SinkShape[T] = SinkShape(new Inlet(inlet.toString))
}

/**
 * In1  => Out1
 * Out2 <= In2
 */
final case class BidiShape[-In1, +Out1, -In2, +Out2](in1: Inlet[In1], out1: Outlet[Out1], in2: Inlet[In2], out2: Outlet[Out2]) extends Shape {
  override val inlets: immutable.Seq[Inlet[_]] = List(in1, in2)
  override val outlets: immutable.Seq[Outlet[_]] = List(out1, out2)

  override def deepCopy(): BidiShape[In1, Out1, In2, Out2] =
    BidiShape(new Inlet(in1.toString), new Outlet(out1.toString), new Inlet(in2.toString), new Outlet(out2.toString))
}
