/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import scala.concurrent.Future

object Graphs {

  final class Port[T](override val toString: String)

  trait Ports {
    def inlets: Set[Port[_]]
    def outlets: Set[Port[_]]
  }

  final case class SourcePorts[T](outlet: Port[T]) extends Ports {
    override def inlets: Set[Port[_]] = Set.empty
    override val outlets: Set[Port[_]] = Set(outlet)
  }

  final case class FlowPorts[I, O](inlet: Port[I], outlet: Port[O]) extends Ports {
    override val inlets: Set[Port[_]] = Set(inlet)
    override val outlets: Set[Port[_]] = Set(outlet)
  }

  final case class SinkPorts[T](inlet: Port[T]) extends Ports {
    override def inlets: Set[Port[_]] = Set(inlet)
    override val outlets: Set[Port[_]] = Set.empty
  }

  /**
   * In1  => Out1
   * Out2 <= In2
   */
  final case class BidiPorts[In1, Out1, In2, Out2](in1: Port[In1], out1: Port[Out1], in2: Port[In2], out2: Port[Out2]) extends Ports {
    override val inlets: Set[Port[_]] = Set(in1, in2)
    override val outlets: Set[Port[_]] = Set(out1, out2)
  }

  trait Graph[P <: Ports, M] extends Materializable {
    override type MaterializedType = M
    type Ports = P
    def ports: P
  }

  //  /**
  //   * This imports g1 and g2 by copying into the builder before running the
  //   * user code block. The user defines how to combine the materialized values
  //   * of the parts, and the return value is the new source’s outlet port (which
  //   * may have been imported with the graphs as well).
  //   *
  //   * To be extended to higher arities using the boilerplate plugin.
  //   */
  //  def source[G1 <: Graph[_, _], G2 <: Graph[_, _], Mat, T](g1: G1, g2: G2)(
  //    combineMat: (G1#MaterializedType, G2#MaterializedType) ⇒ Mat)(
  //      block: FlowGraphBuilder ⇒ (G1#Ports, G2#Ports) ⇒ Port[T]): Source[T, Mat] = ???
  //
  //  def example(g1: Source[Int, Future[Unit]], g2: Flow[Int, String, Unit]) =
  //    source(g1, g2)((f, _) ⇒ f) { implicit b ⇒
  //      (p1, p2) ⇒
  //        import FlowGraphImplicits._
  //
  //        p1.outlet ~> p2.inlet
  //        p2.outlet
  //    }
}