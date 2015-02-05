/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import scala.concurrent.Future
import akka.util.ByteString
import akka.stream.impl.StreamLayout

object Graphs {

  final class InputPort[-T](override val toString: String) extends StreamLayout.InPort
  final class OutputPort[+T](override val toString: String) extends StreamLayout.OutPort

  trait Ports {
    def inlets: Set[InputPort[_]]
    def outlets: Set[OutputPort[_]]
  }

  final case class SourcePorts[+T](outlet: OutputPort[T]) extends Ports {
    override def inlets: Set[InputPort[_]] = Set.empty
    override val outlets: Set[OutputPort[_]] = Set(outlet)
  }

  final case class FlowPorts[-I, +O](inlet: InputPort[I], outlet: OutputPort[O]) extends Ports {
    override val inlets: Set[InputPort[_]] = Set(inlet)
    override val outlets: Set[OutputPort[_]] = Set(outlet)
  }

  final case class SinkPorts[-T](inlet: InputPort[T]) extends Ports {
    override def inlets: Set[InputPort[_]] = Set(inlet)
    override val outlets: Set[OutputPort[_]] = Set.empty
  }

  /**
   * In1  => Out1
   * Out2 <= In2
   */
  final case class BidiPorts[-In1, +Out1, -In2, +Out2](in1: InputPort[In1],
                                                       out1: OutputPort[Out1],
                                                       in2: InputPort[In2],
                                                       out2: OutputPort[Out2]) extends Ports {
    override val inlets: Set[InputPort[_]] = Set(in1, in2)
    override val outlets: Set[OutputPort[_]] = Set(out1, out2)
  }

  implicit class FlowJoin[TopOut, BottomIn, Mat](val left: Graph[FlowPorts[BottomIn, TopOut], Mat]) extends AnyVal {
    def <~>[Mat2](right: Flow[TopOut, BottomIn, Mat2]): RunnableFlow[(Mat, Mat2)] = ???
    def <~>[Top, Bottom, Mat2](right: Graph[BidiPorts[TopOut, Top, Bottom, BottomIn], Mat2]): Flow[Bottom, Top, (Mat, Mat2)] = ???
    def <<~>[Mat2](right: Flow[TopOut, BottomIn, Mat2]): RunnableFlow[Mat] = ???
    def <<~>[Top, Bottom, Mat2](right: Graph[BidiPorts[TopOut, Top, Bottom, BottomIn], Mat2]): Flow[Bottom, Top, Mat] = ???
    def <~>>[Mat2](right: Flow[TopOut, BottomIn, Mat2]): RunnableFlow[Mat2] = ???
    def <~>>[Top, Bottom, Mat2](right: Graph[BidiPorts[TopOut, Top, Bottom, BottomIn], Mat2]): Flow[Bottom, Top, Mat2] = ???
  }

  implicit class GraphMat[P <: Ports, M](val g: Graph[P, M]) extends AnyVal {
    def <*>[M2](f: M ⇒ M2): Graph[P, M2] = ???
  }

  implicit class RunnableMat[M](val r: RunnableFlow[M]) extends AnyVal {
    def <*>[M2](f: M ⇒ M2): RunnableFlow[M2] = ???
  }

  def bidiExample(left: Flow[ByteString, ByteString, Future[Unit]],
                  middle: Graph[BidiPorts[ByteString, String, Int, ByteString], Unit],
                  right: Flow[String, Int, Long]) = {
    val closed1: RunnableFlow[Future[Unit]] = left <~> middle <~> right <*> { case ((f, _), _) ⇒ f }
    val closed2: RunnableFlow[Future[Unit]] = left <~> middle <*> (_._1) <~> right <*> (_._1)
    val closed3: RunnableFlow[Long] = left <~>> middle <~>> right
    val closed4: RunnableFlow[(Future[Unit], Long)] = left <<~> middle <~> right
  }

  trait Graph[+P <: Ports, +M] extends Materializable {
    override type MaterializedType <: M
    type Ports = P
    def ports: P
  }

  /**
   * This imports g1 and g2 by copying into the builder before running the
   * user code block. The user defines how to combine the materialized values
   * of the parts, and the return value is the new source’s outlet port (which
   * may have been imported with the graphs as well).
   *
   * To be extended to higher arities using the boilerplate plugin.
   */
  def source[T, Mat](g1: Graph[_, _], g2: Graph[_, _])(
    combineMat: (g1.MaterializedType, g2.MaterializedType) ⇒ Mat)(
      f: FlowGraphBuilder ⇒ (g1.Ports, g2.Ports) ⇒ OutputPort[T]): Source[T, Mat] = ???

  def source[T]()(f: FlowGraphBuilder => OutputPort[T]): Source[T, Unit] = ???

  def example(g1: Source[Int, Future[Unit]], g2: Flow[Int, String, Unit]) =
    source(g1, g2)((f, _) ⇒ f) { implicit b ⇒
      (p1: SourcePorts[Int], p2: FlowPorts[Int, String]) ⇒
        import FlowGraphImplicits._
        p1.outlet ~> p2.inlet
        p2.outlet
    }

  class FlowGraphBuilder

  object FlowGraphImplicits {
    implicit class portArrow[T](val from: OutputPort[T]) extends AnyVal {
      def ~>(to: InputPort[T])(implicit b: FlowGraphBuilder): Unit = ???
    }
    implicit class sourceArrow[T](val from: Source[T, _]) extends AnyVal {
      def ~>(to: InputPort[T])(implicit b: FlowGraphBuilder): Unit = ???
    }
  }

  /*
   * Sketch for Junctions
   */
  def merge[T](n: Int)(implicit b: FlowGraphBuilder): MergePorts[T] = {
    /*
     * construct a MergeModule with MergePorts, add it to the layout and expose the ports
     * 
     * There is no DSL element like for Flow because this blueprint is generated and not passed around.
     */
    ???
  }

  final case class MergePorts[T](in: Vector[InputPort[T]], out: OutputPort[T]) extends Ports {
    override val inlets: Set[InputPort[_]] = in.toSet
    override val outlets: Set[OutputPort[_]] = Set(out)
  }

  def mergeExample =
    source() { implicit b =>
      import FlowGraphImplicits._
      val m = merge[Int](2)
      Source.empty[Int] ~> m.in(0)
      Source.empty[Int] ~> m.in(1)
      m.out
    }
}
