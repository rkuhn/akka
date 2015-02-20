/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream.impl.Junctions._
import akka.stream.impl.GenJunctions._
import akka.stream.impl.Stages.{ MaterializingStageFactory, StageModule }
import akka.stream.impl._
import akka.stream.impl.StreamLayout._
import akka.stream._
import OperationAttributes.name

import scala.collection.immutable

object Merge {
  def apply[T](inputPorts: Int, attributes: OperationAttributes = OperationAttributes.none): Graph[UniformFanInShape[T, T], Unit] =
    new Graph[UniformFanInShape[T, T], Unit] {
      val shape = new UniformFanInShape[T, T](inputPorts)
      val module = new MergeModule(shape, OperationAttributes.name("Merge") and attributes)
    }
}

object MergePreferred {
  final class MergePreferredShape[T](secondaryPorts: Int) extends UniformFanInShape[T, T](secondaryPorts) {
    val preferred = newInlet[T]("preferred")
    override def deepCopy(): MergePreferredShape[T] = new MergePreferredShape(secondaryPorts)
  }

  def apply[T](secondaryPorts: Int, attributes: OperationAttributes = OperationAttributes.none): Graph[MergePreferredShape[T], Unit] =
    new Graph[MergePreferredShape[T], Unit] {
      val shape = new MergePreferredShape[T](secondaryPorts)
      val module = new MergePreferredModule(shape, OperationAttributes.name("MergePreferred") and attributes)
    }
}

object Broadcast {
  def apply[T](outputPorts: Int, attributes: OperationAttributes = OperationAttributes.none): Graph[UniformFanOutShape[T, T], Unit] =
    new Graph[UniformFanOutShape[T, T], Unit] {
      val shape = new UniformFanOutShape[T, T](outputPorts)
      val module = new BroadcastModule(shape, OperationAttributes.name("Broadcast") and attributes)
    }
}

object Balance {
  def apply[T](outputPorts: Int, waitForAllDownstreams: Boolean = false, attributes: OperationAttributes = OperationAttributes.none): Graph[UniformFanOutShape[T, T], Unit] =
    new Graph[UniformFanOutShape[T, T], Unit] {
      val shape = new UniformFanOutShape[T, T](outputPorts)
      val module = new BalanceModule(shape, waitForAllDownstreams, OperationAttributes.name("Balance") and attributes)
    }
}

object Zip {
  def apply[A, B](attributes: OperationAttributes = OperationAttributes.none): Graph[FanInShape2[A, B, (A, B)], Unit] =
    new Graph[FanInShape2[A, B, (A, B)], Unit] {
      val shape = new FanInShape2[A, B, (A, B)]
      val module = new ZipWith2Module[A, B, (A, B)](shape, Keep.both, OperationAttributes.name("Zip") and attributes)
    }
}

// FIXME express ZipWithXModule in terms of generic FanInShapeX
object ZipWith extends ZipWithApply

object Unzip {
  def apply[A, B](attributes: OperationAttributes = OperationAttributes.none): Graph[FanOutShape2[(A, B), A, B], Unit] =
    new Graph[FanOutShape2[(A, B), A, B], Unit] {
      val shape = new FanOutShape2[(A, B), A, B]
      val module = new UnzipModule(shape, OperationAttributes.name("Unzip") and attributes)
    }
}

object Concat {
  def apply[A](attributes: OperationAttributes = OperationAttributes.none): Graph[UniformFanInShape[A, A], Unit] =
    new Graph[UniformFanInShape[A, A], Unit] {
      val shape = new UniformFanInShape[A, A](2)
      val module = new ConcatModule(shape, OperationAttributes.name("Concat") and attributes)
    }
}

object Graph extends GraphApply {

  class Builder private[stream] () {
    private var moduleInProgress: Module = EmptyModule
    private var InletMapping = Map.empty[InPort, InPort]
    private var OutletMapping = Map.empty[OutPort, OutPort]

    private[stream] def chainEdge[A, B](from: Outlet[A], via: Flow[A, B, _]): Outlet[B] = {
      val flowCopy = via.carbonCopy()
      moduleInProgress =
        moduleInProgress
          .grow(flowCopy.module)
          .connect(resolvePort(from), flowCopy.inlet)
      flowCopy.outlet
    }

    def addEdge[A, B, M](from: Outlet[A], via: Flow[A, B, M], to: Inlet[B]): Unit = {
      val flowCopy = via.carbonCopy()
      moduleInProgress =
        moduleInProgress
          .grow(flowCopy.module)
          .connect(resolvePort(from), flowCopy.inlet)
          .connect(flowCopy.outlet, resolvePort(to))
    }

    def addEdge[T](from: Outlet[T], to: Inlet[T]): Unit = {
      moduleInProgress = moduleInProgress.connect(resolvePort(from), resolvePort(to))
    }

    /**
     * Import a graph into this module, performing a deep copy, discarding its
     * materialized value and returning the copied Ports that are now to be
     * connected.
     */
    def add[S <: Shape](graph: Graph[S, _]): S = importGraph(graph, Keep.left)

    def add[T](s: Source[T, _]): Outlet[T] = importGraph(s, Keep.left).outlet
    def add[T](s: Sink[T, _]): Inlet[T] = importGraph(s, Keep.left).inlet

    // Assumes that junction is a new instance, so no copying needed here
    private[stream] def addModule(module: Module): Unit = {
      moduleInProgress = moduleInProgress.grow(module)
    }

    private[stream] def importModule(module: Module): Mapping = {
      val moduleCopy = module.carbonCopy()
      addModule(moduleCopy.module)
      moduleCopy
    }

    private[stream] def remapPorts[S <: Shape, M1, M2](graph: Graph[S, _], moduleCopy: Mapping): S = {
      /*
       * This cast should not be necessary if we could express the constraint
       * that deepCopy returns the same type as its receiver has. Would’a, could’a.
       */
      val ports = graph.shape.deepCopy().asInstanceOf[S]

      val newInletMap = ports.inlets.zip(graph.shape.inlets) map {
        case (newGraphPort, oldGraphPort) ⇒
          newGraphPort -> moduleCopy.inPorts(oldGraphPort)
      }
      val newOutletMap = ports.outlets.zip(graph.shape.outlets) map {
        case (newGraphPort, oldGraphPort) ⇒
          newGraphPort -> moduleCopy.outPorts(oldGraphPort)
      }
      InletMapping ++= newInletMap
      OutletMapping ++= newOutletMap
      ports
    }

    private[stream] def importGraph[S <: Shape, M1, M2](graph: Graph[S, _], combine: (M1, M2) ⇒ Any): S = {
      val moduleCopy = graph.module.carbonCopy()
      moduleInProgress = moduleInProgress.grow(
        moduleCopy.module,
        combine.asInstanceOf[(Any, Any) ⇒ Any])

      remapPorts(graph, moduleCopy)
    }

    private[stream] def resolvePort[T](port: InPort): InPort = {
      InletMapping.getOrElse(port, port)
    }

    private[stream] def resolvePort[T](port: OutPort): OutPort = {
      OutletMapping.getOrElse(port, port)
    }

    private[stream] def andThen(port: OutPort, op: StageModule): Unit = {
      addModule(op)
      moduleInProgress = moduleInProgress.connect(resolvePort(port), op.inPort)
    }

    private[stream] def buildRunnable[Mat](): RunnableFlow[Mat] = {
      if (!moduleInProgress.isRunnable) {
        throw new IllegalStateException(
          "Cannot build the RunnableFlow because there are unconnected ports: " +
            (moduleInProgress.outPorts ++ moduleInProgress.inPorts).mkString(", "))
      }
      new RunnableFlow(moduleInProgress)
    }

    private[stream] def buildSource[T, Mat](Outlet: Outlet[T]): Source[T, Mat] = {
      if (moduleInProgress.isRunnable)
        throw new IllegalStateException("Cannot build the Source since no ports remain open")
      if (!moduleInProgress.isSource)
        throw new IllegalStateException(
          s"Cannot build Source with open inputs (${moduleInProgress.inPorts.mkString(",")}) and outputs (${moduleInProgress.outPorts.mkString(",")})")
      if (moduleInProgress.outPorts.head != resolvePort(Outlet))
        throw new IllegalStateException(s"provided Outlet $Outlet does not equal the module’s open Outlet ${moduleInProgress.outPorts.head}")
      new Source(moduleInProgress, resolvePort(Outlet).asInstanceOf[Outlet[T]])
    }

    private[stream] def buildFlow[In, Out, Mat](inlet: Inlet[In], outlet: Outlet[Out]): Flow[In, Out, Mat] = {
      if (!moduleInProgress.isFlow)
        throw new IllegalStateException(
          s"Cannot build Flow with open inputs (${moduleInProgress.inPorts.mkString(",")}) and outputs (${moduleInProgress.outPorts.mkString(",")})")
      if (moduleInProgress.outPorts.head != resolvePort(outlet))
        throw new IllegalStateException(s"provided Outlet $outlet does not equal the module’s open Outlet ${moduleInProgress.outPorts.head}")
      if (moduleInProgress.inPorts.head != resolvePort(inlet))
        throw new IllegalStateException(s"provided Inlet $inlet does not equal the module’s open Inlet ${moduleInProgress.inPorts.head}")
      new Flow(moduleInProgress, resolvePort(inlet).asInstanceOf[Inlet[In]], resolvePort(outlet).asInstanceOf[Outlet[Out]])
    }

    private[stream] def buildSink[T, Mat](Inlet: Inlet[T]): Sink[T, Mat] = {
      if (moduleInProgress.isRunnable)
        throw new IllegalStateException("Cannot build the Sink since no ports remain open")
      if (!moduleInProgress.isSink)
        throw new IllegalStateException(
          s"Cannot build Sink with open inputs (${moduleInProgress.inPorts.mkString(",")}) and outputs (${moduleInProgress.outPorts.mkString(",")})")
      if (moduleInProgress.inPorts.head != resolvePort(Inlet))
        throw new IllegalStateException(s"provided Inlet $Inlet does not equal the module’s open Inlet ${moduleInProgress.inPorts.head}")
      new Sink(moduleInProgress, resolvePort(Inlet).asInstanceOf[Inlet[T]])
    }

    private[stream] def module: Module = moduleInProgress

  }

  object Implicits {

    trait CombinerBase[+T] extends Any {
      def importAndGetPort(b: Graph.Builder): Outlet[T]

      def ~>(to: Inlet[T])(implicit b: Graph.Builder): Unit = {
        b.addEdge(importAndGetPort(b), to)
      }

      def ~>[Out](via: Flow[T, Out, _])(implicit b: Graph.Builder): PortOps[Out, Unit] = {
        b.chainEdge(importAndGetPort(b), via)
      }

      def ~>(to: Sink[T, _])(implicit b: Graph.Builder): Unit = {
        val sinkCopy = to.carbonCopy()
        b.addModule(sinkCopy.module)
        b.addEdge(importAndGetPort(b), sinkCopy.inlet)
      }
    }

    class PortOps[+Out, +Mat](port: OutPort, b: Graph.Builder) extends FlowOps[Out, Mat] with CombinerBase[Out] {
      override type Repr[+O, +M] = PortOps[O, M]

      def outlet: Outlet[Out] = port.asInstanceOf[Outlet[Out]]

      override def withAttributes(attr: OperationAttributes): Repr[Out, Mat] =
        throw new UnsupportedOperationException("Cannot set attributes on chained ops from a junction output port")

      override private[scaladsl] def andThen[U](op: StageModule): Repr[U, Mat] = {
        b.andThen(port, op)
        new PortOps(op.outPort, b)
      }

      override private[scaladsl] def andThenMat[U, Mat2](op: MaterializingStageFactory): Repr[U, Mat2] = {
        // We don't track materialization here
        b.andThen(port, op)
        new PortOps(op.outPort, b)
      }

      override def importAndGetPort(b: Graph.Builder): Outlet[Out] = port.asInstanceOf[Outlet[Out]]
    }

    import scala.language.implicitConversions
    implicit def port2flow[T](from: Outlet[T])(implicit b: Graph.Builder): PortOps[T, Unit] = new PortOps(from, b)

    implicit class SourceArrow[T](val s: Source[T, _]) extends AnyVal with CombinerBase[T] {
      override def importAndGetPort(b: Graph.Builder): Outlet[T] = {
        val mapping = b.importModule(s.module)
        mapping.outPorts(s.outlet).asInstanceOf[Outlet[T]]
      }
    }

  }

}
