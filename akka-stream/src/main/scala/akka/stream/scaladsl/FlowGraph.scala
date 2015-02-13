/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream.impl.Junctions._
import akka.stream.impl.GenJunctions._
import akka.stream.impl.Stages.{ MaterializingStageFactory, StageModule }
import akka.stream.impl._
import akka.stream.impl.StreamLayout._
import akka.stream.scaladsl.FlowGraph.FlowGraphBuilder
import akka.stream.scaladsl.Graphs.{ InPort, OutPort }

import scala.collection.immutable

object Merge {

  final case class MergePorts[T](in: Vector[InPort[T]], out: OutPort[T]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = in
    override val outlets: immutable.Seq[OutPort[_]] = List(out)

    override def deepCopy(): MergePorts[T] = MergePorts(in.map(i ⇒ new InPort[T](i.toString)), new OutPort(out.toString))
  }

  def apply[T](inputPorts: Int)(implicit b: FlowGraphBuilder): MergePorts[T] = {
    val mergeModule = new MergeModule(
      Vector.fill(inputPorts)(new InPort[T]("Merge.in")),
      new OutPort[T]("Merge.out"))
    b.addModule(mergeModule)
    MergePorts(mergeModule.ins, mergeModule.out)
  }

}

object MergePreferred {
  final case class MergePreferredPorts[T](preferred: InPort[T], in: Vector[InPort[T]], out: OutPort[T]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = in :+ preferred
    override val outlets: immutable.Seq[OutPort[_]] = List(out)

    override def deepCopy(): MergePreferredPorts[T] =
      MergePreferredPorts(new InPort(preferred.toString), in.map(i ⇒ new InPort[T](i.toString)), new OutPort(out.toString))
  }

  def apply[T](secondaryPorts: Int)(implicit b: FlowGraphBuilder): MergePreferredPorts[T] = {
    val mergeModule = new MergePreferredModule(
      new InPort[T]("Preferred.preferred"),
      Vector.fill(secondaryPorts)(new InPort[T]("Preferred.in")),
      new OutPort[T]("Preferred.out"))
    b.addModule(mergeModule)
    MergePreferredPorts(mergeModule.preferred, mergeModule.ins, mergeModule.out)
  }
}

object Broadcast {

  final case class BroadcastPorts[T](in: InPort[T], out: Vector[OutPort[T]]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = List(in)
    override val outlets: immutable.Seq[OutPort[_]] = out

    override def deepCopy(): BroadcastPorts[T] =
      BroadcastPorts(new InPort(in.toString), out.map(o ⇒ new OutPort[T](o.toString)))
  }

  def apply[T](outputPorts: Int)(implicit b: FlowGraphBuilder): BroadcastPorts[T] = {
    val bcastModule = new BroadcastModule(
      new InPort[T]("Bcast.in"),
      Vector.fill(outputPorts)(new OutPort[T]("Bcast.out")))
    b.addModule(bcastModule)
    BroadcastPorts(bcastModule.in, bcastModule.outs)
  }
}

object Balance {

  final case class BalancePorts[T](in: InPort[T], out: Vector[OutPort[T]]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = List(in)
    override val outlets: immutable.Seq[OutPort[_]] = out

    override def deepCopy(): BalancePorts[T] =
      BalancePorts(new InPort(in.toString), out.map(o ⇒ new OutPort[T](o.toString)))
  }

  def apply[T](outputPorts: Int, waitForAllDownstreams: Boolean = false)(implicit b: FlowGraphBuilder): BalancePorts[T] = {
    val bcastModule = new BalanceModule(
      new InPort[T]("Balance.in"),
      Vector.fill(outputPorts)(new OutPort[T]("Balance.out")),
      waitForAllDownstreams)
    b.addModule(bcastModule)
    BalancePorts(bcastModule.in, bcastModule.outs)
  }

}

object Zip {

  final case class ZipPorts[A, B](left: InPort[A], right: InPort[B], out: OutPort[(A, B)]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = List(left, right)
    override val outlets: immutable.Seq[OutPort[_]] = List(out)

    override def deepCopy(): ZipPorts[A, B] =
      ZipPorts(new InPort(left.toString), new InPort(right.toString), new OutPort(out.toString))
  }

  def apply[A, B](implicit b: FlowGraphBuilder): ZipPorts[A, B] = {
    val zipWithModule = new ZipWith2Module(
      new InPort[A]("Zip.left"),
      new InPort[B]("Zip.right"),
      new OutPort[(A, B)]("Zip.out"),
      (a: A, b: B) ⇒ (a, b))
    b.addModule(zipWithModule)
    ZipPorts(zipWithModule.in1, zipWithModule.in2, zipWithModule.out)
  }

}

object ZipWith {

  def apply[A1, A2, O](zipper: (A1, A2) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith2(zipper)

  def apply[A1, A2, A3, O](zipper: (A1, A2, A3) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith3(zipper)

  def apply[A1, A2, A3, A4, O](zipper: (A1, A2, A3, A4) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith4(zipper)

  def apply[A1, A2, A3, A4, A5, O](zipper: (A1, A2, A3, A4, A5) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith5(zipper)

  def apply[A1, A2, A3, A4, A5, A6, O](zipper: (A1, A2, A3, A4, A5, A6) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith6(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, O](zipper: (A1, A2, A3, A4, A5, A6, A7) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith7(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith8(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith9(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith10(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith11(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith12(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith13(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith14(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith15(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith16(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith17(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith18(zipper)

  def apply[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, O](zipper: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19) ⇒ O)(implicit b: FlowGraphBuilder) =
    ZipWith19(zipper)

}

object Unzip {

  final case class UnzipPorts[A, B](in: InPort[(A, B)], left: OutPort[A], right: OutPort[B]) extends Graphs.Ports {
    override def inlets: immutable.Seq[InPort[_]] = List(in)
    override def outlets: immutable.Seq[OutPort[_]] = List(left, right)

    override def deepCopy(): UnzipPorts[A, B] =
      UnzipPorts(new InPort(in.toString), new OutPort(left.toString), new OutPort(right.toString))
  }

  def apply[A, B](implicit b: FlowGraphBuilder): UnzipPorts[A, B] = {
    val unzipModule = new UnzipModule(
      new InPort[(A, B)]("Unzip.in"),
      new OutPort[A]("Unzip.left"),
      new OutPort[B]("Unzip.right"))
    b.addModule(unzipModule)
    UnzipPorts(unzipModule.in, unzipModule.left, unzipModule.right)
  }
}

object Concat {

  final case class ConcatPorts[A](first: InPort[A], second: InPort[A], out: OutPort[A]) extends Graphs.Ports {
    override val inlets: immutable.Seq[InPort[_]] = List(first, second)
    override val outlets: immutable.Seq[OutPort[_]] = List(out)

    override def deepCopy(): ConcatPorts[A] =
      ConcatPorts(new InPort(first.toString), new InPort(second.toString), new OutPort(out.toString))
  }

  def apply[A](implicit b: FlowGraphBuilder): ConcatPorts[A] = {
    val concatModdule = new ConcatModule(
      new InPort[A]("concat.first"),
      new InPort[A]("concat.second"),
      new OutPort[A]("concat.out"))
    b.addModule(concatModdule)
    ConcatPorts(concatModdule.first, concatModdule.second, concatModdule.out)
  }

}

object FlowGraph extends FlowGraphApply {
  import akka.stream.scaladsl.Graphs._

  def apply(buildBlock: (FlowGraphBuilder) ⇒ Unit): RunnableFlow[Unit] = {
    val builder = new FlowGraphBuilder
    buildBlock(builder)
    builder.buildRunnable()
  }

  class FlowGraphBuilder private[stream] () {
    private var moduleInProgress: Module = EmptyModule
    private var inPortMapping = Map.empty[StreamLayout.InPort, StreamLayout.InPort]
    private var outPortMapping = Map.empty[StreamLayout.OutPort, StreamLayout.OutPort]

    private[stream] def chainEdge[A, B](from: OutPort[A], via: Flow[A, B, _]): OutPort[B] = {
      val flowCopy = via.carbonCopy()
      moduleInProgress =
        moduleInProgress
          .grow(flowCopy.module)
          .connect(resolvePort(from), flowCopy.inlet)
      flowCopy.outlet
    }

    def addEdge[A, B](from: OutPort[A], via: Flow[A, B, _], to: InPort[B]): Unit = {
      val flowCopy = via.carbonCopy()
      moduleInProgress =
        moduleInProgress
          .grow(flowCopy.module)
          .connect(resolvePort(from), flowCopy.inlet)
          .connect(flowCopy.outlet, resolvePort(to))
    }

    def addEdge[T](from: OutPort[T], to: InPort[T]): Unit = {
      moduleInProgress = moduleInProgress.connect(resolvePort(from), resolvePort(to))
    }

    // Assumes that junction is a new instance, so no copying needed here
    private[stream] def addModule(module: Module): Unit = {
      moduleInProgress = moduleInProgress.grow(module)
    }

    private[stream] def importModule(module: Module): Mapping = {
      val moduleCopy = module.carbonCopy()
      addModule(moduleCopy.module)
      moduleCopy
    }

    private[stream] def remapPorts[M1, M2](graph: Graph[Ports, _], moduleCopy: Mapping): Ports = {
      val ports = graph.ports.deepCopy()

      val newInPortMap = ports.inlets.zip(graph.ports.inlets) map {
        case (newGraphPort, oldGraphPort) ⇒
          newGraphPort -> moduleCopy.inPorts(oldGraphPort)
      }
      val newOutPortMap = ports.outlets.zip(graph.ports.outlets) map {
        case (newGraphPort, oldGraphPort) ⇒
          newGraphPort -> moduleCopy.outPorts(oldGraphPort)
      }
      inPortMapping ++= newInPortMap
      outPortMapping ++= newOutPortMap
      ports
    }

    private[stream] def importGraph[M1, M2](graph: Graph[Ports, _], combine: (M1, M2) ⇒ Any): Ports = {
      val moduleCopy = graph.module.carbonCopy()
      moduleInProgress = moduleInProgress.grow(
        moduleCopy.module,
        (m1: Any, m2: Any) ⇒ combine.asInstanceOf[(Any, Any) ⇒ Any](m1, m2))

      remapPorts(graph, moduleCopy)
    }

    private[stream] def resolvePort[T](port: StreamLayout.InPort): StreamLayout.InPort = {
      inPortMapping.getOrElse(port, port)
    }

    private[stream] def resolvePort[T](port: StreamLayout.OutPort): StreamLayout.OutPort = {
      outPortMapping.getOrElse(port, port)
    }

    private[stream] def andThen(port: StreamLayout.OutPort, op: StageModule): Unit = {
      addModule(op)
      moduleInProgress = moduleInProgress.connect(resolvePort(port), op.inPort)
    }

    private[stream] def buildRunnable(): RunnableFlow[Unit] = {
      if (!moduleInProgress.isRunnable) {
        throw new IllegalStateException(
          "Cannot build the RunnableFlow because there are unconnected ports: " +
            (moduleInProgress.outPorts ++ moduleInProgress.inPorts).mkString(","))
      }
      new RunnableFlow[Unit](moduleInProgress)
    }

    private[stream] def module: Module = moduleInProgress

  }

  object Implicits {

    trait CombinerBase[+T] extends Any {
      def importAndGetPort(b: FlowGraphBuilder): OutPort[T]

      def ~>(to: InPort[T])(implicit b: FlowGraphBuilder): Unit = {
        b.addEdge(importAndGetPort(b), to)
      }

      def ~>[Out](via: Flow[T, Out, _])(implicit b: FlowGraphBuilder): PortOps[Out, Unit] = {
        b.chainEdge(importAndGetPort(b), via)
      }

      def ~>(to: Sink[T, _])(implicit b: FlowGraphBuilder): Unit = {
        val sinkCopy = to.carbonCopy()
        b.addModule(sinkCopy.module)
        b.addEdge(importAndGetPort(b), sinkCopy.inlet)
      }
    }

    class PortOps[+Out, +Mat](port: StreamLayout.OutPort, b: FlowGraphBuilder) extends FlowOps[Out, Mat] with CombinerBase[Out] {
      override type Repr[+O, +M] = PortOps[O, M]

      override private[scaladsl] def withAttributes(attr: OperationAttributes): Repr[Out, Mat] =
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

      override def importAndGetPort(b: FlowGraphBuilder): OutPort[Out] = port.asInstanceOf[Graphs.OutPort[Out]]
    }

    import scala.language.implicitConversions
    implicit def port2flow[T](from: OutPort[T])(implicit b: FlowGraphBuilder): PortOps[T, Unit] = new PortOps(from, b)

    implicit class SourceArrow[T](val s: Source[T, _]) extends AnyVal with CombinerBase[T] {
      override def importAndGetPort(b: FlowGraphBuilder): OutPort[T] = {
        val mapping = b.importModule(s.module)
        mapping.outPorts(s.outlet).asInstanceOf[OutPort[T]]
      }
    }

  }

}
