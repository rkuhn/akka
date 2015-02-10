package akka.stream.scaladsl

import akka.stream.impl.Junctions._
import akka.stream.impl.Stages.{ MaterializingStageFactory, StageModule }
import akka.stream.impl.StreamLayout
import akka.stream.impl.StreamLayout._
import akka.stream.scaladsl.FlowGraph.FlowGraphBuilder
import akka.stream.scaladsl.Graphs.{ InPort, OutPort }

object Merge {

  final case class MergePorts[T](in: Vector[InPort[T]], out: OutPort[T]) extends Graphs.Ports {
    override val inlets: Set[InPort[_]] = in.toSet
    override val outlets: Set[OutPort[_]] = Set(out)
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
    override val inlets: Set[InPort[_]] = in.toSet + preferred
    override val outlets: Set[OutPort[_]] = Set(out)
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
    override val inlets: Set[InPort[_]] = Set(in)
    override val outlets: Set[OutPort[_]] = out.toSet
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
    override val inlets: Set[InPort[_]] = Set(in)
    override val outlets: Set[OutPort[_]] = out.toSet
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

object ZipWith2 {

  final case class ZipWith2Ports[A1, A2, B](in1: InPort[A1], in2: InPort[A2], out: OutPort[B]) extends Graphs.Ports {
    override val inlets: Set[InPort[_]] = Set(in1, in2)
    override val outlets: Set[OutPort[_]] = Set(out)
  }

  def apply[A1, A2, B](zipper: (A1, A2) ⇒ B)(implicit b: FlowGraphBuilder): ZipWith2Ports[A1, A2, B] = {
    val zipWithModule = new ZipWithModule2(
      new InPort[A1]("ZipWith2.in1"),
      new InPort[A2]("ZipWith2.in2"),
      new OutPort[B]("ZipWith2.out"),
      zipper)
    b.addModule(zipWithModule)
    ZipWith2Ports(zipWithModule.in1, zipWithModule.in2, zipWithModule.out)
  }

}

object Zip {

  final case class ZipPorts[A, B](left: InPort[A], right: InPort[B], out: OutPort[(A, B)]) extends Graphs.Ports {
    override val inlets: Set[InPort[_]] = Set(left, right)
    override val outlets: Set[OutPort[_]] = Set(out)
  }

  def apply[A, B](implicit b: FlowGraphBuilder): ZipPorts[A, B] = {
    val zipWithModule = new ZipWithModule2(
      new InPort[A]("Zip.left"),
      new InPort[B]("Zip.right"),
      new OutPort[(A, B)]("Zip.out"),
      (a: A, b: B) ⇒ (a, b))
    b.addModule(zipWithModule)
    ZipPorts(zipWithModule.in1, zipWithModule.in2, zipWithModule.out)
  }

}

object Unzip {

  final case class UnzipPorts[A, B](in: InPort[(A, B)], left: OutPort[A], right: OutPort[B]) extends Graphs.Ports {
    override def inlets: Set[InPort[_]] = Set(in)
    override def outlets: Set[OutPort[_]] = Set(left, right)
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
    override val inlets: Set[InPort[_]] = Set(first, second)
    override val outlets: Set[OutPort[_]] = Set(out)
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

object FlowGraph {
  import Graphs._

  def apply(buildBlock: (FlowGraphBuilder) ⇒ Unit): RunnableFlow[Unit] = {
    val builder = new FlowGraphBuilder
    buildBlock(builder)
    builder.buildRunnable()
  }

  class FlowGraphBuilder private[stream] () {
    private[stream] var moduleInProgress: Module = EmptyModule

    private[stream] def chainEdge[A, B](from: OutPort[A], via: Flow[A, B, _]): OutPort[B] = {
      val flowCopy = via.carbonCopy()
      moduleInProgress =
        moduleInProgress
          .grow(flowCopy.module)
          .connect(from, flowCopy.inlet)
      flowCopy.outlet
    }

    def addEdge[A, B](from: OutPort[A], via: Flow[A, B, _], to: InPort[B]): Unit = {
      val flowCopy = via.carbonCopy()
      moduleInProgress =
        moduleInProgress
          .grow(flowCopy.module)
          .connect(from, flowCopy.inlet)
          .connect(flowCopy.outlet, to)
    }

    def addEdge[T](from: OutPort[T], to: InPort[T]): Unit = {
      moduleInProgress = moduleInProgress.connect(from, to)
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

    private[stream] def importModule(module: Module, combine: (Any, Any) ⇒ Any): Mapping = {
      val moduleCopy = module.carbonCopy()
      moduleInProgress = moduleInProgress.grow(moduleCopy.module, combine)
      moduleCopy
    }

    private[stream] def buildRunnable(): RunnableFlow[Unit] = {
      if (!moduleInProgress.isRunnable) {
        throw new IllegalStateException(
          "Cannot build the RunnableFlow because there are unconnected ports: " +
            (moduleInProgress.outPorts ++ moduleInProgress.inPorts).mkString(","))
      }
      new RunnableFlow[Unit](moduleInProgress)
    }

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
        b.addModule(op)
        b.moduleInProgress = b.moduleInProgress.connect(port, op.inPort)
        new PortOps(op.outPort, b)
      }

      override private[scaladsl] def andThenMat[U, Mat2](op: MaterializingStageFactory): Repr[U, Mat2] = {
        b.moduleInProgress = b.moduleInProgress.grow(op, (m: Unit, m2: Mat2) ⇒ m2).connect(port, op.inPort)
        new PortOps(op.outPort, b)
      }

      override def importAndGetPort(b: FlowGraphBuilder): OutPort[Out] = port.asInstanceOf[Graphs.OutPort[Out]]
    }

    import language.implicitConversions
    implicit def port2flow[T](from: OutPort[T])(implicit b: FlowGraphBuilder): PortOps[T, Unit] = new PortOps(from, b)

    implicit class SourceArrow[T](val s: Source[T, _]) extends AnyVal with CombinerBase[T] {
      override def importAndGetPort(b: FlowGraphBuilder): OutPort[T] = {
        val mapping = b.importModule(s.module)
        mapping.outPorts(s.outlet).asInstanceOf[OutPort[T]]
      }
    }

  }

}
