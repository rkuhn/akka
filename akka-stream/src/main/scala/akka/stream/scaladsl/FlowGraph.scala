package akka.stream.scaladsl

import akka.stream.impl.Junctions.{ MergeModule, JunctionModule }
import akka.stream.impl.StreamLayout._
import akka.stream.scaladsl.Graphs.{ InPort, OutPort }

object Merge {
  import FlowGraph._

  final case class MergePorts[T](in: Vector[InPort[T]], out: OutPort[T]) extends Graphs.Ports {
    override val inlets: Set[InPort[_]] = in.toSet
    override val outlets: Set[OutPort[_]] = Set(out)
  }

  def apply[T](inputPorts: Int)(implicit b: FlowGraphBuilder): MergePorts[T] = {
    val mergeModule = new MergeModule(Vector.fill(inputPorts)(new InPort[T]("Merge.in")), new OutPort[T]("Merge.out"))
    b.addModule(mergeModule)
    MergePorts(mergeModule.ins, mergeModule.out)
  }

}

object FlowGraph {
  import Graphs._

  def apply(buildBlock: (FlowGraphBuilder) ⇒ Unit): RunnableFlow[Unit] = {
    val builder = new FlowGraphBuilder
    buildBlock(builder)
    builder.buildRunnable()
  }

  class FlowGraphBuilder {
    private var moduleInProgress: Module = EmptyModule

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

    trait CombinerBase[T] extends Any {
      def importAndGetPort(b: FlowGraphBuilder): OutPort[T]

      def ~>(to: InPort[T])(implicit b: FlowGraphBuilder): Unit = {
        b.addEdge(importAndGetPort(b), to)
      }

      def ~>[Out](via: Flow[T, Out, _])(implicit b: FlowGraphBuilder): PortArrow[Out] = {
        b.chainEdge(importAndGetPort(b), via)
      }

      def ~>(to: Sink[T, _])(implicit b: FlowGraphBuilder): Unit = {
        val sinkCopy = to.carbonCopy()
        b.addModule(sinkCopy.module)
        b.addEdge(importAndGetPort(b), sinkCopy.inlet)
      }
    }

    implicit class PortArrow[T](val from: OutPort[T]) extends AnyVal with CombinerBase[T] {
      override def importAndGetPort(b: FlowGraphBuilder): OutPort[T] = from
    }

    implicit class SourceArrow[T](val s: Source[T, _]) extends AnyVal with CombinerBase[T] {
      override def importAndGetPort(b: FlowGraphBuilder): OutPort[T] = {
        val mapping = b.importModule(s.module)
        mapping.outPorts(s.outlet).asInstanceOf[OutPort[T]]
      }
    }

  }

}
