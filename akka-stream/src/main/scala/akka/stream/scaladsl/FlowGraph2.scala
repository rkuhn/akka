package akka.stream.scaladsl

import akka.stream.impl.StreamLayout._
import akka.stream.scaladsl.Graphs.{ InPort, OutPort }

object FlowGraph2 {
  import Graphs._

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

  }

  trait CombinerBase[T] extends Any {
    def fromPort: OutPort[T]

    def ~>(to: InPort[T])(implicit b: FlowGraphBuilder): Unit = {
      b.addEdge(fromPort, to)
    }

    def ~>[Out](via: Flow[T, Out, _])(implicit b: FlowGraphBuilder): PortArrow[Out] = {
      b.chainEdge(fromPort, via)
    }
  }

  implicit class PortArrow[T](val from: OutPort[T]) extends AnyVal with CombinerBase[T] {
    override def fromPort: OutPort[T] = from
  }

  implicit class SourceArrow[T](val s: Source[T, _]) extends AnyVal with CombinerBase[T] {
    override def fromPort: OutPort[T] = s.outlet
  }
}
