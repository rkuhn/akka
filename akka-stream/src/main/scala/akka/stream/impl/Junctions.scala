package akka.stream.impl

import akka.stream.impl.StreamLayout.{ Mapping, Module }
import akka.stream.scaladsl.FlexiRoute.RouteLogic
import akka.stream.scaladsl.OperationAttributes
import akka.stream.{ Inlet, Outlet, Shape, InPort, OutPort }
import akka.stream.scaladsl.FlexiMerge.MergeLogic
import akka.stream.UniformFanInShape
import akka.stream.UniformFanOutShape
import akka.stream.FanOutShape2
import akka.stream.scaladsl.MergePreferred

object Junctions {

  import OperationAttributes._

  sealed trait JunctionModule extends Module {
    override def subModules: Set[Module] = Set.empty

    override def downstreams: Map[OutPort, InPort] = Map.empty
    override def upstreams: Map[InPort, OutPort] = Map.empty

  }

  // note: can't be sealed as we have boilerplate generated classes which must extend FaninModule/FanoutModule
  private[akka] trait FaninModule extends JunctionModule
  private[akka] trait FanoutModule extends JunctionModule

  final case class MergeModule[T](
    shape: UniformFanInShape[T, T],
    override val attributes: OperationAttributes = name("merge")) extends FaninModule {

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newMerge = MergeModule(shape.deepCopy(), attributes)
      Mapping(newMerge, shape.in.zip(newMerge.shape.in).toMap, Map(shape.out -> newMerge.shape.out))
    }
  }

  final case class BroadcastModule[T](
    shape: UniformFanOutShape[T, T],
    override val attributes: OperationAttributes = name("broadcast")) extends FanoutModule {

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newMerge = BroadcastModule(shape.deepCopy(), attributes)
      Mapping(newMerge, Map(shape.in -> newMerge.shape.in), shape.out.zip(newMerge.shape.out).toMap)
    }
  }

  final case class MergePreferredModule[T](
    shape: MergePreferred.MergePreferredShape[T],
    override val attributes: OperationAttributes = name("preferred")) extends FaninModule {

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newMerge = MergePreferredModule(shape.deepCopy(), attributes)
      Mapping(newMerge, shape.inlets.zip(newMerge.shape.inlets).toMap, Map(shape.out -> newMerge.shape.out))
    }
  }

  final case class FlexiMergeModule[T, S <: Shape](
    shape: S,
    flexi: S ⇒ MergeLogic[T],
    override val attributes: OperationAttributes = name("flexiMerge")) extends FaninModule {

    require(shape.outlets.size == 1, "FlexiMerge can have only one output port")

    override def withAttributes(attributes: OperationAttributes): Module = copy(attributes = attributes)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newModule = new FlexiMergeModule(shape.deepCopy().asInstanceOf[S], flexi, attributes)
      Mapping(newModule, Map(shape.inlets.zip(newModule.shape.inlets): _*), Map(shape.outlets.head → newModule.shape.outlets.head))
    }
  }

  final case class FlexiRouteModule[T, S <: Shape](
    shape: S,
    flexi: S ⇒ RouteLogic[T],
    override val attributes: OperationAttributes = name("flexiRoute")) extends FanoutModule {

    require(shape.inlets.size == 1, "FlexiRoute can have only one input port")

    override def withAttributes(attributes: OperationAttributes): Module = copy(attributes = attributes)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newModule = new FlexiRouteModule(shape.deepCopy().asInstanceOf[S], flexi, attributes)
      Mapping(newModule, Map(shape.inlets.zip(newModule.shape.inlets): _*), Map(shape.outlets.head → newModule.shape.outlets.head))
    }
  }

  final case class BalanceModule[T](
    shape: UniformFanOutShape[T, T],
    waitForAllDownstreams: Boolean,
    override val attributes: OperationAttributes = name("broadcast")) extends FanoutModule {

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newMerge = BalanceModule(shape.deepCopy(), waitForAllDownstreams, attributes)
      Mapping(newMerge, Map(shape.in -> newMerge.shape.in), shape.out.zip(newMerge.shape.out).toMap)
    }
  }

  final case class UnzipModule[A, B](
    shape: FanOutShape2[(A, B), A, B],
    override val attributes: OperationAttributes = name("unzip")) extends FanoutModule {

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newZip2 = UnzipModule(shape.deepCopy(), attributes)
      Mapping(newZip2, Map(shape.in -> newZip2.shape.in), Map(shape.out0 -> newZip2.shape.out0, shape.out1 -> newZip2.shape.out1))
    }

  }

  final case class ConcatModule[T](
    shape: UniformFanInShape[T, T],
    override val attributes: OperationAttributes = name("concat")) extends FaninModule {

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newZip2 = ConcatModule(shape.deepCopy(), attributes)

      Mapping(newZip2, Map(shape.in(0) -> newZip2.shape.in(0), shape.in(1) -> newZip2.shape.in(1)), Map(shape.out -> newZip2.shape.out))
    }
  }

}
