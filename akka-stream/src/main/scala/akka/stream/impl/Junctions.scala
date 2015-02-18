package akka.stream.impl

import akka.stream.impl.StreamLayout.{ Mapping, OutPort, InPort, Module }
import akka.stream.scaladsl.FlexiRoute.RouteLogic
import akka.stream.scaladsl.{ Graphs, OperationAttributes }
import akka.stream.scaladsl.FlexiMerge.MergeLogic

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
    ins: Vector[Graphs.InPort[T]],
    out: Graphs.OutPort[T],
    override val attributes: OperationAttributes = name("merge")) extends FaninModule {

    override val inPorts: Set[InPort] = ins.toSet
    override val outPorts: Set[OutPort] = Set(out)

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newMerge = MergeModule(
        ins.map(i ⇒ new Graphs.InPort[Any](i.toString)),
        new Graphs.OutPort[Any](out.toString),
        attributes)

      Mapping(newMerge, ins.zip(newMerge.ins).toMap, Map(out -> newMerge.out))
    }
  }

  final case class BroadcastModule[T](
    in: Graphs.InPort[T],
    outs: Vector[Graphs.OutPort[T]],
    override val attributes: OperationAttributes = name("broadcast")) extends FanoutModule {

    override val inPorts: Set[InPort] = Set(in)
    override val outPorts: Set[OutPort] = outs.toSet

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newMerge = BroadcastModule(
        new Graphs.InPort[Any](in.toString),
        outs.map(o ⇒ new Graphs.OutPort[Any](o.toString)),
        attributes)

      Mapping(newMerge, Map(in -> newMerge.in), outs.zip(newMerge.outs).toMap)
    }
  }

  final case class MergePreferredModule[T](
    preferred: Graphs.InPort[T],
    ins: Vector[Graphs.InPort[T]],
    out: Graphs.OutPort[T],
    override val attributes: OperationAttributes = name("preferred")) extends FaninModule {
    override val inPorts: Set[InPort] = ins.toSet + preferred
    override val outPorts: Set[OutPort] = Set(out)

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newMerge = MergePreferredModule(
        new Graphs.InPort[Any](preferred.toString),
        ins.map(i ⇒ new Graphs.InPort[Any](i.toString)),
        new Graphs.OutPort[Any](out.toString),
        attributes)

      Mapping(newMerge, (ins.zip(newMerge.ins) :+ (preferred -> newMerge.preferred)).toMap, Map(out -> newMerge.out))
    }
  }

  final case class FlexiMergeModule[T, P <: Graphs.Ports](
    ports: P,
    flexi: P ⇒ MergeLogic[T],
    override val attributes: OperationAttributes = name("flexiMerge")) extends FaninModule {

    require(ports.outlets.size == 1, "FlexiMerge can have only one output port")

    override val inPorts: Set[InPort] = ports.inlets.toSet
    override val outPorts: Set[OutPort] = ports.outlets.toSet

    override def withAttributes(attributes: OperationAttributes): Module = copy(attributes = attributes)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newModule = new FlexiMergeModule(ports.deepCopy().asInstanceOf[P], flexi, attributes)
      Mapping(newModule, Map(ports.inlets.zip(newModule.ports.inlets): _*), Map(ports.outlets.head → newModule.ports.outlets.head))
    }
  }

  final case class FlexiRouteModule[T, P <: Graphs.Ports](
    ports: P,
    flexi: P ⇒ RouteLogic[T],
    override val attributes: OperationAttributes = name("flexiRoute")) extends FanoutModule {

    require(ports.inlets.size == 1, "FlexiRoute can have only one input port")

    override val inPorts: Set[InPort] = ports.inlets.toSet
    override val outPorts: Set[OutPort] = ports.outlets.toSet

    override def withAttributes(attributes: OperationAttributes): Module = copy(attributes = attributes)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newModule = new FlexiRouteModule(ports.deepCopy().asInstanceOf[P], flexi, attributes)
      Mapping(newModule, Map(ports.inlets.zip(newModule.ports.inlets): _*), Map(ports.outlets.head → newModule.ports.outlets.head))
    }
  }

  final case class BalanceModule[T](
    in: Graphs.InPort[T],
    outs: Vector[Graphs.OutPort[T]],
    waitForAllDownstreams: Boolean,
    override val attributes: OperationAttributes = name("broadcast")) extends FanoutModule {

    override val inPorts: Set[InPort] = Set(in)
    override val outPorts: Set[OutPort] = outs.toSet

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newMerge = BalanceModule(
        new Graphs.InPort[Any](in.toString),
        outs.map(o ⇒ new Graphs.OutPort[Any](o.toString)),
        waitForAllDownstreams,
        attributes)

      Mapping(newMerge, Map(in -> newMerge.in), outs.zip(newMerge.outs).toMap)
    }
  }

  final case class UnzipModule[A, B](
    in: Graphs.InPort[(A, B)],
    left: Graphs.OutPort[A],
    right: Graphs.OutPort[B],
    override val attributes: OperationAttributes = name("unzip")) extends FanoutModule {

    override val inPorts: Set[InPort] = Set(in)
    override val outPorts: Set[OutPort] = Set(left, right)

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newZip2 = UnzipModule(
        new Graphs.InPort[(A, B)](in.toString),
        new Graphs.OutPort[A](left.toString),
        new Graphs.OutPort[B](right.toString),
        attributes)

      Mapping(newZip2, Map(in -> newZip2.in), Map(right -> newZip2.right, left -> newZip2.right))
    }

  }

  final case class ConcatModule[A1, A2, B](
    first: Graphs.InPort[A1],
    second: Graphs.InPort[A2],
    out: Graphs.OutPort[B],
    override val attributes: OperationAttributes = name("concat")) extends FaninModule {

    override val inPorts: Set[InPort] = Set(first, second)
    override val outPorts: Set[OutPort] = Set(out)

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)

    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newZip2 = ConcatModule(
        new Graphs.InPort[A1](first.toString),
        new Graphs.InPort[A1](second.toString),
        new Graphs.OutPort[B](out.toString),
        attributes)

      Mapping(newZip2, Map(first -> newZip2.first, second -> newZip2.second), Map(out -> newZip2.out))
    }
  }

}