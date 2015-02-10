package akka.stream.impl

import akka.stream.impl.StreamLayout.{ Mapping, OutPort, InPort, Module }
import akka.stream.scaladsl.{ Graphs, OperationAttributes }

object Junctions {

  sealed trait JunctionModule extends Module {
    override def subModules: Set[Module] = Set.empty

    override def downstreams: Map[OutPort, InPort] = Map.empty
    override def upstreams: Map[InPort, OutPort] = Map.empty

  }

  final case class MergeModule[T](
    ins: Vector[Graphs.InPort[T]],
    out: Graphs.OutPort[T],
    override val attributes: OperationAttributes = OperationAttributes.none) extends JunctionModule {

    override val inPorts: Set[InPort] = ins.toSet
    override val outPorts: Set[OutPort] = Set(out)

    override def withAttributes(attr: OperationAttributes): Module = copy(attributes = attr)
    override def carbonCopy: () ⇒ Mapping = () ⇒ {
      val newMerge = MergeModule(
        Vector.tabulate(ins.size)(i ⇒ new Graphs.InPort[Any](s"Merge.in($i)")),
        new Graphs.OutPort[Any]("Merge.out"),
        attributes)

      Mapping(newMerge, ins.zip(newMerge.ins).toMap, Map(out -> newMerge.out))
    }
  }

}
