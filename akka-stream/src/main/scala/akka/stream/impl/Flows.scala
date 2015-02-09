/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.impl.StreamLayout.Module
import akka.stream.scaladsl.{ Graphs, OperationAttributes }

trait FlowModule[In, Out, Mat] extends StreamLayout.Module {
  override def subModules = Set.empty
  override def downstreams = Map.empty
  override def upstreams = Map.empty
  val inPort = new Graphs.InPort[In]("FIXME")
  val outPort = new Graphs.OutPort[Out]("FIXME")
  override val inPorts: Set[StreamLayout.InPort] = Set(inPort)
  override val outPorts: Set[StreamLayout.OutPort] = Set(outPort)

}

