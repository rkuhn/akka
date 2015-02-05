/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.impl.StreamLayout.Module
import akka.stream.scaladsl.OperationAttributes
import akka.stream.scaladsl.Graphs

abstract class FlowModule[In, Out, Mat](val inPort: Graphs.InputPort[In], val outPort: Graphs.OutputPort[Out]) extends StreamLayout.Module {
  override def subModules = Set.empty
  override def downstreams = Map.empty
  override def upstreams = Map.empty
  override val inPorts: Set[StreamLayout.InPort] = Set(inPort)
  override val outPorts: Set[StreamLayout.OutPort] = Set(outPort)

}

