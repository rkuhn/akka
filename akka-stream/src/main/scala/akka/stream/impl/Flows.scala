/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.impl.StreamLayout.Module
import akka.stream.scaladsl.OperationAttributes

trait FlowModule[In, Out, Mat] extends StreamLayout.Module {
  override def subModules = Set.empty
  override def downstreams = Map.empty
  override def upstreams = Map.empty
  val inPort = new StreamLayout.InPort
  val outPort = new StreamLayout.OutPort
  override val inPorts = Set(inPort)
  override val outPorts = Set(outPort)

}

