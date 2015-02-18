/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.scaladsl.Graphs

trait FlowModule[In, Out, Mat] extends StreamLayout.Module {
  override def subModules = Set.empty
  override def downstreams = Map.empty
  override def upstreams = Map.empty
  val inPort = new Graphs.InPort[In]("Flow.in")
  val outPort = new Graphs.OutPort[Out]("Flow.out")
  override val inPorts: Set[StreamLayout.InPort] = Set(inPort)
  override val outPorts: Set[StreamLayout.OutPort] = Set(outPort)

}

