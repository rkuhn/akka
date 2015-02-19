/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream._

trait FlowModule[In, Out, Mat] extends StreamLayout.Module {
  override def subModules = Set.empty
  override def downstreams = Map.empty
  override def upstreams = Map.empty

  val inPort = new Inlet[In]("Flow.in")
  val outPort = new Outlet[Out]("Flow.out")
  override val shape = new FlowShape(inPort, outPort)
}

