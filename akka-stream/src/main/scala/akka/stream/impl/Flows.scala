/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream._
import akka.stream.impl.StreamLayout.LinearModule

trait FlowModule[In, Out, Mat] extends StreamLayout.LinearModule {
  override def replaceShape(s: Shape) =
    if (s == shape) this
    else throw new UnsupportedOperationException("cannot replace the shape of a FlowModule")

  val inPort = new Inlet[In]("Flow.in")
  val outPort = new Outlet[Out]("Flow.out")
  override val shape = new FlowShape(inPort, outPort)

  override val inPortOption: Option[InPort] = Some(inPort)
  override val outPortOption: Option[OutPort] = Some(outPort)
  override def stages: Vector[LinearModule] = Vector.empty
}

