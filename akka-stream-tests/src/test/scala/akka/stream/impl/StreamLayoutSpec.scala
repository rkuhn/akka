/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.testkit.AkkaSpec

class StreamLayoutSpec extends AkkaSpec {
  import StreamLayout._

  def graphStage(inPortCount: Int, outPortCount: Int): Atomic = Atomic(
    List.fill(inPortCount)(new InPort).toSet,
    List.fill(outPortCount)(new OutPort).toSet)

  def pipe(): Atomic = graphStage(1, 1)
  def source(): Atomic = graphStage(0, 1)
  def sink(): Atomic = graphStage(1, 0)

  "StreamLayout" must {

    "be able to model simple linear stages" in {
      val stage1 = pipe()

      stage1.inPorts.size should be(1)
      stage1.outPorts.size should be(1)
      stage1.isRunnable should be(false)
      stage1.isFlow should be(true)
      stage1.isSink should be(false)
      stage1.isSource should be(false)

      val stage2 = pipe()
      val flow12 = stage1.composeConnect(stage1.outPorts.head, stage2, stage2.inPorts.head)

      flow12.inPorts should be(stage1.inPorts)
      flow12.outPorts should be(stage2.outPorts)
      flow12.isRunnable should be(false)
      flow12.isFlow should be(true)
      flow12.isSink should be(false)
      flow12.isSource should be(false)

      val source0 = source()
      source0.inPorts.size should be(0)
      source0.outPorts.size should be(1)
      source0.isRunnable should be(false)
      source0.isFlow should be(false)
      source0.isSink should be(false)
      source0.isSource should be(true)

      val sink3 = sink()
      sink3.inPorts.size should be(1)
      sink3.outPorts.size should be(0)
      sink3.isRunnable should be(false)
      sink3.isFlow should be(false)
      sink3.isSink should be(true)
      sink3.isSource should be(false)

      val source012 = source0.composeConnect(source0.outPorts.head, flow12, flow12.inPorts.head)
      source012.inPorts.size should be(0)
      source012.outPorts should be(flow12.outPorts)
      source012.isRunnable should be(false)
      source012.isFlow should be(false)
      source012.isSink should be(false)
      source012.isSource should be(true)

      val sink123 = flow12.composeConnect(flow12.outPorts.head, sink3, sink3.inPorts.head)
      sink123.inPorts should be(flow12.inPorts)
      sink123.outPorts.size should be(0)
      sink123.isRunnable should be(false)
      sink123.isFlow should be(false)
      sink123.isSink should be(true)
      sink123.isSource should be(false)

      val runnable0123a = source0.composeConnect(source0.outPorts.head, sink123, sink123.inPorts.head)
      val runnable0123b = source012.composeConnect(source012.outPorts.head, sink3, sink3.inPorts.head)
      val runnable0123c =
        source0
          .composeConnect(source0.outPorts.head, flow12, flow12.inPorts.head)
          .composeConnect(flow12.outPorts.head, sink3, sink3.inPorts.head)

      runnable0123a should be(runnable0123b)
      runnable0123a should be(runnable0123c)

      runnable0123a.inPorts.size should be(0)
      runnable0123a.outPorts.size should be(0)
      runnable0123a.isRunnable should be(true)
      runnable0123a.isFlow should be(false)
      runnable0123a.isSink should be(false)
      runnable0123a.isSource should be(false)
    }

    "be able to model hierarchic attributes" in {

    }

  }

}
