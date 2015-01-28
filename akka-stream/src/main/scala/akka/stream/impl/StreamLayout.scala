/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

/**
 * INTERNAL API
 */
private[akka] object StreamLayout {
  class InPort
  class OutPort

  // TODO: Module support
  // TODO: Attribute support
  // TODO: Copy support

  trait Stage {

    def inPorts: Set[InPort]
    def outPorts: Set[OutPort]

    def isRunnable: Boolean = inPorts.isEmpty && outPorts.isEmpty
    def isSink: Boolean = (inPorts.size == 1) && outPorts.isEmpty
    def isSource: Boolean = (outPorts.size == 1) && inPorts.isEmpty
    def isFlow: Boolean = (inPorts.size == 1) && (outPorts.size == 1)

    def traverseForMaterialization(): Unit = ???

    // TODO: hierarchic attributes
    def compose(that: Stage): Stage = {
      assert(that ne this)

      Composite(
        this.inPorts ++ that.inPorts,
        this.outPorts ++ that.outPorts,
        this.downstreams ++ that.downstreams,
        this.upstreams ++ that.upstreams,
        this.inToAtomic ++ that.inToAtomic,
        this.outToAtomic ++ that.outToAtomic)
    }

    def connect(from: OutPort, to: InPort): Stage = {
      assert(outPorts(from))
      assert(inPorts(to))

      Composite(
        inPorts - to,
        outPorts - from,
        downstreams + ((outToAtomic(from), from) -> (inToAtomic(to), to)),
        upstreams + ((inToAtomic(to), to) -> (outToAtomic(from), from)),
        inToAtomic,
        outToAtomic)
    }

    def composeConnect(from: OutPort, toStage: Stage, to: InPort): Stage = {
      assert(outPorts(from))
      assert(!inPorts(to))
      assert(toStage ne this)
      assert(toStage.inPorts(to))

      // TODO: Optimize
      compose(toStage).connect(from, to)
    }

    protected def inToAtomic: Map[InPort, Atomic]
    protected def outToAtomic: Map[OutPort, Atomic]

    protected def downstreams: Map[(Atomic, OutPort), (Atomic, InPort)]
    protected def upstreams: Map[(Atomic, InPort), (Atomic, OutPort)]

  }

  case class Atomic(inPorts: Set[InPort], outPorts: Set[OutPort]) extends Stage {
    protected override val downstreams = Map.empty[(Atomic, OutPort), (Atomic, InPort)]
    protected override val upstreams = Map.empty[(Atomic, InPort), (Atomic, OutPort)]
    protected override val inToAtomic = inPorts.map(_ -> this).toMap
    protected override val outToAtomic = outPorts.map(_ -> this).toMap

  }

  // TODO: Special case linear composites
  case class Composite(
    inPorts: Set[InPort],
    outPorts: Set[OutPort],
    downstreams: Map[(Atomic, OutPort), (Atomic, InPort)],
    upstreams: Map[(Atomic, InPort), (Atomic, OutPort)],
    inToAtomic: Map[InPort, Atomic],
    outToAtomic: Map[OutPort, Atomic]) extends Stage {
    // TODO: Materialization order
    // TODO: Cycles
  }

}

//  private def maintainTopologicOrder(v: Node, w: Node, g: Graph): Graph = {
//    val newVertex = java.util.Arrays.copyOf(vertex, vertex.length)
//    var newPosition = position
//
//    val forwardQueue = mutable.Queue(w)
//    val backwardQueue = mutable.Queue(v)
//
//    var i = newPosition(w)
//    var j = newPosition(v)
//    newVertex(i) = null
//    newVertex(j) = null
//
//    def topologicalSearch(): Unit = {
//
//      while (true) {
//        i = i + 1
//        while (i < j && forwardQueue.forall { u ⇒ !successors(u).contains(newVertex(i)) }) i = i + 1
//        if (i == j) return
//        else {
//          forwardQueue.enqueue(newVertex(i))
//          newVertex(i) = null
//        }
//        j = j - 1
//        while (i < j && backwardQueue.forall { u ⇒ !predecessors(u).contains(newVertex(j)) }) j = j - 1
//        if (i == j) return
//        else {
//          backwardQueue.enqueue(newVertex(j))
//          newVertex(j) = null
//        }
//      }
//    }
//
//    def reorder(): Unit = {
//      while (forwardQueue.nonEmpty) {
//        if (newVertex(i) != null && forwardQueue.exists { u ⇒ successors(u).contains(newVertex(i)) }) {
//          forwardQueue.enqueue(newVertex(i))
//          newVertex(i) = null
//        }
//        if (newVertex(i) == null) {
//          val x = forwardQueue.dequeue()
//          newVertex(i) = x
//          newPosition += x -> i
//        }
//        i = i + 1
//      }
//
//      while (backwardQueue.nonEmpty) {
//        j = j - 1
//        if (newVertex(j) != null && backwardQueue.exists { u ⇒ predecessors(u).contains(newVertex(i)) }) {
//          backwardQueue.enqueue(newVertex(i))
//          newVertex(j) = null
//        }
//        if (newVertex(j) == null) {
//          val x = backwardQueue.dequeue()
//          newVertex(j) = x
//          newPosition += x -> j
//        }
//      }
//    }
//
//    topologicalSearch()
//    reorder()
//    g.copy(vertex = newVertex, position = newPosition)
//  }

