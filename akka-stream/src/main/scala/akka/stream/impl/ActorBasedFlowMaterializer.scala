/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import java.util.concurrent.atomic.AtomicLong

import akka.dispatch.Dispatchers
import akka.event.Logging
import akka.stream.impl.Stages.StageModule
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.fusing.ActorInterpreter
import akka.stream.scaladsl.OperationAttributes._
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ Promise, ExecutionContext, Await, Future }
import akka.actor._
import akka.stream.{ FlowMaterializer, MaterializerSettings, OverflowStrategy, TimerTransformer }
import akka.stream.MaterializationException
import akka.stream.actor.ActorSubscriber
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.pattern.ask
import org.reactivestreams.{ Processor, Publisher, Subscriber }

/**
 * INTERNAL API
 */
final object Optimizations {
  val none: Optimizations = Optimizations(collapsing = false, elision = false, simplification = false, fusion = false)
  val all: Optimizations = Optimizations(collapsing = true, elision = true, simplification = true, fusion = true)
}
/**
 * INTERNAL API
 */
final case class Optimizations(collapsing: Boolean, elision: Boolean, simplification: Boolean, fusion: Boolean) {
  def isEnabled: Boolean = collapsing || elision || simplification || fusion
}

/**
 * INTERNAL API
 */
case class ActorBasedFlowMaterializer(override val settings: MaterializerSettings,
                                      dispatchers: Dispatchers, // FIXME is this the right choice for loading an EC?
                                      supervisor: ActorRef,
                                      flowNameCounter: AtomicLong,
                                      namePrefix: String,
                                      optimizations: Optimizations)
  extends FlowMaterializer(settings) {
  import Stages._

  def withNamePrefix(name: String): FlowMaterializer = this.copy(namePrefix = name)

  private[this] def nextFlowNameCount(): Long = flowNameCounter.incrementAndGet()

  private[this] def createFlowName(): String = s"$namePrefix-${nextFlowNameCount()}"

  //  //FIXME Optimize the implementation of the optimizer (no joke)
  //  // AstNodes are in reverse order, Fusable Ops are in order
  //  private[this] final def optimize[Mat](ops: List[StageModule], mmFuture: Future[Mat]): (List[StageModule], Int) = {
  //    @tailrec def analyze(rest: List[Ast.AstNode], optimized: List[Ast.AstNode], fuseCandidates: List[Stage[_, _]]): (List[Ast.AstNode], Int) = {
  //
  //      //The `verify` phase
  //      def verify(rest: List[Ast.AstNode], orig: List[Ast.AstNode]): List[Ast.AstNode] =
  //        rest match {
  //          case (f: Ast.Fused) :: _ ⇒ throw new IllegalStateException("Fused AST nodes not allowed to be present in the input to the optimizer: " + f)
  //          //TODO Ast.Take(-Long.MaxValue..0) == stream doesn't do anything. Perhaps output warning for that?
  //          case noMatch             ⇒ noMatch
  //        }
  //
  //      // The `elide` phase
  //      // TODO / FIXME : This phase could be pulled out to be executed incrementally when building the Ast
  //      def elide(rest: List[Ast.AstNode], orig: List[Ast.AstNode]): List[Ast.AstNode] =
  //        rest match {
  //          case noMatch if !optimizations.elision || (noMatch ne orig) ⇒ orig
  //          //Collapses consecutive Take's into one
  //          case (t1: Ast.Take) :: (t2: Ast.Take) :: rest ⇒ (if (t1.n < t2.n) t1 else t2) :: rest
  //
  //          //Collapses consecutive Drop's into one
  //          case (d1: Ast.Drop) :: (d2: Ast.Drop) :: rest ⇒ new Ast.Drop(d1.n + d2.n, d1.attributes and d2.attributes) :: rest
  //
  //          case Ast.Drop(n, _) :: rest if n < 1 ⇒ rest // a 0 or negative drop is a NoOp
  //
  //          case noMatch ⇒ noMatch
  //        }
  //      // The `simplify` phase
  //      def simplify(rest: List[Ast.AstNode], orig: List[Ast.AstNode]): List[Ast.AstNode] =
  //        rest match {
  //          case noMatch if !optimizations.simplification || (noMatch ne orig) ⇒ orig
  //
  //          // Two consecutive maps is equivalent to one pipelined map
  //          case Ast.Map(second, secondAttributes) :: Ast.Map(first, firstAttributes) :: rest ⇒
  //            Ast.Map(first andThen second, firstAttributes and secondAttributes) :: rest
  //
  //          case noMatch ⇒ noMatch
  //        }
  //
  //      // the `Collapse` phase
  //      def collapse(rest: List[Ast.AstNode], orig: List[Ast.AstNode]): List[Ast.AstNode] =
  //        rest match {
  //          case noMatch if !optimizations.collapsing || (noMatch ne orig) ⇒ orig
  //
  //          // Collapses a filter and a map into a collect
  //          case Ast.Map(mapFn, mapAttributes) :: Ast.Filter(filFn, filAttributes) :: rest ⇒
  //            Ast.Collect({ case i if filFn(i) ⇒ mapFn(i) }, filAttributes and mapAttributes) :: rest
  //
  //          case noMatch ⇒ noMatch
  //        }
  //
  //      // Tries to squeeze AstNode into a single fused pipeline
  //      def ast2op(head: Ast.AstNode, prev: List[Stage[_, _]]): List[Stage[_, _]] =
  //        head match {
  //          // Always-on below
  //          case Ast.StageFactory(mkStage, _)     ⇒ mkStage() :: prev
  //
  //          // Optimizations below
  //          case noMatch if !optimizations.fusion ⇒ prev
  //
  //          case Ast.Map(f, _)                    ⇒ fusing.Map(f) :: prev
  //          case Ast.Filter(p, _)                 ⇒ fusing.Filter(p) :: prev
  //          case Ast.Drop(n, _)                   ⇒ fusing.Drop(n) :: prev
  //          case Ast.Take(n, _)                   ⇒ fusing.Take(n) :: prev
  //          case Ast.Collect(pf, _)               ⇒ fusing.Collect(pf) :: prev
  //          case Ast.Scan(z, f, _)                ⇒ fusing.Scan(z, f) :: prev
  //          case Ast.Expand(s, f, _)              ⇒ fusing.Expand(s, f) :: prev
  //          case Ast.Conflate(s, f, _)            ⇒ fusing.Conflate(s, f) :: prev
  //          case Ast.Buffer(n, s, _)              ⇒ fusing.Buffer(n, s) :: prev
  //          case Ast.MapConcat(f, _)              ⇒ fusing.MapConcat(f) :: prev
  //          case Ast.Grouped(n, _)                ⇒ fusing.Grouped(n) :: prev
  //          //FIXME Add more fusion goodies here
  //          case _                                ⇒ prev
  //        }
  //
  //      // First verify, then try to elide, then try to simplify, then try to fuse
  //      collapse(rest, simplify(rest, elide(rest, verify(rest, rest)))) match {
  //
  //        case Nil ⇒
  //          if (fuseCandidates.isEmpty) (optimized.reverse, optimized.length) // End of optimization run without fusion going on, wrap up
  //          else ((Ast.Fused(fuseCandidates) :: optimized).reverse, optimized.length + 1) // End of optimization run with fusion going on, so add it to the optimized stack
  //
  //        // If the Ast was changed this pass simply recur
  //        case modified if modified ne rest ⇒ analyze(modified, optimized, fuseCandidates)
  //
  //        // No changes to the Ast, lets try to see if we can squeeze the current head Ast node into a fusion pipeline
  //        case head :: rest ⇒
  //          ast2op(head, fuseCandidates) match {
  //            case Nil               ⇒ analyze(rest, head :: optimized, Nil)
  //            case `fuseCandidates`  ⇒ analyze(rest, head :: Ast.Fused(fuseCandidates) :: optimized, Nil)
  //            case newFuseCandidates ⇒ analyze(rest, optimized, newFuseCandidates)
  //          }
  //      }
  //    }
  //    val result = analyze(ops, Nil, Nil)
  //    result
  //  }

  override def materialize[Mat](runnableFlow: RunnableFlow[Mat]): Mat = {
    val session = new MaterializerSession(runnableFlow.module) {
      override protected def materializeAtomic(atomic: Module): Any = atomic match {
        case sink: SinkModule[_, _] ⇒
          val (sub, mat) = sink.create(ActorBasedFlowMaterializer.this, createFlowName())
          assignPort(sink.inPort, sub.asInstanceOf[Subscriber[Any]])
          mat
        case source: SourceModule[_, _] ⇒
          val (pub, mat) = source.create(ActorBasedFlowMaterializer.this, createFlowName())
          assignPort(source.outPort, pub.asInstanceOf[Publisher[Any]])
          mat

        case matStage @ MaterializingStageFactory(mkStageAndMat, _) ⇒
          val (stage, mat) = mkStageAndMat()
          val props = ActorInterpreter.props(settings, List(stage))
          val processor = ActorProcessorFactory[Any, Any](actorOf(props, createFlowName()))
          assignPort(matStage.inPort, processor)
          assignPort(matStage.outPort, processor)
          mat
        case stage: StageModule ⇒
          val props = ActorProcessorFactory.props(ActorBasedFlowMaterializer.this, stage)
          val processor = ActorProcessorFactory[Any, Any](actorOf(props, createFlowName()))
          assignPort(stage.inPort, processor)
          assignPort(stage.outPort, processor)
          ()
      }

    }

    session.materialize().asInstanceOf[Mat]
  }

  //  // Ops come in reverse order
  //  override def materialize[In, Out](source: Source[In], sink: Sink[Out], rawOps: List[Ast.AstNode], keys: List[Key[_]]): MaterializedMap = {
  //    val flowName = createFlowName() //FIXME: Creates Id even when it is not used in all branches below
  //
  //    def throwUnknownType(typeName: String, s: AnyRef): Nothing =
  //      throw new MaterializationException(s"unknown $typeName type ${s.getClass}")
  //
  //    def attachSink(pub: Publisher[Out], flowName: String) = sink match {
  //      case s: ActorFlowSink[Out] ⇒ s.attach(pub, this, flowName)
  //      case s                     ⇒ throwUnknownType("Sink", s)
  //    }
  //    def attachSource(sub: Subscriber[In], flowName: String) = source match {
  //      case s: ActorFlowSource[In] ⇒ s.attach(sub, this, flowName)
  //      case s                      ⇒ throwUnknownType("Source", s)
  //    }
  //    def createSink(flowName: String) = sink match {
  //      case s: ActorFlowSink[In] ⇒ s.create(this, flowName)
  //      case s                    ⇒ throwUnknownType("Sink", s)
  //    }
  //    def createSource(flowName: String) = source match {
  //      case s: ActorFlowSource[Out] ⇒ s.create(this, flowName)
  //      case s                       ⇒ throwUnknownType("Source", s)
  //    }
  //    def isActive(s: AnyRef) = s match {
  //      case s: ActorFlowSource[_] ⇒ s.isActive
  //      case s: ActorFlowSink[_]   ⇒ s.isActive
  //      case s: Source[_]          ⇒ throwUnknownType("Source", s)
  //      case s: Sink[_]            ⇒ throwUnknownType("Sink", s)
  //    }
  //    def addIfKeyed(m: Materializable, v: Any, map: MaterializedMap) = m match {
  //      case km: KeyedMaterializable[_] ⇒ map.updated(km, v)
  //      case _                          ⇒ map
  //    }
  //
  //    val mmPromise = Promise[MaterializedMap]
  //    val mmFuture = mmPromise.future
  //
  //    val (sourceValue, sinkValue, pipeMap) =
  //      if (rawOps.isEmpty) {
  //        if (isActive(sink)) {
  //          val (sub, value) = createSink(flowName)
  //          (attachSource(sub, flowName), value, MaterializedMap.empty)
  //        } else if (isActive(source)) {
  //          val (pub, value) = createSource(flowName)
  //          (value, attachSink(pub, flowName), MaterializedMap.empty)
  //        } else {
  //          val (id, empty) = processorForNode[In, Out](identityStageNode, flowName, 1)
  //          (attachSource(id, flowName), attachSink(id, flowName), empty)
  //        }
  //      } else {
  //        val (ops, opsSize) = if (optimizations.isEnabled) optimize(rawOps, mmFuture) else (rawOps, rawOps.length)
  //        val (last, lastMap) = processorForNode[Any, Out](ops.head, flowName, opsSize)
  //        val (first, map) = processorChain(last, ops.tail, flowName, opsSize - 1, lastMap)
  //        (attachSource(first.asInstanceOf[Processor[In, Any]], flowName), attachSink(last, flowName), map)
  //      }
  //    val sourceSinkMap = addIfKeyed(sink, sinkValue, addIfKeyed(source, sourceValue, pipeMap))
  //
  //    if (keys.isEmpty) sourceSinkMap
  //    else (sourceSinkMap /: keys) {
  //      case (mm, k) ⇒ mm.updated(k, k.materialize(mm))
  //    }
  //  }
  //  //FIXME Should this be a dedicated AstNode?
  //  private[this] val identityStageNode = Ast.StageFactory(() ⇒ FlowOps.identityStage[Any], Ast.Defaults.identityOp)

  def executionContext: ExecutionContext = dispatchers.lookup(settings.dispatcher match {
    case Deploy.NoDispatcherGiven ⇒ Dispatchers.DefaultDispatcherId
    case other                    ⇒ other
  })

  //  /**
  //   * INTERNAL API
  //   */
  //  private[akka] def processorForNode[In, Out](op: StageModule, flowName: String, n: Int): (Processor[In, Out], MaterializedMap) = op match {
  //    // FIXME #16376 should probably be replaced with an ActorFlowProcessor similar to ActorFlowSource/Sink
  //    case Ast.DirectProcessor(p, _) ⇒ (p().asInstanceOf[Processor[In, Out]], MaterializedMap.empty)
  //    case Ast.DirectProcessorWithKey(p, key, _) ⇒
  //      val (processor, value) = p()
  //      (processor.asInstanceOf[Processor[In, Out]], MaterializedMap.empty.updated(key, value))
  //    case _ ⇒
  //      (ActorProcessorFactory[In, Out](actorOf(ActorProcessorFactory.props(this, op), s"$flowName-$n-${op.attributes.name}", op)), MaterializedMap.empty)
  //  }

  private[akka] def actorOf(props: Props, name: String): ActorRef =
    actorOf(props, name, settings.dispatcher)

  private[akka] def actorOf(props: Props, name: String, stage: StageModule): ActorRef =
    actorOf(props, name, stage.attributes.settings(settings).dispatcher)

  private[akka] def actorOf(props: Props, name: String, dispatcher: String): ActorRef = supervisor match {
    case ref: LocalActorRef ⇒
      ref.underlying.attachChild(props.withDispatcher(dispatcher), name, systemService = false)
    case ref: RepointableActorRef ⇒
      if (ref.isStarted)
        ref.underlying.asInstanceOf[ActorCell].attachChild(props.withDispatcher(dispatcher), name, systemService = false)
      else {
        implicit val timeout = ref.system.settings.CreationTimeout
        val f = (supervisor ? StreamSupervisor.Materialize(props.withDispatcher(dispatcher), name)).mapTo[ActorRef]
        Await.result(f, timeout.duration)
      }
    case unknown ⇒
      throw new IllegalStateException(s"Stream supervisor must be a local actor, was [${unknown.getClass.getName}]")
  }

}

/**
 * INTERNAL API
 */
private[akka] object FlowNameCounter extends ExtensionId[FlowNameCounter] with ExtensionIdProvider {
  override def get(system: ActorSystem): FlowNameCounter = super.get(system)
  override def lookup = FlowNameCounter
  override def createExtension(system: ExtendedActorSystem): FlowNameCounter = new FlowNameCounter
}

/**
 * INTERNAL API
 */
private[akka] class FlowNameCounter extends Extension {
  val counter = new AtomicLong(0)
}

/**
 * INTERNAL API
 */
private[akka] object StreamSupervisor {
  def props(settings: MaterializerSettings): Props = Props(new StreamSupervisor(settings))

  final case class Materialize(props: Props, name: String) extends DeadLetterSuppression
}

private[akka] class StreamSupervisor(settings: MaterializerSettings) extends Actor {
  import StreamSupervisor._

  override def supervisorStrategy = SupervisorStrategy.stoppingStrategy

  def receive = {
    case Materialize(props, name) ⇒
      val impl = context.actorOf(props, name)
      sender() ! impl
  }
}

/**
 * INTERNAL API
 */
private[akka] object ActorProcessorFactory {
  import Stages._

  def props(materializer: FlowMaterializer, op: StageModule): Props = {
    val settings = materializer.settings // USE THIS TO AVOID CLOSING OVER THE MATERIALIZER BELOW
    op match {
      case Fused(ops, _)              ⇒ ActorInterpreter.props(settings, ops)
      case Map(f, _)                  ⇒ ActorInterpreter.props(settings, List(fusing.Map(f)))
      case Filter(p, _)               ⇒ ActorInterpreter.props(settings, List(fusing.Filter(p)))
      case Drop(n, _)                 ⇒ ActorInterpreter.props(settings, List(fusing.Drop(n)))
      case Take(n, _)                 ⇒ ActorInterpreter.props(settings, List(fusing.Take(n)))
      case Collect(pf, _)             ⇒ ActorInterpreter.props(settings, List(fusing.Collect(pf)))
      case Scan(z, f, _)              ⇒ ActorInterpreter.props(settings, List(fusing.Scan(z, f)))
      case Expand(s, f, _)            ⇒ ActorInterpreter.props(settings, List(fusing.Expand(s, f)))
      case Conflate(s, f, _)          ⇒ ActorInterpreter.props(settings, List(fusing.Conflate(s, f)))
      case Buffer(n, s, _)            ⇒ ActorInterpreter.props(settings, List(fusing.Buffer(n, s)))
      case MapConcat(f, _)            ⇒ ActorInterpreter.props(settings, List(fusing.MapConcat(f)))
      case MapAsync(f, _)             ⇒ MapAsyncProcessorImpl.props(settings, f)
      case MapAsyncUnordered(f, _)    ⇒ MapAsyncUnorderedProcessorImpl.props(settings, f)
      case Grouped(n, _)              ⇒ ActorInterpreter.props(settings, List(fusing.Grouped(n)))
      case GroupBy(f, _)              ⇒ GroupByProcessorImpl.props(settings, f)
      case PrefixAndTail(n, _)        ⇒ PrefixAndTailImpl.props(settings, n)
      case SplitWhen(p, _)            ⇒ SplitWhenProcessorImpl.props(settings, p)
      case ConcatAll(_)               ⇒ ConcatAllImpl.props(materializer) //FIXME closes over the materializer, is this good?
      case StageFactory(mkStage, _)   ⇒ ActorInterpreter.props(settings, List(mkStage()))
      case TimerTransform(mkStage, _) ⇒ TimerTransformerProcessorsImpl.props(settings, mkStage())
    }
  }

  def apply[I, O](impl: ActorRef): ActorProcessor[I, O] = {
    val p = new ActorProcessor[I, O](impl)
    impl ! ExposedPublisher(p.asInstanceOf[ActorPublisher[Any]])
    p
  }
}
