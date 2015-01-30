/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.event.Logging
import akka.stream.{ javadsl, OverflowStrategy, TimerTransformer }
import akka.stream.impl.Ast.Fused
import akka.stream.scaladsl.{ Key, OperationAttributes }
import akka.stream.scaladsl.OperationAttributes._
import akka.stream.stage.Stage
import org.reactivestreams.Processor
import StreamLayout._

import scala.collection.immutable
import scala.concurrent.Future

private[akka] trait StreamProcessingLayout {
  def representation: Layout
}

private[akka] trait StreamProcessingModule extends StreamProcessingLayout {
  abstract override def representation: Module
}

private[akka] object AtomicModules {

  // FIXME Fix the name `Defaults` is waaaay too opaque. How about "Names"?
  object Defaults {
    val timerTransform = name("timerTransform")
    val stageFactory = name("stageFactory")
    val fused = name("fused")
    val map = name("map")
    val filter = name("filter")
    val collect = name("collect")
    val mapAsync = name("mapAsync")
    val mapAsyncUnordered = name("mapAsyncUnordered")
    val grouped = name("grouped")
    val take = name("take")
    val drop = name("drop")
    val scan = name("scan")
    val buffer = name("buffer")
    val conflate = name("conflate")
    val expand = name("expand")
    val mapConcat = name("mapConcat")
    val groupBy = name("groupBy")
    val prefixAndTail = name("prefixAndTail")
    val splitWhen = name("splitWhen")
    val concatAll = name("concatAll")
    val processor = name("processor")
    val processorWithKey = name("processorWithKey")
    val identityOp = name("identityOp")

    val merge = name("merge")
    val mergePreferred = name("mergePreferred")
    val broadcast = name("broadcast")
    val balance = name("balance")
    val zip = name("zip")
    val unzip = name("unzip")
    val concat = name("concat")
    val flexiMerge = name("flexiMerge")
    val flexiRoute = name("flexiRoute")
    val identityJunction = name("identityJunction")
  }

  import Defaults._

  final case class TimerTransform(mkStage: () ⇒ TimerTransformer[Any, Any], attributes: OperationAttributes = timerTransform) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class StageFactory(mkStage: () ⇒ Stage[_, _], attributes: OperationAttributes = stageFactory) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  object Fused {
    def apply(ops: immutable.Seq[Stage[_, _]]): Fused =
      Fused(ops, name(ops.map(x ⇒ Logging.simpleName(x).toLowerCase).mkString("+"))) //FIXME change to something more performant for name
  }
  final case class Fused(ops: immutable.Seq[Stage[_, _]], attributes: OperationAttributes = fused) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class Map(f: Any ⇒ Any, attributes: OperationAttributes = map) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class Filter(p: Any ⇒ Boolean, attributes: OperationAttributes = filter) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class Collect(pf: PartialFunction[Any, Any], attributes: OperationAttributes = collect) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  // FIXME Replace with OperateAsync
  final case class MapAsync(f: Any ⇒ Future[Any], attributes: OperationAttributes = mapAsync) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  //FIXME Should be OperateUnorderedAsync
  final case class MapAsyncUnordered(f: Any ⇒ Future[Any], attributes: OperationAttributes = mapAsyncUnordered) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class Grouped(n: Int, attributes: OperationAttributes = grouped) extends LinearModule {
    require(n > 0, "n must be greater than 0")

    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  //FIXME should be `n: Long`
  final case class Take(n: Int, attributes: OperationAttributes = take) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  //FIXME should be `n: Long`
  final case class Drop(n: Int, attributes: OperationAttributes = drop) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class Scan(zero: Any, f: (Any, Any) ⇒ Any, attributes: OperationAttributes = scan) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class Buffer(size: Int, overflowStrategy: OverflowStrategy, attributes: OperationAttributes = buffer) extends LinearModule {
    require(size > 0, s"Buffer size must be larger than zero but was [$size]")

    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }
  final case class Conflate(seed: Any ⇒ Any, aggregate: (Any, Any) ⇒ Any, attributes: OperationAttributes = conflate) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }
  final case class Expand(seed: Any ⇒ Any, extrapolate: Any ⇒ (Any, Any), attributes: OperationAttributes = expand) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }
  final case class MapConcat(f: Any ⇒ immutable.Seq[Any], attributes: OperationAttributes = mapConcat) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class GroupBy(f: Any ⇒ Any, attributes: OperationAttributes = groupBy) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class PrefixAndTail(n: Int, attributes: OperationAttributes = prefixAndTail) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class SplitWhen(p: Any ⇒ Boolean, attributes: OperationAttributes = splitWhen) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class ConcatAll(attributes: OperationAttributes = concatAll) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class DirectProcessor(p: () ⇒ Processor[Any, Any], attributes: OperationAttributes = processor) extends LinearModule {
    override def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  final case class DirectProcessorWithKey(p: () ⇒ (Processor[Any, Any], Any), key: Key[_], attributes: OperationAttributes = processorWithKey) extends LinearModule {
    def withAttributes(attributes: OperationAttributes) = copy(attributes = attributes)
  }

  sealed trait JunctionAstNode {
    def attributes: OperationAttributes
  }

  // FIXME: Try to eliminate these
  sealed trait FanInAstNode extends JunctionAstNode
  sealed trait FanOutAstNode extends JunctionAstNode

  /**
   * INTERNAL API
   * `f` MUST be implemented as value of type `scala.FunctionN`
   */
  sealed trait ZipWith extends FanInAstNode {
    /** MUST be implemented as type of FunctionN */
    def f: Any
  }
  final case class Zip2With[T1, T2](f: Function2[T1, T2, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip3With[T1, T2, T3](f: Function3[T1, T2, T3, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip4With[T1, T2, T3, T4](f: Function4[T1, T2, T3, T4, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip5With[T1, T2, T3, T4, T5](f: Function5[T1, T2, T3, T4, T5, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip6With[T1, T2, T3, T4, T5, T6](f: Function6[T1, T2, T3, T4, T5, T6, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip7With[T1, T2, T3, T4, T5, T6, T7](f: Function7[T1, T2, T3, T4, T5, T6, T7, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip8With[T1, T2, T3, T4, T5, T6, T7, T8](f: Function8[T1, T2, T3, T4, T5, T6, T7, T8, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip9With[T1, T2, T3, T4, T5, T6, T7, T8, T9](f: Function9[T1, T2, T3, T4, T5, T6, T7, T8, T9, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip10With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](f: Function10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip11With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11](f: Function11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip12With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12](f: Function12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip13With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13](f: Function13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip14With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14](f: Function14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip15With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15](f: Function15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip16With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16](f: Function16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip17With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17](f: Function17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip18With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18](f: Function18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip19With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19](f: Function19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip20With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20](f: Function20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip21With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21](f: Function21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, Any], attributes: OperationAttributes) extends ZipWith
  final case class Zip22With[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22](f: Function22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, Any], attributes: OperationAttributes) extends ZipWith

  // FIXME Why do we need this?
  final case class IdentityAstNode(attributes: OperationAttributes) extends JunctionAstNode

  final case class Merge(attributes: OperationAttributes) extends FanInAstNode
  final case class MergePreferred(attributes: OperationAttributes) extends FanInAstNode

  final case class Broadcast(attributes: OperationAttributes) extends FanOutAstNode
  final case class Balance(waitForAllDownstreams: Boolean, attributes: OperationAttributes) extends FanOutAstNode

  final case class Unzip(attributes: OperationAttributes) extends FanOutAstNode

  final case class Concat(attributes: OperationAttributes) extends FanInAstNode

  final case class FlexiMergeNode(factory: FlexiMergeImpl.MergeLogicFactory[Any], attributes: OperationAttributes) extends FanInAstNode
  final case class FlexiRouteNode(factory: FlexiRouteImpl.RouteLogicFactory[Any], attributes: OperationAttributes) extends FanOutAstNode

}
