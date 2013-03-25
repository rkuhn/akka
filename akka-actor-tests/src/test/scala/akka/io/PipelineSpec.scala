/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.io

import akka.testkit.AkkaSpec
import akka.util.ByteString
import scala.annotation.tailrec
import java.nio.ByteOrder
import scala.concurrent.forkjoin.ThreadLocalRandom

class PipelineSpec extends AkkaSpec {

  trait Level1
  trait Level2
  trait Level3
  trait Level4

  trait LevelFactory[Lvl] {
    def msgA: Lvl
    def msgB: Lvl
  }

  implicit object Level1 extends LevelFactory[Level1] {
    object msgA extends Level1 { override def toString = "Lvl1msgA" }
    object msgB extends Level1 { override def toString = "Lvl1msgB" }
  }

  implicit object Level2 extends LevelFactory[Level2] {
    object msgA extends Level2 { override def toString = "Lvl2msgA" }
    object msgB extends Level2 { override def toString = "Lvl2msgB" }
  }

  implicit object Level3 extends LevelFactory[Level3] {
    object msgA extends Level3 { override def toString = "Lvl3msgA" }
    object msgB extends Level3 { override def toString = "Lvl3msgB" }
  }

  implicit object Level4 extends LevelFactory[Level4] {
    object msgA extends Level4 { override def toString = "Lvl4msgA" }
    object msgB extends Level4 { override def toString = "Lvl4msgB" }
  }

  "A Pipeline" must {

    "be correctly evaluated if single stage" in {
      val (cmd, evt) = PipelineFactory.buildFunctionPair(null, stage[Level1, Level2](1, 0, false))
      cmd(Level2.msgA) must be(Seq(Level1.msgA) -> Nil)
      evt(Level1.msgA) must be(Nil -> Seq(Level2.msgA))
      cmd(Level2.msgB) must be(Seq(Level1.msgB) -> Nil)
      evt(Level1.msgB) must be(Nil -> Seq(Level2.msgB))
    }

    "be correctly evaluated when two combined" in {
      val stage1 = stage[Level1, Level2](1, 0, false)
      val stage2 = stage[Level2, Level3](1, 0, false)
      val (cmd, evt) = PipelineFactory.buildFunctionPair(null, stage1 >> stage2)
      cmd(Level3.msgA) must be(Seq(Left(Level1.msgA)))
      evt(Level1.msgA) must be(Seq(Right(Level3.msgA)))
      cmd(Level3.msgB) must be(Seq(Left(Level1.msgB)))
      evt(Level1.msgB) must be(Seq(Right(Level3.msgB)))
    }

    "be correctly evaluated when three combined" in {
      val stage1 = stage[Level1, Level2](1, 0, false)
      val stage2 = stage[Level2, Level3](2, 0, false)
      val stage3 = stage[Level3, Level4](1, 0, false)
      val ts = stage1 >> stage2 >> stage3 apply null
      ts.commandPipeline(Level4.msgA) must be(Seq(Left(Level1.msgA), Left(Level1.msgA)))
      ts.eventPipeline(Level1.msgA) must be(Seq(Right(Level4.msgA), Right(Level4.msgA)))
      ts.commandPipeline(Level4.msgB) must be(Seq(Left(Level1.msgB), Left(Level1.msgB)))
      ts.eventPipeline(Level1.msgB) must be(Seq(Right(Level4.msgB), Right(Level4.msgB)))
    }

    "be correctly evaluated with back-scatter" in {
      val stage1 = stage[Level1, Level2](1, 0, true)
      val stage2 = stage[Level2, Level3](1, 1, true)
      val stage3 = stage[Level3, Level4](1, 0, false)
      val ts = stage1 >> stage2 >> stage3 apply null
      ts.commandPipeline(Level4.msgA) must be(Seq(Left(Level1.msgA), Right(Level4.msgB)))
      ts.eventPipeline(Level1.msgA) must be(Seq(Right(Level4.msgA), Left(Level1.msgB)))
    }

  }

  def stage[Below: LevelFactory, Above: LevelFactory](forward: Int, backward: Int, invert: Boolean) =
    new SymmetricPipelineStage[AnyRef, Below, Above] {
      override def apply(ctx: AnyRef) = {
        val below = implicitly[LevelFactory[Below]]
        val above = implicitly[LevelFactory[Above]]
        PipePairFactory(
          { a ⇒
            val msgA = a == above.msgA
            val msgAbove = if (invert ^ msgA) above.msgA else above.msgB
            val msgBelow = if (invert ^ msgA) below.msgA else below.msgB
            (for (_ ← 1 to forward) yield Left(msgBelow)) ++ (for (_ ← 1 to backward) yield Right(msgAbove))
          },
          { b ⇒
            val msgA = b == below.msgA
            val msgAbove = if (invert ^ msgA) above.msgA else above.msgB
            val msgBelow = if (invert ^ msgA) below.msgA else below.msgB
            (for (_ ← 1 to forward) yield Right(msgAbove)) ++ (for (_ ← 1 to backward) yield Left(msgBelow))
          })
      }
    }

}

object PipelineBench extends App {

  val frame = new SymmetricPipelineStage[AnyRef, ByteString, ByteString] {
    override def apply(ctx: AnyRef) =
      new SymmetricPipePair[ByteString, ByteString] {
        var buffer = None: Option[ByteString]
        implicit val byteOrder = ByteOrder.BIG_ENDIAN

        @tailrec def extractFrames(bs: ByteString, acc: List[Right[ByteString, ByteString]]): (Option[ByteString], Seq[Right[ByteString, ByteString]]) = {
          if (bs.isEmpty) {
            (None, acc.reverse)
          } else if (bs.length < 4) {
            (Some(bs.compact), acc.reverse)
          } else {
            val length = bs.iterator.getInt
            if (bs.length >= length) {
              extractFrames(bs drop length, Right(bs.slice(4, length)) :: acc)
            } else {
              (Some(bs.compact), acc.reverse)
            }
          }
        }

        override def commandPipeline =
          { bs ⇒
            val bb = java.nio.ByteBuffer.allocate(4)
            bb.order(byteOrder)
            bb.putInt(bs.length + 4).flip
            Seq(Left(ByteString(bb) ++ bs))
          }
        override def eventPipeline =
          { bs ⇒
            val data = if (buffer.isEmpty) bs else buffer.get ++ bs
            extractFrames(data, Nil) match {
              case (nb, result) ⇒ buffer = nb; result
            }
          }
      }
  }

  val pipe = frame >> frame >> frame >> frame apply null

  val bytes = pipe.commandPipeline(ByteString("hello")).head.fold(identity, identity).compact
  println(bytes)
  println(pipe.eventPipeline(bytes))

  class Bytes {
    var pos = 0
    var emitted = 0
    def get(): ByteString = {
      val r = ThreadLocalRandom.current()
      val l = r.nextInt(2 * bytes.length)
      @tailrec def rec(left: Int, acc: ByteString): ByteString = {
        if (pos + left <= bytes.length) {
          val result = acc ++ bytes.slice(pos, pos + left)
          pos = (pos + left) % bytes.length
          result
        } else {
          val oldpos = pos
          pos = 0
          rec(left - bytes.length + oldpos, acc ++ bytes.slice(oldpos, bytes.length))
        }
      }
      emitted += l
      rec(l, ByteString.empty)
    }
  }

  val b = new Bytes
  val y = for (_ ← 1 to 1000000; x ← pipe.eventPipeline(b.get())) yield x
  assert(y forall { case Right(b) ⇒ b == ByteString("hello"); case _ ⇒ false })
  assert(y.size == b.emitted / bytes.length)

  val N = 1000000
  val start = System.nanoTime
  for (_ ← 1 to N; x ← pipe.eventPipeline(b.get())) yield x
  val time = System.nanoTime - start
  println(s"1 iteration took ${time / N}ns")
}
