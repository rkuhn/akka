package spray.io

import akka.util.ByteString
import java.nio.ByteOrder
import scala.annotation.tailrec
import akka.io.Tcp
import scala.concurrent.forkjoin.ThreadLocalRandom

object SprayPipeline extends App {
  val frame = new RawPipelineStage[PipelineContext] {
    override def apply(ctx: PipelineContext, commandPL: CPL, eventPL: EPL) =
      new Pipelines {
        var buffer = None: Option[ByteString]
        implicit val byteOrder = ByteOrder.BIG_ENDIAN

        @tailrec def extractFrames(bs: ByteString): Unit = {
          if (bs.isEmpty) {
            buffer = None
          } else if (bs.length < 4) {
            buffer = Some(bs.compact)
          } else {
            val length = bs.iterator.getInt
            if (bs.length >= length) {
              eventPL(Tcp.Received(bs.slice(4, length)))
              extractFrames(bs drop length)
            } else {
              buffer = Some(bs.compact)
            }
          }
        }

        override def commandPipeline =
          {
            case Tcp.Write(bs, x) ⇒
              val bb = java.nio.ByteBuffer.allocate(4)
              bb.order(ByteOrder.BIG_ENDIAN)
              bb.putInt(bs.length + 4).flip
              commandPL(Tcp.Write(ByteString(bb) ++ bs, x))
          }
        override def eventPipeline =
          {
            case Tcp.Received(bs) ⇒
              val data = if (buffer.isEmpty) bs else buffer.get ++ bs
              extractFrames(data)
          }
      }
  }

  var output = Vector.newBuilder[ByteString]
  def getOut = {
    val res = output.result
    output = Vector.newBuilder[ByteString]
    res
  }

  val pipe = (frame >> frame >> frame >> frame)(null, { case Tcp.Write(bs, _) ⇒ output += bs }, { case Tcp.Received(bs) ⇒ output += bs })

  pipe.commandPipeline(Tcp.Write(ByteString("hello")))
  val bytes = getOut.head.compact
  println(bytes)
  pipe.eventPipeline(Tcp.Received(bytes))
  println(getOut.head)

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
  val y = for (_ ← 1 to 1000000; x ← { pipe.eventPipeline(Tcp.Received(b.get())); getOut }) yield x
  assert(y forall { _ == ByteString("hello") })
  assert(y.size == b.emitted / bytes.length)

  val N = 1000000
  val start = System.nanoTime
  for (_ ← 1 to N; x ← { pipe.eventPipeline(Tcp.Received(b.get())); getOut }) yield x
  val time = System.nanoTime - start
  println(s"1 iteration took ${time / N}ns")
}