/*
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.http.util

import java.io.InputStream

import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try
import akka.actor.Props
import akka.http.model.RequestEntity
import akka.stream.{ MaterializerSettings, FlowMaterializer, impl }
import akka.stream.impl.fusing.IteratorInterpreter
import akka.stream.scaladsl._
import akka.stream.scaladsl.OperationAttributes._
import akka.stream.stage._
import akka.util.ByteString
import org.reactivestreams.{ Subscriber, Publisher }

/**
 * INTERNAL API
 */
private[http] object StreamUtils {

  /**
   * Creates a transformer that will call `f` for each incoming ByteString and output its result. After the complete
   * input has been read it will call `finish` once to determine the final ByteString to post to the output.
   */
  def byteStringTransformer(f: ByteString ⇒ ByteString, finish: () ⇒ ByteString): Stage[ByteString, ByteString] = {
    new PushPullStage[ByteString, ByteString] {
      override def onPush(element: ByteString, ctx: Context[ByteString]): Directive =
        ctx.push(f(element))

      override def onPull(ctx: Context[ByteString]): Directive =
        if (ctx.isFinishing) ctx.pushAndFinish(finish())
        else ctx.pull()

      override def onUpstreamFinish(ctx: Context[ByteString]): TerminationDirective = ctx.absorbTermination()
    }
  }

  def failedPublisher[T](ex: Throwable): Publisher[T] =
    impl.ErrorPublisher(ex, "failed").asInstanceOf[Publisher[T]]

  def mapErrorTransformer(f: Throwable ⇒ Throwable): Flow[ByteString, ByteString, Unit] = {
    val transformer = new PushStage[ByteString, ByteString] {
      override def onPush(element: ByteString, ctx: Context[ByteString]): Directive =
        ctx.push(element)

      override def onUpstreamFailure(cause: Throwable, ctx: Context[ByteString]): TerminationDirective =
        ctx.fail(f(cause))
    }

    Flow[ByteString].section(name("transformError"))(_.transform(() ⇒ transformer))
  }

  def sliceBytesTransformer(start: Long, length: Long): Flow[ByteString, ByteString, Unit] = {
    val transformer = new StatefulStage[ByteString, ByteString] {

      def skipping = new State {
        var toSkip = start
        override def onPush(element: ByteString, ctx: Context[ByteString]): Directive =
          if (element.length < toSkip) {
            // keep skipping
            toSkip -= element.length
            ctx.pull()
          } else {
            become(taking(length))
            // toSkip <= element.length <= Int.MaxValue
            current.onPush(element.drop(toSkip.toInt), ctx)
          }
      }
      def taking(initiallyRemaining: Long) = new State {
        var remaining: Long = initiallyRemaining
        override def onPush(element: ByteString, ctx: Context[ByteString]): Directive = {
          val data = element.take(math.min(remaining, Int.MaxValue).toInt)
          remaining -= data.size
          if (remaining <= 0) ctx.pushAndFinish(data)
          else ctx.push(data)
        }
      }

      override def initial: State = if (start > 0) skipping else taking(length)
    }
    Flow[ByteString].section(name("sliceBytes"))(_.transform(() ⇒ transformer))
  }

  def limitByteChunksStage(maxBytesPerChunk: Int): Stage[ByteString, ByteString] =
    new StatefulStage[ByteString, ByteString] {
      def initial = WaitingForData
      case object WaitingForData extends State {
        def onPush(elem: ByteString, ctx: Context[ByteString]): Directive =
          if (elem.size <= maxBytesPerChunk) ctx.push(elem)
          else {
            become(DeliveringData(elem.drop(maxBytesPerChunk)))
            ctx.push(elem.take(maxBytesPerChunk))
          }
      }
      case class DeliveringData(remaining: ByteString) extends State {
        def onPush(elem: ByteString, ctx: Context[ByteString]): Directive =
          throw new IllegalStateException("Not expecting data")

        override def onPull(ctx: Context[ByteString]): Directive = {
          val toPush = remaining.take(maxBytesPerChunk)
          val toKeep = remaining.drop(maxBytesPerChunk)

          become {
            if (toKeep.isEmpty) WaitingForData
            else DeliveringData(toKeep)
          }
          if (ctx.isFinishing) ctx.pushAndFinish(toPush)
          else ctx.push(toPush)
        }
      }

      override def onUpstreamFinish(ctx: Context[ByteString]): TerminationDirective =
        current match {
          case WaitingForData    ⇒ ctx.finish()
          case _: DeliveringData ⇒ ctx.absorbTermination()
        }
    }

  /**
   * Applies a sequence of transformers on one source and returns a sequence of sources with the result. The input source
   * will only be traversed once.
   */
  def transformMultiple(input: Source[ByteString, Unit], transformers: immutable.Seq[Flow[ByteString, ByteString, _]])(implicit materializer: FlowMaterializer): immutable.Seq[Source[ByteString, Unit]] = ???
  //    transformers match {
  //      case Nil      ⇒ Nil
  //      case Seq(one) ⇒ Vector(input.via(one, Keep.left))
  //      case multiple ⇒
  //        val results = Vector.fill(multiple.size)(Sink.publisher[ByteString])
  //        val mat =
  //          FlowGraph() { implicit b ⇒
  //            import FlowGraph.Implicits._
  //
  //            val broadcast = Broadcast[ByteString](multiple.size, OperationAttributes.name("transformMultipleInputBroadcast"))
  //            input ~> broadcast.in
  //            var portIdx = 0
  //            (multiple, results).zipped.foreach { (trans, sink) ⇒
  //              broadcast.out(portIdx) ~> trans ~> sink
  //              portIdx += 1
  //            }
  //          }.run()
  //        results.map(s ⇒ Source(mat))
  //    }

  def mapEntityError(f: Throwable ⇒ Throwable): RequestEntity ⇒ RequestEntity =
    _.transformDataBytes(mapErrorTransformer(f))

  /**
   * Simple blocking Source backed by an InputStream.
   *
   * FIXME: should be provided by akka-stream, see #15588
   */
  def fromInputStreamSource(inputStream: InputStream, defaultChunkSize: Int = 65536): Source[ByteString, Unit] = {
    import akka.stream.impl._

    val onlyOnceFlag = new AtomicBoolean(false)

    val iterator = new Iterator[ByteString] {
      var finished = false
      if (onlyOnceFlag.get() || !onlyOnceFlag.compareAndSet(false, true))
        throw new IllegalStateException("One time source can only be instantiated once")

      def hasNext: Boolean = !finished
      def next(): ByteString =
        if (!finished) {
          val buffer = new Array[Byte](defaultChunkSize)
          val read = inputStream.read(buffer)
          if (read < 0) {
            finished = true
            inputStream.close()
            ByteString.empty
          } else ByteString.fromArray(buffer, 0, read)
        } else ByteString.empty
    }

    // FIXME FIXME FIXME: This should take the fileIODispatcher somehow, see commented out line
    //Source(() ⇒ iterator).withAttributes(OperationAttributes.dispatcher(settings.fileIODispatcher))
    Source(() ⇒ iterator)

  }

  /**
   * Returns a source that can only be used once for testing purposes.
   */
  def oneTimeSource[T, Mat](other: Source[T, Mat]): Source[T, Mat] = {
    import akka.stream.impl._

    val onlyOnceFlag = new AtomicBoolean(false)
    other.map { elem ⇒
      if (onlyOnceFlag.get() || !onlyOnceFlag.compareAndSet(false, true))
        throw new IllegalStateException("One time source can only be instantiated once")
      elem
    }
  }

  def runStrict(sourceData: ByteString, transformer: Flow[ByteString, ByteString, _], maxByteSize: Long, maxElements: Int): Try[Option[ByteString]] =
    runStrict(Iterator.single(sourceData), transformer, maxByteSize, maxElements)

  def runStrict(sourceData: Iterator[ByteString], transformer: Flow[ByteString, ByteString, _], maxByteSize: Long, maxElements: Int): Try[Option[ByteString]] =
    Try {
      // FIXME FIXME FIXME: This is just to make the tests pass, this should not get into the real version
      import scala.concurrent.Await
      import scala.concurrent.duration._

      import akka.actor.ActorSystem
      val sys = ActorSystem()
      implicit val mat = FlowMaterializer()(sys)

      Some(Await.result(Source(() ⇒ sourceData).via(transformer).runFold(ByteString.empty)(_ ++ _), 3.seconds))
    }

}

/**
 * INTERNAL API
 */
private[http] class EnhancedByteStringSource[Mat](val byteStringStream: Source[ByteString, Mat]) extends AnyVal {
  def join(implicit materializer: FlowMaterializer): Future[ByteString] =
    byteStringStream.runFold(ByteString.empty)(_ ++ _)
  def utf8String(implicit materializer: FlowMaterializer, ec: ExecutionContext): Future[String] =
    join.map(_.utf8String)
}
