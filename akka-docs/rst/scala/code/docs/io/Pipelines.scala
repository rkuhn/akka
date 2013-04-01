/**
 * Copyright (C) 2013 Typesafe Inc. <http://www.typesafe.com>
 */

package docs.io

import docs.io.japi.LengthFieldFrame;
import akka.testkit.AkkaSpec
import akka.io.SymmetricPipelineStage
import akka.util.ByteString
import akka.io.SymmetricPipePair
import akka.util.ByteStringBuilder
import akka.util.ByteIterator
import akka.io.PipelineFactory
import scala.util.Success
import scala.util.Try
import akka.actor.Actor
import akka.actor.ActorRef
import akka.io.TickGenerator
import scala.concurrent.duration._
import akka.io.HasActorContext
import akka.actor.Props
import akka.actor.PoisonPill
import akka.io.PipelineContext

class PipelinesDocSpec extends AkkaSpec {

  //#data
  case class Person(first: String, last: String)
  case class HappinessCurve(points: IndexedSeq[Double])
  case class Message(persons: Seq[Person], stats: HappinessCurve)
  //#data

  //#format
  /**
   * This trait is used to formualate a requirement for the pipeline context.
   * In this example it is used to configure the byte order to be used.
   */
  trait HasByteOrder extends PipelineContext {
    def byteOrder: java.nio.ByteOrder
  }

  class MessageStage extends SymmetricPipelineStage[HasByteOrder, Message, ByteString] {

    override def apply(ctx: HasByteOrder) = new SymmetricPipePair[Message, ByteString] {

      implicit val byteOrder = ctx.byteOrder

      /**
       * Append a length-prefixed UTF-8 encoded string to the ByteStringBuilder.
       */
      def putString(builder: ByteStringBuilder, str: String): Unit = {
        val bs = ByteString(str, "UTF-8")
        builder putInt bs.length
        builder ++= bs
      }

      override val commandPipeline = { msg: Message ⇒
        val bs = ByteString.newBuilder

        // first store the persons
        bs putInt msg.persons.size
        msg.persons foreach { p ⇒
          putString(bs, p.first)
          putString(bs, p.last)
        }

        // then store the doubles
        bs putInt msg.stats.points.length
        bs putDoubles (msg.stats.points.toArray)

        // and return the result as a command
        Seq(Right(bs.result))
      }

      //#decoding-omitted
      //#decoding
      def getString(iter: ByteIterator): String = {
        val length = iter.getInt
        val bytes = new Array[Byte](length)
        iter getBytes bytes
        ByteString(bytes).utf8String
      }

      override val eventPipeline = { bs: ByteString ⇒
        val iter = bs.iterator

        val personLength = iter.getInt
        val persons =
          (1 to personLength) map (_ ⇒ Person(getString(iter), getString(iter)))

        val curveLength = iter.getInt
        val curve = new Array[Double](curveLength)
        iter getDoubles curve

        // verify that this was all; could be left out to allow future extensions
        assert(iter.isEmpty)

        Seq(Left(Message(persons, HappinessCurve(curve))))
      }
      //#decoding

      override val managementPort: Mgmt = {
        case x @ TickGenerator.Tick ⇒ testActor ! x; Nil
      }
      //#decoding-omitted
    }
  }
  //#format

  "A MessageStage" must {

    //#message
    val msg =
      Message(
        Seq(
          Person("Alice", "Gibbons"),
          Person("Bob", "Sparsely")),
        HappinessCurve(Array(1.0, 3.0, 5.0)))
    //#message

    //#byteorder
    val ctx = new HasByteOrder {
      def byteOrder = java.nio.ByteOrder.BIG_ENDIAN
    }
    //#byteorder

    "correctly encode and decode" in {
      //#build-pipeline
      val stages =
        new MessageStage >>
          new LengthFieldFrame(10000)

      val (cmd, evt, mgmt) = PipelineFactory.buildFunctionTriple(ctx, stages)

      val encoded: (Iterable[Message], Iterable[ByteString]) = cmd(msg)
      //#build-pipeline
      encoded._1 must have size 0
      encoded._2 must have size 1

      evt(encoded._2.head)._1 must be === Seq(msg)
    }

    "demonstrate Injector/Sink" in {
      val commandHandler = testActor
      val eventHandler = testActor

      //#build-sink
      val stages =
        new MessageStage >>
          new LengthFieldFrame(10000)

      val injector = PipelineFactory.buildWithSinkFunctions(ctx, stages)(
        commandHandler ! _, // will receive messages of type Try[ByteString]
        eventHandler ! _ // will receive messages of type Try[Message]
        )

      injector.injectCommand(msg)
      //#build-sink
      val encoded = expectMsgType[Success[ByteString]].get

      injector.injectEvent(encoded)
      expectMsgType[Try[Message]].get must be === msg
    }

    "demonstrate management port and context" in {
      //#actor
      class Processor(cmds: ActorRef, evts: ActorRef) extends Actor {

        val ctx = new HasActorContext with HasByteOrder {
          def context = Processor.this.context
          def byteOrder = java.nio.ByteOrder.BIG_ENDIAN
        }

        val pipeline = PipelineFactory.buildWithSinkFunctions(ctx,
          new TickGenerator(1000.millis) >>
            new MessageStage >>
            new LengthFieldFrame(10000) //
            )(
            // failure in the pipeline will fail this actor
            cmd ⇒ cmds ! cmd.get,
            evt ⇒ evts ! evt.get)

        // start off the ticks, but only ONCE
        import TickGenerator.Tick
        override def preStart() { pipeline.managementCommand(Tick) }
        override def postRestart(thr: Throwable) {} // do not call preStart() again

        def receive = {
          case m: Message    ⇒ pipeline.injectCommand(m)
          case b: ByteString ⇒ pipeline.injectEvent(b)
          case Tick          ⇒ pipeline.managementCommand(Tick)
        }
      }
      //#actor

      import TickGenerator.Tick
      val proc = system.actorOf(Props(new Processor(testActor, testActor)), "processor")
      expectMsg(Tick)
      proc ! msg
      val encoded = expectMsgType[ByteString]
      proc ! encoded
      val decoded = expectMsgType[Message]
      decoded must be === msg

      within(1.7.seconds, 3.seconds) {
        expectMsg(Tick)
        expectMsg(Tick)
        proc ! PoisonPill
        expectNoMsg
      }
    }

  }

}