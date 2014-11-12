package akka.typed.patterns

import akka.typed.TypedSpec
import akka.typed.EffectfulActorContext
import akka.typed.Props
import akka.typed.ActorRef
import akka.typed.Inbox
import scala.concurrent.duration._

object ReceiverSpec {
  case class Msg(x: Int)
}

class ReceiverSpec extends TypedSpec {
  import ReceiverSpec._
  import Receiver._

  private def setup(): (EffectfulActorContext[Command[Msg]], EffectfulActorContext[Msg]) = {
    val ctx = new EffectfulActorContext("ctx", Props(behavior[Msg]), system)
    ctx -> ctx.asInstanceOf[EffectfulActorContext[Msg]]
  }

  object `A Receiver` {

    /*
     * This test suite assumes that the Receiver is only one actor with two
     * sides that share the same ActorRef.
     */
    def `must return "self" as external address`(): Unit = {
      val (int, ext) = setup()
      val inbox = Inbox.sync[ActorRef[Msg]]("extAddr")
      int.run(ExternalAddress(inbox.ref))
      int.hasEffects should be(false)
      inbox.receiveAll() should be(List(int.self))
    }

    def `must receive one message which arrived first`(): Unit = {
      val (int, ext) = setup()
      val inbox = Inbox.sync[Replies[Msg]]("getOne")
      // first with zero timeout
      ext.run(Msg(1))
      int.run(GetOne(Duration.Zero)(inbox.ref))
      int.getAllEffects() should be(Nil)
      inbox.receiveAll() should be(GetOneResult(int.self, Some(Msg(1))) :: Nil)
      // then with positive timeout
      ext.run(Msg(2))
      int.run(GetOne(1.second)(inbox.ref))
      int.getAllEffects() should be(Nil)
      inbox.receiveAll() should be(GetOneResult(int.self, Some(Msg(2))) :: Nil)
      // then with negative timeout
      ext.run(Msg(3))
      int.run(GetOne(1.second)(inbox.ref))
      int.getAllEffects() should be(Nil)
      inbox.receiveAll() should be(GetOneResult(int.self, Some(Msg(3))) :: Nil)
    }

  }

}