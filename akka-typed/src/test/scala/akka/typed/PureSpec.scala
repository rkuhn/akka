package akka.typed

import Pure._
import Behavior._
import Pure.semantics._
import akka.actor.ActorSystem
import scala.collection.immutable

class PureSpec extends TypedSpec {

  object `A Pure Behavior` {

    def `must allow access to the context`() {
      type Things = (ActorRef[SendThings], Props[SendThings], ActorSystem)
      case class SendThings(target: ActorRef[Things])
      val behavior = Total[SendThings] { (ctx, msg) ⇒
        msg match {
          case Left(_) ⇒ ctx.unit(Same)
          case Right(m) ⇒
            for {
              self ← ctx.self
              props ← ctx.props
              system ← ctx.system
              _ ← ctx.send(m.target, (self, props, system))
            } yield Same
        }
      }
      val props = Props(behavior)
      val ctx = new DummyActorContext("context", props, system)
      val inbox = Inbox.sync[Things]("context_probe")
      val msg = (ctx.self, props, system)
      behavior.run(ctx, Right(SendThings(inbox.ref))).requireEffects(PF) {
        case Messaged(_, `msg`) :: Nil ⇒
      }
      inbox.receiveAll() should be(List(msg))
    }

    def `must track child creation`() {
      type Ref = Either[ActorRef[Nothing], ActorRef[Nothing]]
      case class DoIt(p: Props[(Ref, Ref)])
      val absorber = Props(Static[(Ref, Ref)] { case _ ⇒ })
      val behavior = Total[DoIt] { (ctx, msg) ⇒
        msg match {
          case Left(_) ⇒ ctx.unit(Same)
          case Right(DoIt(p)) ⇒
            for {
              c1 ← ctx.spawn(p, "child")
              c2 ← ctx.spawn(p, "child")
              _ ← ctx.send(c1.fold(_ ⇒ c2.right.get, identity), c1 -> c2)
            } yield Same
        }
      }
      val ctx = new DummyActorContext("context", Props(behavior), system)
      behavior.run(ctx, Right(DoIt(absorber))).requireEffects(PF) {
        case Spawned("child") :: Spawned("child") :: Messaged(ref1, (Right(ref2), Left(ref3))) :: Nil ⇒
          val childRef = ctx.child("child").get
          ref1 should be(childRef)
          ref2 should be(childRef)
          ref3 should be(childRef)
      }
    }

    def `must track child watching`() {

    }

    def `must allow failure handling`() {
      pending
    }

    def `must support receive timeouts`() {
      pending
    }

    def `must not fire receive timeout after disabling it`() {
      pending
    }

  }

}