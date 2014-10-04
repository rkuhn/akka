package akka.typed

import Receptionist._
import Behavior._

class ReceptionistSpec extends TypedSpec {

  trait ServiceA
  case object ServiceKeyA extends ServiceKey[ServiceA]
  val propsA = Props(Static[ServiceA](msg ⇒ ()))

  trait ServiceB
  case object ServiceKeyB extends ServiceKey[ServiceB]
  val propsB = Props(Static[ServiceB](msg ⇒ ()))

  object `A Receptionist` {

    def `must register a service`(): Unit = {
      val ctx = new EffectfulActorContext("register", Props(receptionist), system)
      val a = Inbox.sync[ServiceA]("a")
      ctx.run(Register(ServiceKeyA, a.ref))
      val q = Inbox.sync[Listing[ServiceA]]("q")
      ctx.run(Find(ServiceKeyA, q.ref))
      q.receiveMsg() should be(Listing(ServiceKeyA, Set(a.ref)))
    }

    def `must register two services`(): Unit = {
      val ctx = new EffectfulActorContext("registertwo", Props(receptionist), system)
      val a = Inbox.sync[ServiceA]("a")
      ctx.run(Register(ServiceKeyA, a.ref))
      val b = Inbox.sync[ServiceB]("b")
      ctx.run(Register(ServiceKeyB, b.ref))
      val q = Inbox.sync[Listing[_]]("q")
      ctx.run(Find(ServiceKeyA, q.ref))
      q.receiveMsg() should be(Listing(ServiceKeyA, Set(a.ref)))
      ctx.run(Find(ServiceKeyB, q.ref))
      q.receiveMsg() should be(Listing(ServiceKeyB, Set(b.ref)))
    }

    def `must register two services with the same key`(): Unit = {
      val ctx = new EffectfulActorContext("registertwosame", Props(receptionist), system)
      val a1 = Inbox.sync[ServiceA]("a1")
      ctx.run(Register(ServiceKeyA, a1.ref))
      val a2 = Inbox.sync[ServiceA]("a2")
      ctx.run(Register(ServiceKeyA, a2.ref))
      val q = Inbox.sync[Listing[_]]("q")
      ctx.run(Find(ServiceKeyA, q.ref))
      q.receiveMsg() should be(Listing(ServiceKeyA, Set(a1.ref, a2.ref)))
      ctx.run(Find(ServiceKeyB, q.ref))
      q.receiveMsg() should be(Listing(ServiceKeyB, Set.empty[ActorRef[ServiceB]]))
    }

    def `must unregister services when they terminate`(): Unit = {
      val ctx = new EffectfulActorContext("registertwosame", Props(receptionist), system)
      val a = Inbox.sync[ServiceA]("a")
      ctx.run(Register(ServiceKeyA, a.ref))
      ctx.getEffect() should be(Pure.Watched(a.ref))

      val b = Inbox.sync[ServiceB]("b")
      ctx.run(Register(ServiceKeyB, b.ref))
      ctx.getEffect() should be(Pure.Watched(b.ref))

      val c = Inbox.sync[Any]("c")
      ctx.run(Register(ServiceKeyA, c.ref))
      ctx.run(Register(ServiceKeyB, c.ref))
      ctx.getAllEffects() should be(Seq(Pure.Watched(c.ref), Pure.Watched(c.ref)))

      val q = Inbox.sync[Listing[_]]("q")
      ctx.run(Find(ServiceKeyA, q.ref))
      q.receiveMsg() should be(Listing(ServiceKeyA, Set(a.ref, c.ref)))
      ctx.run(Find(ServiceKeyB, q.ref))
      q.receiveMsg() should be(Listing(ServiceKeyB, Set(b.ref, c.ref)))

      ctx.signal(Terminated(c.ref))
      ctx.run(Find(ServiceKeyA, q.ref))
      q.receiveMsg() should be(Listing(ServiceKeyA, Set(a.ref)))
      ctx.run(Find(ServiceKeyB, q.ref))
      q.receiveMsg() should be(Listing(ServiceKeyB, Set(b.ref)))
    }

  }

}