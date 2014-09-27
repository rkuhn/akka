package akka.typed

class BehaviorSpec extends TypedSpec {

  val childBehavior = Behavior.Static[(Int, ActorRef[String])] { case (n, ref) ⇒ ref ! n.toString }

  val behavior = Behavior.Contextual[String] { (ctx, msg) ⇒
    val child = ctx.spawn(Props(childBehavior), msg)
    child ! (42 -> ctx.self)
    Behavior.Same
  }

  object `A Behavior` {

    def `must interact with the context`() {
      val ctx = new EffectfulActorContext("effectual", Props(behavior), system)
      val next = behavior.message(ctx, "tester")
      ctx.getEffect() should be(Pure.Spawned("tester"))
      ctx.hasEffects should be(false)
      val child = ctx.getInbox[(Int, ActorRef[String])]("tester")
      val (num, ref) = child.receiveMsg()
      num should be(42)
      ref ! "42"
      ctx.inbox.receiveMsg() should be("42")
    }

  }

}