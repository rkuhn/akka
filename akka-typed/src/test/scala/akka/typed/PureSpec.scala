package akka.typed

class PureSpec extends TypedSpec {

  sealed trait Command
  case object Create extends Command
  case object Send extends Command
  case class MkChild(name: String, props: Props[String]) extends Command
  case object GetRefs extends Command
  case class SetRefs(ref1: Either[ActorRef[Nothing], ActorRef[String]], ref2: Either[ActorRef[Nothing], ActorRef[String]]) extends Command

  val behavior = Pure.Behavior[Command] { (ctx, msg) ⇒
    msg match {
      case Left(x) ⇒ ctx.unit(Behavior.Same)
      case Right(Create) ⇒
        for {
          ref ← ctx.spawn(Props(child))
        } yield next(ref)
      case Right(MkChild(name, props)) ⇒
        for {
          ref1 ← ctx.spawn(props, name)
          ref2 ← ctx.spawn(props, name)
        } yield next2(ref1, ref2)
    }
  }

  def next(ref: ActorRef[String]) = Pure.Behavior[Command] { (ctx, msg) ⇒
    msg match {
      case Right(Send) ⇒
        ref ! "Hello World"
        ctx.unit(Behavior.Same)
    }
  }

  def next2(ref1: Either[ActorRef[Nothing], ActorRef[String]], ref2: Either[ActorRef[Nothing], ActorRef[String]]) =
    Pure.Behavior[Command] { (ctx, msg) ⇒
      msg match {
        case Right(GetRefs) ⇒
          for {
            self ← ctx.self
            _ ← ctx.unit(self ! SetRefs(ref1, ref2))
          } yield Behavior.Same
      }
    }

  val child = Behavior.Static[String] { case msg ⇒ }

  object `A Pure Behavior` {

    def `must return effects`() {
      val ctx = new DummyActorContext("fancy", Props(behavior), system)
      // TODO: maybe stash away effects etc. in a context to the test procedure,
      // so that this extraction of the return values does not clutter the test
      val (effects, next: Pure.Behavior[_]) = behavior.run(ctx, Right(Create))
      // TODO: think about tracking message sends as effects
      val child = effects match {
        case Pure.Spawned(x) :: Nil ⇒ x
        case other                  ⇒ fail(s"$other was not Pure.Spawned")
      }
      next.run(ctx, Right(Send))
      // TODO: need synchronous Inbox instead of this one
      ctx.getInbox[String](child).receiveMsg() should be("Hello World")
    }

    def `must track created child actors`() {
      val ctx = new DummyActorContext("fancy", Props(behavior), system)
      val (effects, next: Pure.Behavior[_]) = behavior.run(ctx, Right(MkChild("hello", Props(child))))
      effects should be(List(Pure.Spawned("hello"), Pure.Spawned("hello")))
      next.run(ctx, Right(GetRefs))
      val c = ctx.child("hello").get.upcast[String]
      ctx.inbox.receiveMsg() should be(SetRefs(Right(c), Left(c)))
    }

  }

}