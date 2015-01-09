/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

class BehaviorSpec extends TypedSpec {

  sealed trait Command {
    def expectedResponse(ctx: ActorContext[Command]): Seq[Event]
  }
  case object GetSelf extends Command {
    def expectedResponse(ctx: ActorContext[Command]): Seq[Event] = Self(ctx.self) :: Nil
  }
  case object Miss extends Command {
    def expectedResponse(ctx: ActorContext[Command]): Seq[Event] = Missed :: Nil
  }
  case object Ignore extends Command {
    def expectedResponse(ctx: ActorContext[Command]): Seq[Event] = Ignored :: Nil
  }
  case object Ping extends Command {
    def expectedResponse(ctx: ActorContext[Command]): Seq[Event] = Pong :: Nil
  }
  case object Swap extends Command {
    def expectedResponse(ctx: ActorContext[Command]): Seq[Event] = Swapped :: Nil
  }
  case class GetState(replyTo: ActorRef[State]) extends Command {
    def expectedResponse(ctx: ActorContext[Command]): Seq[Event] = Nil
  }
  object GetState {
    def apply()(implicit inbox: Inbox.SyncInbox[State]): GetState = GetState(inbox.ref)
  }

  sealed trait Event
  case class GotSignal(signal: Signal) extends Event
  case class Self(self: ActorRef[Command]) extends Event
  case object Missed extends Event
  case object Ignored extends Event
  case object Pong extends Event
  case object Swapped extends Event

  trait State { def next: State }
  val StateA: State = new State { override def toString = "StateA"; override def next = StateB }
  val StateB: State = new State { override def toString = "StateB"; override def next = StateA }

  trait Common {
    def behavior(monitor: ActorRef[Event]): Behavior[Command]

    case class Setup(ctx: EffectfulActorContext[Command], inbox: Inbox.SyncInbox[Event])

    protected def mkCtx(requirePreStart: Boolean = false) = {
      val inbox = Inbox.sync[Event]("evt")
      val ctx = new EffectfulActorContext("ctx", Props(behavior(inbox.ref)), system)
      val msgs = inbox.receiveAll()
      if (requirePreStart)
        msgs should be(GotSignal(PreStart) :: Nil)
      Setup(ctx, inbox)
    }

    protected implicit class Check(val setup: Setup) {
      def check(signal: Signal): Setup = {
        setup.ctx.signal(signal)
        setup.inbox.receiveAll() should be(GotSignal(signal) :: Nil)
        setup
      }
      def check(command: Command): Setup = {
        setup.ctx.run(command)
        setup.inbox.receiveAll() should be(command.expectedResponse(setup.ctx))
        setup
      }
      def check[T](command: Command, aux: T*)(implicit inbox: Inbox.SyncInbox[T]): Setup = {
        setup.ctx.run(command)
        setup.inbox.receiveAll() should be(command.expectedResponse(setup.ctx))
        inbox.receiveAll() should be(aux)
        setup
      }
    }

    protected val ex = new Exception("mine!")
  }

  trait Lifecycle extends Common {
    def `must react to PreStart`(): Unit = {
      mkCtx(requirePreStart = true)
    }

    def `must react to PostStop`(): Unit = {
      mkCtx().check(PostStop)
    }

    def `must react to PostStop after a message`(): Unit = {
      mkCtx().check(GetSelf).check(PostStop)
    }

    def `must react to PreRestart`(): Unit = {
      mkCtx().check(PreRestart(ex))
    }

    def `must react to PreRestart after a message`(): Unit = {
      mkCtx().check(GetSelf).check(PreRestart(ex))
    }

    def `must react to PostRestart`(): Unit = {
      mkCtx().check(PostRestart(ex))
    }

    def `must react to a message after PostRestart`(): Unit = {
      mkCtx().check(PostRestart(ex)).check(GetSelf)
    }

    def `must react to Failed`(): Unit = {
      val setup @ Setup(ctx, inbox) = mkCtx()
      ctx.setFailureResponse(Failed.NoFailureResponse)
      setup.check(Failed(ex, inbox.ref))
      ctx.getFailureResponse should be(Failed.Restart)
    }

    def `must react to Failed after a message`(): Unit = {
      val setup @ Setup(ctx, inbox) = mkCtx().check(GetSelf)
      ctx.setFailureResponse(Failed.NoFailureResponse)
      setup.check(Failed(ex, inbox.ref))
      ctx.getFailureResponse should be(Failed.Restart)
    }

    def `must react to a message after Failed`(): Unit = {
      val setup @ Setup(ctx, inbox) = mkCtx()
      ctx.setFailureResponse(Failed.NoFailureResponse)
      setup.check(Failed(ex, inbox.ref))
      ctx.getFailureResponse should be(Failed.Restart)
      setup.check(GetSelf)
    }

    def `must react to ReceiveTimeout`(): Unit = {
      mkCtx().check(ReceiveTimeout)
    }

    def `must react to ReceiveTimeout after a message`(): Unit = {
      mkCtx().check(GetSelf).check(ReceiveTimeout)
    }

    def `must react to a message after ReceiveTimeout`(): Unit = {
      mkCtx().check(ReceiveTimeout).check(GetSelf)
    }

    def `must react to Terminated`(): Unit = {
      mkCtx().check(Terminated(Inbox.sync("x").ref))
    }

    def `must react to Terminated after a message`(): Unit = {
      mkCtx().check(GetSelf).check(Terminated(Inbox.sync("x").ref))
    }

    def `must react to a message after Terminated`(): Unit = {
      mkCtx().check(Terminated(Inbox.sync("x").ref)).check(GetSelf)
    }
  }

  trait Messages extends Common {
    def `must react to two messages`(): Unit = {
      mkCtx().check(Ping).check(Ping)
    }

    def `must react to a message after missing one`(): Unit = {
      mkCtx().check(Miss).check(Ping)
    }

    def `must react to a message after ignoring one`(): Unit = {
      mkCtx().check(Ignore).check(Ping)
    }
  }

  trait Become extends Common {
    private implicit val inbox = Inbox.sync[State]("state")

    def `must be in state A`(): Unit = {
      mkCtx().check(GetState(), StateA)
    }

    def `must switch to state B`(): Unit = {
      mkCtx().check(Swap).check(GetState(), StateB)
    }

    def `must switch back to state A`(): Unit = {
      mkCtx().check(Swap).check(Swap).check(GetState(), StateA)
    }
  }

  trait BecomeWithLifecycle extends Become with Lifecycle {
    def `must react to PostStop after swap`(): Unit = {
      mkCtx().check(Swap).check(PostStop)
    }

    def `must react to PostStop after a message after swap`(): Unit = {
      mkCtx().check(Swap).check(GetSelf).check(PostStop)
    }

    def `must react to PreRestart after swap`(): Unit = {
      mkCtx().check(Swap).check(PreRestart(ex))
    }

    def `must react to PreRestart after a message after swap`(): Unit = {
      mkCtx().check(Swap).check(GetSelf).check(PreRestart(ex))
    }

    def `must react to a message after PostRestart after swap`(): Unit = {
      mkCtx().check(PostRestart(ex)).check(Swap).check(GetSelf)
    }

    def `must react to Failed after swap`(): Unit = {
      val setup @ Setup(ctx, inbox) = mkCtx().check(Swap)
      ctx.setFailureResponse(Failed.NoFailureResponse)
      setup.check(Failed(ex, inbox.ref))
      ctx.getFailureResponse should be(Failed.Restart)
    }

    def `must react to Failed after a message after swap`(): Unit = {
      val setup @ Setup(ctx, inbox) = mkCtx().check(Swap).check(GetSelf)
      ctx.setFailureResponse(Failed.NoFailureResponse)
      setup.check(Failed(ex, inbox.ref))
      ctx.getFailureResponse should be(Failed.Restart)
    }

    def `must react to a message after Failed after swap`(): Unit = {
      val setup @ Setup(ctx, inbox) = mkCtx().check(Swap)
      ctx.setFailureResponse(Failed.NoFailureResponse)
      setup.check(Failed(ex, inbox.ref))
      ctx.getFailureResponse should be(Failed.Restart)
      setup.check(GetSelf)
    }

    def `must react to ReceiveTimeout after swap`(): Unit = {
      mkCtx().check(Swap).check(ReceiveTimeout)
    }

    def `must react to ReceiveTimeout after a message after swap`(): Unit = {
      mkCtx().check(Swap).check(GetSelf).check(ReceiveTimeout)
    }

    def `must react to a message after ReceiveTimeout after swap`(): Unit = {
      mkCtx().check(Swap).check(ReceiveTimeout).check(GetSelf)
    }

    def `must react to Terminated after swap`(): Unit = {
      mkCtx().check(Swap).check(Terminated(Inbox.sync("x").ref))
    }

    def `must react to Terminated after a message after swap`(): Unit = {
      mkCtx().check(Swap).check(GetSelf).check(Terminated(Inbox.sync("x").ref))
    }

    def `must react to a message after Terminated after swap`(): Unit = {
      mkCtx().check(Swap).check(Terminated(Inbox.sync("x").ref)).check(GetSelf)
    }
  }

  private def mkFull(monitor: ActorRef[Event], state: State = StateA): Behavior[Command] =
    Behavior.Full {
      case (ctx, Left(signal)) ⇒
        monitor ! GotSignal(signal)
        if (signal.isInstanceOf[Failed]) ctx.setFailureResponse(Failed.Restart)
        Behavior.Same
      case (ctx, Right(GetSelf)) ⇒
        monitor ! Self(ctx.self)
        Behavior.Same
      case (ctx, Right(Miss)) ⇒
        monitor ! Missed
        Behavior.Unhandled
      case (ctx, Right(Ignore)) ⇒
        monitor ! Ignored
        Behavior.Same
      case (ctx, Right(Ping)) ⇒
        monitor ! Pong
        mkFull(monitor, state)
      case (ctx, Right(Swap)) ⇒
        monitor ! Swapped
        mkFull(monitor, state.next)
      case (ctx, Right(GetState(replyTo))) ⇒
        replyTo ! state
        Behavior.Same
    }

  object `A Full Behavior` extends Messages with BecomeWithLifecycle {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] = mkFull(monitor)
  }

  object `A FullTotal Behavior` extends Messages with BecomeWithLifecycle {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] = behv(monitor, StateA)
    private def behv(monitor: ActorRef[Event], state: State): Behavior[Command] =
      Behavior.FullTotal { (ctx, msg) ⇒
        msg match {
          case Left(signal) ⇒
            monitor ! GotSignal(signal)
            if (signal.isInstanceOf[Failed]) ctx.setFailureResponse(Failed.Restart)
            Behavior.Same
          case Right(GetSelf) ⇒
            monitor ! Self(ctx.self)
            Behavior.Same
          case Right(Miss) ⇒
            monitor ! Missed
            Behavior.Unhandled
          case Right(Ignore) ⇒
            monitor ! Ignored
            Behavior.Same
          case Right(Ping) ⇒
            monitor ! Pong
            behv(monitor, state)
          case Right(Swap) ⇒
            monitor ! Swapped
            behv(monitor, state.next)
          case Right(GetState(replyTo)) ⇒
            replyTo ! state
            Behavior.Same
        }
      }
  }

  object `A ContextAware Behavior` extends Messages with BecomeWithLifecycle {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.ContextAware(ctx ⇒ mkFull(monitor))
  }

  object `A SelfAware Behavior` extends Messages with BecomeWithLifecycle {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.SelfAware(self ⇒ mkFull(monitor))
  }

  object `A SynchronousSelf Behavior` extends Messages with BecomeWithLifecycle {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.SynchronousSelf(self ⇒ mkFull(monitor))
  }

  object `A Behavior combined with And (left)` extends Messages with BecomeWithLifecycle {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.And(mkFull(monitor), Behavior.Empty)
  }

  object `A Behavior combined with And (right)` extends Messages with BecomeWithLifecycle {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.And(Behavior.Empty, mkFull(monitor))
  }

  object `A Behavior combined with Or (left)` extends Messages with BecomeWithLifecycle {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.Or(mkFull(monitor), Behavior.Empty)
  }

  object `A Behavior combined with Or (right)` extends Messages with BecomeWithLifecycle {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.Or(Behavior.Empty, mkFull(monitor))
  }

  object `A Simple Behavior` extends Messages with Become {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] = behaviorA(monitor)
    def behaviorA(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.Simple {
        case Ping ⇒
          monitor ! Pong
          behavior(monitor)
        case Miss ⇒
          monitor ! Missed
          Behavior.Unhandled
        case Ignore ⇒
          monitor ! Ignored
          Behavior.Same
        case GetSelf ⇒ Behavior.Unhandled
        case Swap ⇒
          monitor ! Swapped
          behaviorB(monitor)
        case GetState(replyTo) ⇒
          replyTo ! StateA
          Behavior.Same
      }
    def behaviorB(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.Simple {
        case Ping ⇒
          monitor ! Pong
          behavior(monitor)
        case Miss ⇒
          monitor ! Missed
          Behavior.Unhandled
        case Ignore ⇒
          monitor ! Ignored
          Behavior.Same
        case GetSelf ⇒ Behavior.Unhandled
        case Swap ⇒
          monitor ! Swapped
          behaviorA(monitor)
        case GetState(replyTo) ⇒
          replyTo ! StateB
          Behavior.Same
      }
  }

  object `A Static Behavior` extends Messages {
    override def behavior(monitor: ActorRef[Event]): Behavior[Command] =
      Behavior.Static {
        case Ping ⇒
          monitor ! Pong
        case Miss ⇒
          monitor ! Missed
        case Ignore ⇒
          monitor ! Ignored
        case GetSelf     ⇒ Behavior.Unhandled
        case Swap        ⇒ Behavior.Unhandled
        case GetState(_) ⇒ Behavior.Unhandled
      }
  }
}
