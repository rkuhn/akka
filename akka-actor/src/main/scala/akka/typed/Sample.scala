package akka.typed

import akka.actor.ActorSystem

object Sample {

  sealed trait Command
  case class Increment(x: Int) extends Command
  case class Get(sender: ActorRef[GetResult]) extends Command
  case object Reset extends Command
  case class Other(s: String) extends Holder[String](s) with Command

  case class GetResult(x: Int)

  class Counter extends Actor[Command] {
    def behavior(count: Int): Behavior = Behavior.Simple {
      case Increment(x) =>
        behavior(count + x)
      case Get(sender) =>
        sender ! GetResult(count)
        Behavior.Same
    }
    def initialBehavior = behavior(0)
  }

  val sys = ActorSystem(Props(new Counter), "Counter")

  sys ! Increment(42)

  class Wrapper[T](target: ActorRef[T]) extends Actor[T] {
    def initialBehavior = Behavior.Simple { msg =>
      target ! msg
      Behavior.Same
    }
  }

  class Wrapper2[T](props: Props[T]) extends Actor[T] {
    def initialBehavior = Behavior.Full {
      case (ctx, Left(PreStart)) =>
        behavior(ctx.actorOf(props, "target"))
    }
    def behavior(target: ActorRef[T]): Behavior = Behavior.Composite(
      {
        case (ctx, Failure(_, _)) => Behavior.Stopped
      },
      { (ctx, msg) =>
        target ! msg
        Behavior.Same
      })
  }

}