package akka.typed

object Sample {

  sealed trait Command
  case class Increment(x: Int) extends Command
  case class Get(sender: ActorRef[GetResult]) extends Command
  case object Reset extends Command
  case class Other(s: String) extends Holder[String](s) with Command

  case class GetResult(x: Int)

  class Counter extends Actor[Command] {
    def behavior(count: Int): Behavior = SimpleBehavior {
      case Increment(x) =>
        behavior(count + x)
      case Get(sender) =>
        sender ! GetResult(count)
        SameBehavior
    }
    def initialBehavior = behavior(0)
  }

  val sys = ActorSystem(Props(new Counter), "Counter")

  sys ! Increment(42)

  class Wrapper[T](target: ActorRef[T]) extends Actor[T] {
    def initialBehavior = SimpleBehavior { msg =>
      target ! msg
      SameBehavior
    }
  }

  class Wrapper2[T](props: Props[T]) extends Actor[T] {
    def initialBehavior = FullBehavior {
      case (ctx, Left(PreStart)) =>
        behavior(ctx.actorOf(props, "target"))
    }
    def behavior(target: ActorRef[T]): Behavior = CompositeBehavior(
      {
        case (ctx, Failure(_, _)) => StoppedBehavior
      },
      { (ctx, msg) =>
        target ! msg
        SameBehavior
      })
  }

}