package akka.typed

import akka.actor.ActorSystem
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.Future

object Sample {

  sealed trait Command
  case class Increment(x: Int) extends Command
  case class Get(sender: ActorRef[GetResult]) extends Command
  case object Reset extends Command

  case class GetResult(x: Int)

  class Counter extends Actor[Command] {
    // warning: match may not be exhaustive. It would fail on the following input: Reset
    def behavior(count: Int): Behavior = Behavior.Simple {
      case Increment(x) =>
        behavior(count + x)
      case Get(sender) =>
        sender ! GetResult(count)
        Behavior.Same
    }
    def initialBehavior = behavior(0)
  }

  class Wrapper[T](target: ActorRef[T]) extends Actor[T] {
    def initialBehavior = Behavior.Simple { msg =>
      target ! msg
      Behavior.Same
    }
  }

  class Terminator extends Actor[Exception] {
    def initialBehavior = Behavior.Simple {
      case ex => throw ex
    }
  }

  abstract class Done
  case object Done extends Done
  case class WrapMsg[T](msg: T, replyTo: ActorRef[Done])

  class Wrapper2[T](props: Props[T]) extends Actor[WrapMsg[T]] {
    def initialBehavior = Behavior.Full {
      case (ctx, Left(PreStart)) =>
        waiting(ctx.watch(ctx.spawn(props, "target")))
    }
    def waiting(target: ActorRef[T]): Behavior = Behavior.Simple { msg =>
      target ! msg.msg
      finishing(target, msg.replyTo)
    }
    def finishing(target: ActorRef[T], replyTo: ActorRef[Done.type]): Behavior = Behavior.Composite(
      {
        case (ctx, Failure(_, _)) => Failure.Stop(Behavior.Same)
        case (ctx, Terminated(ref)) =>
          replyTo ! Done
          Behavior.Stopped
      },
      { (ctx, msg) =>
        target ! msg.msg
        Behavior.Same
      })
  }

  def main(args: Array[String]): Unit = {
    import Ops._
    import AskRef._
    implicit val t = Timeout(1.second)

    val sys = ActorSystem("Sample")

    /*
     * types ascribed just for information purposes and to prove that this compiles
     */
    val counter: ActorRef[Command] = sys.spawn(Props(new Counter))
    val wrapper: ActorRef[Command] = sys.spawn(Props(new Wrapper(counter)))
    val count = Await.result(wrapper ? Get, t.duration).x
    wrapper ! Increment(13)
    val count2 = Await.result(wrapper ? Get, t.duration).x
    println(s"first $count, then $count2, amazing!")

    val wrapper2: ActorRef[WrapMsg[Exception]] = sys.spawn(Props(new Wrapper2(Props(new Terminator))))
    val future: Future[Done] =
      // it is unfortunate that we need to annotate the type of the _ in this expression
      wrapper2 ? (WrapMsg(new RuntimeException("hello!"), _: ActorRef[Done]))
    // just for demonstration it could also be done like this:
    wrapper2 ?[Done](WrapMsg(new RuntimeException("hello!"), _))
    println(Await.result(future, t.duration))

    sys.shutdown()
  }

}