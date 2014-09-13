package akka.typed

import akka.actor.ActorSystem
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.Future

object Sample {

  import AskPattern._

  sealed trait Command
  case class Increment(x: Int) extends Command
  case class Get(sender: ActorRef[GetResult]) extends Command
  case object Reset extends Command

  case class GetResult(x: Int)

  // warning: match may not be exhaustive. It would fail on the following input: Reset
  def counter(count: Int): Behavior[Command] =
    Behavior.Simple {
      case Increment(x) ⇒
        counter(count + x)
      case Get(sender) ⇒
        sender ! GetResult(count)
        Behavior.Same
    }

  def wrapper[T](target: ActorRef[T]): Behavior[T] =
    Behavior.Simple { msg ⇒
      target ! msg
      Behavior.Same
    }

  val terminator: Behavior[Exception] = Behavior.Simple { case ex ⇒ throw ex }

  abstract class Done
  case object Done extends Done
  case class WrapMsg[T](msg: T, replyTo: ActorRef[Done])

  /*
   * The new way of writing actors: just behaviors, nothing special about them.
   * 
   */
  object wrapper2 {
    def apply[T](props: Props[T]): Behavior[WrapMsg[T]] =
      Behavior.Full {
        case (ctx, Left(PreStart)) ⇒
          waiting(ctx.watch(ctx.spawn(props, "target")))
      }
    def waiting[T](target: ActorRef[T]): Behavior[WrapMsg[T]] =
      Behavior.Simple { msg ⇒
        target ! msg.msg
        finishing(target, msg.replyTo)
      }
    def finishing[T](target: ActorRef[T], replyTo: ActorRef[Done.type]): Behavior[WrapMsg[T]] =
      Behavior.Composite(
        {
          case (ctx, Failure(_, _)) ⇒ Failure.Stop(Behavior.Same)
          case (ctx, Terminated(ref)) ⇒
            replyTo ! Done
            Behavior.Stopped
        },
        { (ctx, msg) ⇒
          target ! msg.msg
          Behavior.Same
        })
  }

  class Stateful {
    import Behavior._
    var x = 0
    val behavior: Behavior[Command] = Static {
      case Get(replyTo) => replyTo ! GetResult(x)
      case Increment(n) => x += n
      case Reset        => x = 0
    }
  }

  def main(args: Array[String]): Unit = {
    import Ops._
    implicit val t = Timeout(1.second)

    val sys = ActorSystem("Sample")

    /*
     * types ascribed just for information purposes and to prove that this compiles
     */
    val count: ActorRef[Command] = sys.spawn(Props(counter(0)))
    val wrap: ActorRef[Command] = sys.spawn(Props(wrapper(count)))
    val c1 = Await.result(wrap ? Get, t.duration).x
    wrap ! Increment(13)
    val c2 = Await.result(wrap ? Get, t.duration).x
    println(s"first $c1, then $c2, amazing!")

    val wrap2: ActorRef[WrapMsg[Exception]] = sys.spawn(Props(wrapper2(Props(terminator))))
    val future: Future[Done] =
      /*
       * it is unfortunate that we need to annotate the type of the _ in this expression;
       * in this sample the information comes from the expected return type since val future
       * is annotated, so this will probably not be an issue in most cases
       */
      wrap2 ? (WrapMsg(new RuntimeException("hello!"), _))
    // just for demonstration it could also be done like this:
    //wrap2 ?[Done](WrapMsg(new RuntimeException("hello!"), _))
    // or like this (doesn’t matter which ActorRef is passed in, just needs the system’s ActorRefProvider)
    val pr = PromiseRef[Done](wrap2)
    wrap2 ! WrapMsg(new RuntimeException("hello!"), pr.ref)
    println(Await.result(future, t.duration))

    val count2 = sys.spawn(Props(new Stateful().behavior))
    count2 ! Get(sys.deadLetters)
    count2 ! Increment(42)
    println(Await.result(count2 ? Get, t.duration).x)

    sys.terminate()
  }

}
