package akka.typed

import Behavior._
import Ops._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future

class PerformanceSpec extends TypedSpec {

  import system.dispatcher

  object `A static behavior` {
    def pingpong(pairs: Int, pings: Int, count: Int, executor: String) = {
      case class Ping(x: Int, pong: ActorRef[Pong])
      case class Pong(x: Int, ping: ActorRef[Ping])
      val inbox = Inbox.async[Pong]

      val pinger = Props(SelfAware[Ping](self ⇒ Static { msg ⇒
        if (msg.x == 0) inbox.ref ! Pong(0, self)
        else msg.pong ! Pong(msg.x - 1, self)
      })).withDispatcher(executor)

      val ponger = Props(SelfAware[Pong](self ⇒ Static { msg ⇒
        msg.ping ! Ping(msg.x, self)
      })).withDispatcher(executor)

      val actors =
        for (i ← 1 to pairs)
          yield (system.spawn(pinger, s"pinger-$i"), system.spawn(ponger, s"ponger-$i"))

      val start = Deadline.now
      
      for {
        (ping, pong) ← actors
        _ ← 1 to pings
      } ping ! Ping(count, pong)

      val futures = for (_ ← 1 to pairs * pings) yield inbox.receiveMsg(20.seconds)
      Await.result(Future.sequence(futures), 30.seconds)
      
      val stop = Deadline.now

      val rate = 2L * count * pairs * pings / (stop - start).toMillis
      info(s"messaging rate was $rate/ms")

      for ((ping, pong) ← actors) {
        inbox.watch(ping)
        system.stop(ping.ref)
        await(inbox.receiveTerminated(1.second))
        inbox.watch(pong)
        system.stop(pong.ref)
        await(inbox.receiveTerminated(1.second))
      }
    }

    object `must be fast` {

      def `01 when warming up`() { pingpong(1, 1, 1000000, "dispatcher-1") }
      def `02 when using a single message on a single thread`() { pingpong(1, 1, 1000000, "dispatcher-1") }
      def `03 when using a 10 messages on a single thread`() { pingpong(1, 10, 1000000, "dispatcher-1") }
      def `04 when using a single message on two threads`() { pingpong(1, 1, 1000000, "dispatcher-2") }
      def `05 when using a 10 messages on two threads`() { pingpong(1, 10, 1000000, "dispatcher-2") }
      def `06 when using 4 pairs with a single message`() { pingpong(4, 1, 1000000, "dispatcher-8") }
      def `07 when using 4 pairs with 10 messages`() { pingpong(4, 10, 1000000, "dispatcher-8") }
      def `08 when using 8 pairs with a single message`() { pingpong(8, 1, 1000000, "dispatcher-8") }
      def `09 when using 8 pairs with 10 messages`() { pingpong(8, 10, 1000000, "dispatcher-8") }

    }
  }

}