/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.TreeSet
import scala.collection.mutable.Queue
import scala.concurrent.Await
import scala.concurrent.duration.{ Deadline, Duration, DurationInt, FiniteDuration }
import akka.{ actor ⇒ a }
import akka.pattern.ask
import akka.util.Helpers.ConfigOps
import akka.util.Timeout
import scala.concurrent.Future
import akka.actor.MinimalActorRef
import java.util.concurrent.ConcurrentLinkedQueue
import akka.actor.ActorPath
import akka.actor.RootActorPath
import akka.actor.Address
import scala.reflect.ClassTag
import scala.collection.immutable
import scala.annotation.tailrec
import akka.actor.ActorRefProvider
import scala.concurrent.ExecutionContext

/**
 * INTERNAL API
 */
object Inbox {

  private sealed trait Query {
    def deadline: Deadline
    def withClient(c: a.ActorRef): Query
    def client: a.ActorRef
  }
  private final case class Get(deadline: Deadline, client: a.ActorRef = null) extends Query {
    def withClient(c: a.ActorRef) = copy(client = c)
  }
  private final case class Select(deadline: Deadline, predicate: PartialFunction[Any, Any], client: a.ActorRef = null) extends Query {
    def withClient(c: a.ActorRef) = copy(client = c)
  }
  private final case class StartWatch(target: a.ActorRef)
  private case object Kick

  private object Extension extends a.ExtensionId[Extension] with a.ExtensionIdProvider {
    override def lookup = Extension
    override def createExtension(system: a.ExtendedActorSystem): Extension = new Extension(system)
    override def get(system: a.ActorSystem): Extension = super.get(system)
  }

  private class Extension(val system: a.ExtendedActorSystem) extends akka.actor.Extension {

    lazy val config = system.settings.config.getConfig("akka.actor.dsl")

    val DSLDefaultTimeout = config.getMillisDuration("default-timeout")

    val DSLInboxQueueSize = config.getInt("inbox-size")

    val inboxNr = new AtomicInteger
    val inboxProps = a.Props(classOf[InboxActor], DSLInboxQueueSize)
  }

  private implicit val deadlineOrder: Ordering[Query] = new Ordering[Query] {
    def compare(left: Query, right: Query): Int = left.deadline.time compare right.deadline.time
  }

  private class InboxActor(size: Int) extends a.Actor with a.ActorLogging {
    var clients = Queue.empty[Query]
    val messages = Queue.empty[Any]
    var clientsByTimeout = TreeSet.empty[Query]
    var printedWarning = false

    def enqueueQuery(q: Query) {
      val query = q withClient sender()
      clients enqueue query
      clientsByTimeout += query
    }

    def enqueueMessage(msg: Any) {
      if (messages.size < size) messages enqueue msg
      else {
        if (!printedWarning) {
          log.warning("dropping message: either your program is buggy or you might want to increase akka.actor.dsl.inbox-size, current value is " + size)
          printedWarning = true
        }
      }
    }

    var currentMsg: Any = _
    val clientPredicate: (Query) ⇒ Boolean = {
      case _: Get          ⇒ true
      case Select(_, p, _) ⇒ p isDefinedAt currentMsg
      case _               ⇒ false
    }

    var currentSelect: Select = _
    val messagePredicate: (Any ⇒ Boolean) = (msg) ⇒ currentSelect.predicate.isDefinedAt(msg)

    var currentDeadline: Option[(Deadline, a.Cancellable)] = None

    def receive = ({
      case g: Get ⇒
        if (messages.isEmpty) enqueueQuery(g)
        else sender() ! wrap(messages.dequeue())
      case s @ Select(_, predicate, _) ⇒
        if (messages.isEmpty) enqueueQuery(s)
        else {
          currentSelect = s
          messages.dequeueFirst(messagePredicate) match {
            case Some(msg) ⇒ sender() ! wrap(msg)
            case None      ⇒ enqueueQuery(s)
          }
          currentSelect = null
        }
      case StartWatch(target) ⇒ context watch target
      case Kick ⇒
        val now = Deadline.now
        val pred = (q: Query) ⇒ q.deadline.time < now.time
        val overdue = clientsByTimeout.iterator.takeWhile(pred)
        while (overdue.hasNext) {
          val toKick = overdue.next()
          toKick.client ! Failure(new TimeoutException("deadline passed"))
        }
        // TODO: this wants to lose the `Queue.empty ++=` part when SI-6208 is fixed
        clients = Queue.empty ++= clients.filterNot(pred)
        clientsByTimeout = clientsByTimeout.from(Get(now))
      case msg ⇒
        if (clients.isEmpty) enqueueMessage(msg)
        else {
          currentMsg = msg
          clients.dequeueFirst(clientPredicate) match {
            case Some(q) ⇒ { clientsByTimeout -= q; q.client ! wrap(msg) }
            case None    ⇒ enqueueMessage(msg)
          }
          currentMsg = null
        }
    }: Receive) andThen { _ ⇒
      if (clients.isEmpty) {
        if (currentDeadline.isDefined) {
          currentDeadline.get._2.cancel()
          currentDeadline = None
        }
      } else {
        val next = clientsByTimeout.head.deadline
        import context.dispatcher
        if (currentDeadline.isEmpty) {
          currentDeadline = Some((next, context.system.scheduler.scheduleOnce(next.timeLeft, self, Kick)))
        } else {
          // must not rely on the Scheduler to not fire early (for robustness)
          currentDeadline.get._2.cancel()
          currentDeadline = Some((next, context.system.scheduler.scheduleOnce(next.timeLeft, self, Kick)))
        }
      }
    }

    private def wrap(msg: Any): Response[_] = msg match {
      case a.Terminated(ref) ⇒ Terminated(ActorRef(ref))
      case msg               ⇒ Message(msg)
    }
  }

  /*
   * make sure that AskTimeout does not accidentally mess up message reception
   * by adding this extra time to the real timeout
   */
  private val extraTime = 1.minute

  sealed trait Response[+T]
  case class Message[T](msg: T) extends Response[T]
  case class Terminated(ref: ActorRef[Nothing]) extends Response[Nothing]
  case class Failure(ex: Throwable) extends Response[Nothing]

  implicit class WithStackTrace[T](val f: Future[T]) extends AnyVal {
    def localTrace(implicit ec: ExecutionContext): Future[T] = {
      val ex = new RuntimeException
      val r: Throwable ⇒ T = { other ⇒
        throw new RuntimeException(other.getMessage(), other) {
          override def fillInStackTrace(): Throwable = {
            setStackTrace(ex.getStackTrace())
            this
          }
        }
      }
      f.recover(PartialFunction(r))
    }
  }

  /**
   * Create a new actor which will internally queue up messages it gets so that
   * they can be interrogated with the [[akka.actor.dsl.Inbox!.Inbox!.receive]]
   * and [[akka.actor.dsl.Inbox!.Inbox!.select]] methods. It will be created as
   * a system actor in the ActorSystem which is implicitly (or explicitly)
   * supplied.
   */
  def async[T](ctx: ActorContext[_], name: String): AsyncInbox[T] = new AsyncInbox[T](ctx, name)

  class AsyncInbox[T](ctx: ActorContext[_], name: String) {
    import ctx.executionContext

    private val receiver: a.ActorRef = ctx.actorOf(Extension(ctx.system.untyped).inboxProps, name)

    def ref: ActorRef[T] = ActorRef(receiver)

    private val defaultTimeout: FiniteDuration = Extension(ctx.system.untyped).DSLDefaultTimeout

    /**
     * Receive a single message from the internal `receiver` actor. The supplied
     * timeout is used for cleanup purposes and its precision is subject to the
     * resolution of the system’s scheduler (usually 100ms, but configurable).
     */
    def receive(timeout: FiniteDuration = defaultTimeout): Future[Response[T]] = {
      implicit val t = Timeout(timeout + extraTime)
      (receiver ? Get(Deadline.now + timeout)).asInstanceOf[Future[Response[T]]].localTrace
    }

    def receiveMsg(timeout: FiniteDuration = defaultTimeout): Future[T] =
      receive(timeout).collect { case Message(m) ⇒ m }.localTrace

    def receiveTerminated(timeout: FiniteDuration = defaultTimeout): Future[Terminated] =
      receive(timeout).collect { case t: Terminated ⇒ t }.localTrace

    /**
     * Receive a single message for which the given partial function is defined
     * and return the transformed result, using the internal `receiver` actor.
     * The supplied timeout is used for cleanup purposes and its precision is
     * subject to the resolution of the system’s scheduler (usually 100ms, but
     * configurable).
     *
     * <b>Warning:</b> This method blocks the current thread until a message is
     * received, thus it can introduce dead-locks (directly as well as
     * indirectly by causing starvation of the thread pool). <b>Do not use
     * this method within an actor!</b>
     */
    def select[U](timeout: FiniteDuration = defaultTimeout)(predicate: PartialFunction[T, U]): U = {
      implicit val t = Timeout(timeout + extraTime)
      predicate(Await.result(
        receiver ? Select(Deadline.now + timeout, predicate.asInstanceOf[PartialFunction[Any, Any]]),
        Duration.Inf).asInstanceOf[T])
    }

    /**
     * Make the inbox’s actor watch the target actor such that reception of the
     * Terminated message can then be awaited.
     */
    def watch(target: ActorRef[_]): Unit = receiver ! StartWatch(target.ref)

    def stop(): Unit = ctx.stop(receiver.path.name)
  }

  def sync[T](name: String): SyncInbox[T] = new SyncInbox(name)

  class SyncInbox[T](name: String) {
    private val q = new ConcurrentLinkedQueue[T]
    private val r = new akka.actor.MinimalActorRef {
      override def provider: ActorRefProvider = ???
      override val path: ActorPath = RootActorPath(Address("akka", "SyncInbox")) / name
      override def !(msg: Any)(implicit sender: akka.actor.ActorRef) = q.offer(msg.asInstanceOf[T])
    }

    val ref: ActorRef[T] = ActorRef(r)
    def receiveMsg(): T = q.poll() match {
      case null ⇒ throw new NoSuchElementException(s"polling on an empty inbox: $name")
      case x    ⇒ x
    }
    def receiveAll(): immutable.Seq[T] = {
      @tailrec def rec(acc: List[T]): List[T] = q.poll() match {
        case null ⇒ acc.reverse
        case x    ⇒ rec(x :: acc)
      }
      rec(Nil)
    }
    def hasMessages: Boolean = q.peek() != null
  }
}
