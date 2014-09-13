package akka.typed

import scala.reflect.ClassTag
import akka.actor.OneForOneStrategy
import scala.concurrent.duration.Duration

private[typed] class ActorAdapter[T: ClassTag](_initialBehavior: () ⇒ Behavior[T]) extends akka.actor.Actor {
  import Behavior._

  val clazz = implicitly[ClassTag[T]].runtimeClass

  var behavior = _initialBehavior()
  val ctx = new ActorContextAdapter[T](context)

  def receive = {
    case akka.actor.Terminated(ref)   ⇒ next(behavior.management(ctx, Terminated(new ActorRef(ref))))
    case akka.actor.ReceiveTimeout    ⇒ next(behavior.management(ctx, ReceiveTimeout))
    case msg if clazz.isInstance(msg) ⇒ next(behavior.message(ctx, msg.asInstanceOf[T]))
  }

  private def next(b: Behavior[T]): Unit =
    if (b == sameBehavior) {}
    else if (b == stoppedBehavior) {
      context.stop(self)
    } else behavior = b

  override val supervisorStrategy = OneForOneStrategy() {
    case ex ⇒
      import Failure._
      import akka.actor.{ SupervisorStrategy ⇒ s }
      val b = behavior.management(ctx, Failure(ex, new ActorRef(sender())))
      next(unwrap(b))
      b match {
        case Resume(_)  ⇒ s.Resume
        case Restart(_) ⇒ s.Restart
        case Stop(_)    ⇒ s.Stop
        case _          ⇒ s.Escalate
      }
  }

  override def preStart(): Unit =
    next(behavior.management(ctx, PreStart))
  override def preRestart(reason: Throwable, message: Option[Any]): Unit =
    next(behavior.management(ctx, PreRestart(reason)))
  override def postRestart(reason: Throwable): Unit =
    next(behavior.management(ctx, PreRestart(reason)))
  override def postStop(): Unit =
    next(behavior.management(ctx, PostStop))
}

private[typed] class ActorContextAdapter[T](ctx: akka.actor.ActorContext) extends ActorContext[T] {
  import Ops._
  def self = new ActorRef(ctx.self)
  def props = Props(ctx.props)
  def system = ctx.system
  def children = ctx.children.map(new ActorRef(_))
  def child(name: String) = ctx.child(name).map(new ActorRef(_))
  def spawn[U](props: Props[U]) = ctx.spawn(props)
  def spawn[U](props: Props[U], name: String) = ctx.spawn(props, name)
  def stop(name: String) = ctx.child(name) foreach (ctx.stop)
  def watch[U](other: ActorRef[U]) = { ctx.watch(other.ref); other }
  def watch(other: akka.actor.ActorRef) = { ctx.watch(other); other }
  def unwatch[U](other: ActorRef[U]) = { ctx.unwatch(other.ref); other }
  def unwatch(other: akka.actor.ActorRef) = { ctx.unwatch(other); other }
  def setReceiveTimeout(d: Duration) = ctx.setReceiveTimeout(d)
  def createWrapper[U](f: U ⇒ T) = new ActorRef[U](ctx.actorOf(akka.actor.Props(classOf[MessageWrapper], f)))
}

private[typed] class MessageWrapper(f: Any ⇒ Any) extends akka.actor.Actor {
  def receive = {
    case msg ⇒ context.parent ! f(msg)
  }
}
