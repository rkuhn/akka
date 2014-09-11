package akka.typed

import akka.actor.ActorSystem

object Ops {

  implicit class ActorSystemOps(val sys: ActorSystem) extends AnyVal {
    def spawn[T](props: Props[T]): ActorRef[T] =
      new ActorRef(sys.actorOf(Props.untyped(props)))
    def spawn[T](props: Props[T], name: String): ActorRef[T] =
      new ActorRef(sys.actorOf(Props.untyped(props), name))
  }

  implicit class ActorContextOps(val ctx: akka.actor.ActorContext) extends AnyVal {
    def spawn[T](props: Props[T]): ActorRef[T] =
      new ActorRef(ctx.actorOf(Props.untyped(props)))
    def spawn[T](props: Props[T], name: String): ActorRef[T] =
      new ActorRef(ctx.actorOf(Props.untyped(props), name))
  }

}