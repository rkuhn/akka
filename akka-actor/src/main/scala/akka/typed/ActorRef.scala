package akka.typed

import akka.actor.ActorPath

class ActorRef[-T](val ref: akka.actor.ActorRef) {
  def !(msg: T): Unit = ref ! msg
  def tell(msg: T): Unit = ref ! msg
  def path: ActorPath = ref.path
}