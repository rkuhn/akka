package akka.typed

import akka.actor.ActorPath

trait ActorRef[-T] {
  def !(msg: T): Unit
  def tell(msg: T): Unit = this ! msg
  def path: ActorPath
}