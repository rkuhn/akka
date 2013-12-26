package akka.typed

import akka.actor.ActorPath

trait ActorRef[-T] {
  def !(msg: T): Unit
  def path: ActorPath
}