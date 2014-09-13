package akka.typed

import akka.event.EventStream
import akka.actor.Scheduler
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executor
import scala.concurrent.duration.Duration
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ActorRefProvider
import java.util.concurrent.ThreadFactory
import akka.actor.DynamicAccess

/**
 * Eventually there will be a new kind of ActorSystem that is started from a
 * configurable guardian (i.e. from a Props[T]). This is not yet done.
 *
 * TODO
 */
//trait ActorSystem[-T] extends ActorRef[T] {
//  def name: String
//  def settings: akka.actor.ActorSystem.Settings
//  def untyped: akka.actor.ActorSystem
//  def logConfiguration(): Unit
//  def startTime: Long
//  def uptime: Long
//  def eventStream: EventStream
//  def deadLetters: akka.actor.ActorRef
//  def scheduler: Scheduler
//  implicit def dispatcher: ExecutionContext with Executor
//  def registerOnTermination[T](code: => T): Unit
//  def registerOnTermination(runnable: Runnable): Unit
//  def awaitTermination(timeout: Duration): Unit
//  def awaitTermination(): Unit
//  def isTerminated: Boolean
//  def shutdown(): Unit
//  def registerExtension[T <: Extension](ext: ExtensionId[T]): T
//  def extension[T <: Extension](ext: ExtensionId[T]): T
//  def hasExtension(ext: ExtensionId[_ <: Extension]): Boolean
//}
//
//trait ExtendedActorSystem[-T] extends ActorSystem[T] {
//  def provider: ActorRefProvider
//  def threadFactory: ThreadFactory
//  def dynamicAccess: DynamicAccess
//}
