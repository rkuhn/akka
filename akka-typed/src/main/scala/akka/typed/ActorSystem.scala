/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
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
import akka.actor.ActorSystemImpl
import com.typesafe.config.Config
import akka.actor.ExtendedActorSystem
import com.typesafe.config.ConfigFactory

/**
 */
abstract class ActorSystem[-T](val name: String) extends ActorRef[T] { this: ScalaActorRef[T] ⇒

  val untyped: ExtendedActorSystem

  lazy val ref = untyped.provider.guardian

  def deadLetters[T]: ActorRef[T] = ActorRef(untyped.deadLetters)
}

object ActorSystem {
  private class Impl[T](_name: String, _config: Config, _cl: ClassLoader, _ec: Option[ExecutionContext], _p: Props[T])
    extends ActorSystem[T](_name) with ScalaActorRef[T] {
    val untyped: ExtendedActorSystem = new ActorSystemImpl(_name, _config, _cl, _ec, Some(Props.untyped(_p))).start()
  }

  private class Wrapper(val untyped: ExtendedActorSystem) extends ActorSystem[Nothing](untyped.name) with ScalaActorRef[Nothing]

  def apply[T](name: String, guardianProps: Props[T],
               config: Option[Config] = None,
               classLoader: Option[ClassLoader] = None,
               executionContext: Option[ExecutionContext] = None): ActorSystem[T] = {
    val cl = classLoader.getOrElse(akka.actor.ActorSystem.findClassLoader())
    val appConfig = config.getOrElse(ConfigFactory.load(cl))
    new Impl(name, appConfig, cl, executionContext, guardianProps)
  }

  def apply(untyped: akka.actor.ActorSystem): ActorSystem[Nothing] = new Wrapper(untyped.asInstanceOf[ExtendedActorSystem])
}