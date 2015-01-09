/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import akka.actor.Deploy
import akka.routing.RouterConfig
import scala.reflect.{ ClassTag, classTag }

/**
 * Props describe how to dress up a [[Behavior]] so that it can become an Actor.
 */
case class Props[T](creator: () ⇒ Behavior[T], deploy: Deploy)(implicit val tag: ClassTag[T]) {
  def withDispatcher(d: String) = copy(deploy = deploy.copy(dispatcher = d))
  def withMailbox(m: String) = copy(deploy = deploy.copy(mailbox = m))
  def withRouter(r: RouterConfig) = copy(deploy = deploy.copy(routerConfig = r))
  def withDeploy(d: Deploy) = copy(deploy = d)
}

/**
 * Props describe how to dress up a [[Behavior]] so that it can become an Actor.
 */
object Props {
  /**
   * Create a Props instance from a block of code that creates a [[Behavior]].
   */
  def apply[T: ClassTag](block: ⇒ Behavior[T]): Props[T] = Props(() ⇒ block, akka.actor.Props.defaultDeploy)

  /**
   * Props for a Behavior that just ignores all messages.
   */
  def empty[T]: Props[T] = EMPTY.asInstanceOf[Props[T]]
  private val EMPTY: Props[Any] = Props(Behavior.Static[Any] { case _ ⇒ Behavior.Unhandled })

  /**
   * INTERNAL API.
   */
  private[typed] def untyped[T](p: Props[T]): akka.actor.Props =
    new akka.actor.Props(p.deploy, classOf[ActorAdapter[_]], p.creator :: p.tag :: Nil)

  /**
   * INTERNAL API.
   */
  private[typed] def apply[T](p: akka.actor.Props): Props[T] = {
    assert(p.clazz == classOf[ActorAdapter[_]], "typed.Actor must have typed.Props")
    p.args match {
      case (creator: Function0[_]) :: (tag: ClassTag[T]) :: Nil ⇒
        Props(creator.asInstanceOf[Function0[Behavior[T]]], p.deploy)(tag)
      case _ ⇒ throw new AssertionError("typed.Actor args must be right")
    }
  }
}
