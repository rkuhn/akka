/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.typed

import akka.actor.Deploy
import akka.routing.RouterConfig
import scala.reflect.ClassTag

case class Props[T](creator: () ⇒ Behavior[T], deploy: Deploy)(implicit val tag: ClassTag[T]) {
  def withDispatcher(d: String) = copy(deploy = deploy.copy(dispatcher = d))
  def withMailbox(m: String) = copy(deploy = deploy.copy(mailbox = m))
  def withRouter(r: RouterConfig) = copy(deploy = deploy.copy(routerConfig = r))
  def withDeploy(d: Deploy) = copy(deploy = d)
}

object Props {
  def apply[T: ClassTag](block: ⇒ Behavior[T]): Props[T] = Props(() ⇒ block, akka.actor.Props.defaultDeploy)

  private[typed] def untyped[T](p: Props[T]): akka.actor.Props =
    new akka.actor.Props(p.deploy, classOf[ActorAdapter[_]], p.creator :: p.tag :: Nil)

  private[typed] def apply[T](p: akka.actor.Props): Props[T] = {
    assert(p.clazz == classOf[ActorAdapter[_]], "typed.Actor must have typed.Props")
    p.args match {
      case (creator: Function0[Behavior[T]]) :: (tag: ClassTag[T]) :: Nil ⇒
        Props(creator, p.deploy)(tag)
      case _ ⇒ throw new AssertionError("typed.Actor args must be right")
    }
  }
}
