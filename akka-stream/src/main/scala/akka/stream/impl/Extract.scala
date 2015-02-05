/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

import akka.stream.scaladsl

/**
 * INTERNAL API
 *
 * Unapply methods aware of both DSLs.
 * Use these instead of manually casting to [[scaladsl.Source]].
 */
private[akka] object Extract {

  object Source {
    def unapply(a: Any): Option[scaladsl.Source[Any, _]] = a match {
      case s: scaladsl.Source[Any, _] ⇒ Some(s)
      case _                          ⇒ None
    }
  }

  object Sink {
    def unapply(a: Any): Option[scaladsl.Sink[Any, _]] = a match {
      case s: scaladsl.Sink[Any, _] ⇒ Some(s)
      case _                        ⇒ None
    }
  }

}
