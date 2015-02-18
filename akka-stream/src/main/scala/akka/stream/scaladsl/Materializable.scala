/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream.impl.{ PublisherSink, SubscriberSource, StreamLayout }

/**
 * Convenience functions for often-encountered purposes like keeping only the
 * left (first) or only the right (second) of two input values.
 */
object Keep {
  private val _left = (l: Any, r: Any) ⇒ l
  private val _right = (l: Any, r: Any) ⇒ r
  private val _both = (l: Any, r: Any) ⇒ (l, r)

  def left[L, R]: (L, R) ⇒ L = _left.asInstanceOf[(L, R) ⇒ L]
  def right[L, R]: (L, R) ⇒ R = _right.asInstanceOf[(L, R) ⇒ R]
  def both[L, R]: (L, R) ⇒ (L, R) = _both.asInstanceOf[(L, R) ⇒ (L, R)]
}

/**
 * Common trait for things that have a MaterializedType.
 */
trait Materializable {
  type MaterializedType

  /**
   * Every materializable element must be backed by a stream layout module
   */
  private[stream] def module: StreamLayout.Module
}