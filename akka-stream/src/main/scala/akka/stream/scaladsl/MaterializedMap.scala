/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream.impl.{ PublisherSink, SubscriberSource, StreamLayout }

/**
 * Common trait for things that have a MaterializedType.
 */
trait Materializable {
  type MaterializedType
  //
  //  /**
  //   * Every materializable element must be backed by a stream layout module
  //   */
  //  private[stream] def module: StreamLayout.Module = ??? // FIXME make abstract
}
