/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.impl

private[akka] trait StreamProcessingLayout {
  def layout: StreamLayout.Layout
}

private[akka] trait StreamProcessingModule extends StreamProcessingLayout {
  abstract override def layout: StreamLayout.Module
}
