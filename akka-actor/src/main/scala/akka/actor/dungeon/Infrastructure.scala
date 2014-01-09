package akka.actor.dungeon

import scala.util.control.NonFatal
import akka.actor.Cell
import akka.event.Logging.LogEvent

trait Infrastructure { this: Cell =>

  // logging is not the main purpose, and if it fails there’s nothing we can do
  protected final def publish(e: LogEvent): Unit = try system.eventStream.publish(e) catch { case NonFatal(_) ⇒ }

  protected final def clazz(o: AnyRef): Class[_] = if (o eq null) this.getClass else o.getClass

}