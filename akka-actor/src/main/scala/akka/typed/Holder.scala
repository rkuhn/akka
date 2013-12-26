package akka.typed

/**
 * This class is meant to be subclassed to introduce a type-safe wrapper into
 * the type hierarchy accepted by an actor which includes (part of) the protocol
 * of another actor into it.
 */
abstract class Holder[T](val value: T) {
  override def toString = s"Holder($value)"
  override def equals(other: Any) = other match {
    case h: Holder[_] => value equals h.value
  }
  override def hashCode = value.hashCode
}
