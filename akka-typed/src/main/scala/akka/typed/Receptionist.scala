package akka.typed

import Behavior._

object Receptionist {

  trait ServiceKey[T]

  sealed trait Command
  case class Register[T](key: ServiceKey[T], address: ActorRef[T])(implicit val replyTo: ActorRef[Registered[T]]) extends Command
  case class Find[T](key: ServiceKey[T])(implicit val replyTo: ActorRef[Listing[T]]) extends Command

  case class Registered[T](key: ServiceKey[T], address: ActorRef[T])
  case class Listing[T](key: ServiceKey[T], addresses: Set[ActorRef[T]])

  /*
   * These wrappers are just there to get around the type madness that would otherwise ensue
   * by declaring a Map[ServiceKey[_], Set[ActorRef[_]]] (and actually trying to use it).
   */
  private class Key(val key: ServiceKey[_]) {
    override def equals(other: Any) = other match {
      case k: Key ⇒ key == k.key
      case _      ⇒ false
    }
    override def hashCode = key.hashCode
  }
  private object Key {
    def apply(r: Register[_]) = new Key(r.key)
    def apply(f: Find[_]) = new Key(f.key)
  }

  private class Address(val address: ActorRef[_]) {
    def extract[T]: ActorRef[T] = address.asInstanceOf[ActorRef[T]]
    override def equals(other: Any) = other match {
      case a: Address ⇒ address == a.address
      case _          ⇒ false
    }
    override def hashCode = address.hashCode
  }
  private object Address {
    def apply(r: Register[_]) = new Address(r.address)
    def apply(r: ActorRef[_]) = new Address(r)
  }

  val receptionist: Behavior[Command] = behavior(Map.empty)

  private def behavior(map: Map[Key, Set[Address]]): Behavior[Command] = Full {
    case (ctx, Right(r: Register[t])) ⇒
      ctx.watch(r.address)
      val key = Key(r)
      val set = map get key match {
        case Some(old) ⇒ old + Address(r)
        case None      ⇒ Set(Address(r))
      }
      r.replyTo ! Registered(r.key, r.address)
      behavior(map.updated(key, set))
    case (ctx, Right(f: Find[t])) ⇒
      val set = map get Key(f) getOrElse Set.empty
      f.replyTo ! Listing(f.key, set.map(_.extract[t]))
      Same
    case (ctx, Left(Terminated(ref))) ⇒
      val addr = Address(ref)
      // this is not at all optimized
      behavior(map.map { case (k, v) ⇒ k -> (v - addr) })
  }
}
