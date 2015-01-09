package akka.util

import java.io.DataInputStream
import java.io.BufferedInputStream
import scala.annotation.tailrec

object LineNumbers {
  sealed trait Result
  case object NoSourceInfo extends Result
  case class SourceFile(filename: String) extends Result {
    override def toString = filename
  }
  case class SourceFileLines(filename: String, from: Int, to: Int) extends Result {
    override def toString = s"$filename:$from-$to"
  }

  private class Constants(val fwd: Map[Int, String], val rev: Map[String, Int]) {
    def this() = this(Map.empty, Map.empty)
    def apply(idx: Int): String = fwd(idx)
    def apply(str: String): Int = rev(str)
    def updated(idx: Int, str: String): Constants =
      if (rev contains str) new Constants(fwd.updated(idx, str), rev)
      else new Constants(fwd.updated(idx, str), rev.updated(str, idx))
    def contains(str: String): Boolean = rev contains str
  }

  final val debug = false
}

class LineNumbers {
  import LineNumbers._

  // FIXME: this needs memoization with an LRU cache
  def forClass(c: Class[_]): Result = {
    val resource = c.getName.replace('.', '/') + ".class"
    val stream = c.getClassLoader.getResourceAsStream(resource)
    val dis = new DataInputStream(stream)

    try {
      skipID(dis)
      skipVersion(dis)
      implicit val constants = getConstants(dis)
      dbg(s"  fwd(${constants.fwd.size}) rev(${constants.rev.size}) ${constants.fwd.keys.toList.sorted}")
      skipClassInfo(dis)
      skipInterfaceInfo(dis)
      skipFields(dis)
      val lines = readMethods(dis)
      val source = readAttributes(dis)

      if (source.isEmpty) NoSourceInfo
      else lines match {
        case None             ⇒ SourceFile(source.get)
        case Some((from, to)) ⇒ SourceFileLines(source.get, from, to)
      }

    } finally dis.close()
  }

  private def skipID(d: DataInputStream): Unit = {
    val magic = d.readInt()
    dbg(f"magic=0x$magic%08X")
    if (magic != 0xcafebabe) throw new IllegalArgumentException("not a Java class file")
  }

  private def skipVersion(d: DataInputStream): Unit = {
    val minor = d.readShort()
    val major = d.readShort()
    dbg(s"version=$major:$minor")
  }

  private def getConstants(d: DataInputStream): Constants = {
    val count = d.readUnsignedShort()
    dbg(s"reading $count constants")
    @tailrec def rec(index: Int, acc: Constants): Constants =
      if (index >= count) acc
      else {
        readConstant(d, acc) match {
          case (jump, Some(str)) ⇒ rec(index + jump, acc.updated(index, str))
          case (jump, None)      ⇒ rec(index + jump, acc)
        }
      }
    rec(1, new Constants)
  }

  private def readConstant(d: DataInputStream, c: Constants): (Int, Option[String]) = {
    d.readByte() match {
      case 1 ⇒ // Utf8
        1 -> Some(d.readUTF())
      case 3 ⇒ // Integer
        d.readInt()
        1 -> None
      case 4 ⇒ // Float
        d.readFloat()
        1 -> None
      case 5 ⇒ // Long
        d.readLong()
        2 -> None
      case 6 ⇒ // Double
        d.readDouble()
        2 -> None
      case 7 ⇒ // Class
        1 -> Some(c(d.readUnsignedShort()))
      case 8 ⇒ // String
        d.readUnsignedShort()
        1 -> None
      case 9 ⇒ // FieldRef
        d.readUnsignedShort()
        d.readUnsignedShort()
        1 -> None
      case 10 ⇒ // MethodRef
        d.readUnsignedShort()
        d.readUnsignedShort()
        1 -> None
      case 11 ⇒ // InterfaceMethodRef
        d.readUnsignedShort()
        d.readUnsignedShort()
        1 -> None
      case 12 ⇒ // NameAndType
        d.readUnsignedShort()
        d.readUnsignedShort()
        1 -> None
      case 15 ⇒ // MethodHandle
        d.readUnsignedByte()
        d.readUnsignedShort()
        1 -> None
      case 16 ⇒ // MethodType
        d.readUnsignedShort()
        1 -> None
      case 18 ⇒ // InvokeDynamic
        d.readUnsignedShort()
        d.readUnsignedShort()
        1 -> None
    }
  }

  private def skipClassInfo(d: DataInputStream)(implicit c: Constants): Unit = {
    d.readUnsignedShort() // access flags
    val name = d.readUnsignedShort() // class name
    d.readUnsignedShort() // superclass name
    dbg(s"class name = ${c(name)}")
  }

  private def skipInterfaceInfo(d: DataInputStream)(implicit c: Constants): Unit = {
    val count = d.readUnsignedShort()
    for (_ ← 1 to count) {
      val intf = d.readUnsignedShort()
      dbg(s"  implements ${c(intf)}")
    }
  }

  private def skipFields(d: DataInputStream)(implicit c: Constants): Unit = {
    val count = d.readUnsignedShort()
    dbg(s"reading $count fields:")
    for (_ ← 1 to count) skipMethodOrField(d)
  }

  private def skipMethodOrField(d: DataInputStream)(implicit c: Constants): Unit = {
    d.readUnsignedShort() // access flags
    val name = d.readUnsignedShort() // name
    d.readUnsignedShort() // signature
    val attributes = d.readUnsignedShort()
    for (_ ← 1 to attributes) skipAttribute(d)
    dbg(s"  ${c(name)} ($attributes attributes)")
  }

  private def skipAttribute(d: DataInputStream): Unit = {
    d.readUnsignedShort() // tag
    val length = d.readInt()
    skip(d, length)
  }

  private def readMethods(d: DataInputStream)(implicit c: Constants): Option[(Int, Int)] = {
    val count = d.readUnsignedShort()
    dbg(s"reading $count methods")
    if (c.contains("Code") && c.contains("LineNumberTable")) {
      (1 to count).map(_ ⇒ readMethod(d, c("Code"), c("LineNumberTable"))).flatten.foldLeft(Int.MaxValue -> 0) {
        case ((low, high), (start, end)) ⇒ (Math.min(low, start), Math.max(high, end))
      } match {
        case (Int.MaxValue, 0) ⇒ None
        case other             ⇒ Some(other)
      }
    } else {
      dbg(s"  (skipped)")
      for (_ ← 1 to count) skipMethodOrField(d)
      None
    }
  }

  private def readMethod(d: DataInputStream, code: Int, lnt: Int)(implicit c: Constants): Option[(Int, Int)] = {
    d.readUnsignedShort() // access flags
    val name = d.readUnsignedShort() // name
    d.readUnsignedShort() // signature
    dbg(s"  ${c(name)}")
    val attributes =
      for (_ ← 1 to d.readUnsignedShort()) yield {
        val tag = d.readUnsignedShort()
        val length = d.readInt()
        if (tag != code) {
          skip(d, length)
          None
        } else {
          d.readUnsignedShort() // max stack
          d.readUnsignedShort() // max locals
          skip(d, d.readInt()) // skip byte-code
          for (_ ← 1 to d.readUnsignedShort()) { // skip exception table
            d.readUnsignedShort() // start PC
            d.readUnsignedShort() // end PC
            d.readUnsignedShort() // handler PC
            d.readUnsignedShort() // catch type
          }
          val possibleLines =
            for (_ ← 1 to d.readUnsignedShort()) yield {
              val tag = d.readUnsignedShort()
              val length = d.readInt()
              if (tag != lnt) {
                skip(d, length)
                None
              } else {
                val lines =
                  for (_ ← 1 to d.readUnsignedShort()) yield {
                    d.readUnsignedShort() // start PC
                    d.readUnsignedShort() // finally: the line number
                  }
                Some(lines.min -> lines.max)
              }
            }
          dbg(s"    nested attributes yielded: $possibleLines")
          possibleLines.flatten.headOption
        }
      }
    attributes.flatten.headOption
  }

  private def readAttributes(d: DataInputStream)(implicit c: Constants): Option[String] = {
    val count = d.readUnsignedShort()
    dbg(s"reading $count attributes")
    if (c contains "SourceFile") {
      val s = c("SourceFile")
      val attributes =
        for (_ ← 1 to count) yield {
          val tag = d.readUnsignedShort()
          val length = d.readInt()
          dbg(s"  tag ${c(tag)} ($length bytes)")
          if (tag != s) {
            skip(d, length)
            None
          } else {
            val name = d.readUnsignedShort()
            Some(c(name))
          }
        }
      dbg(s"  yielded $attributes")
      attributes.flatten.headOption
    } else {
      dbg(s"  (skipped)")
      None
    }
  }

  private def skip(d: DataInputStream, length: Int): Unit =
    if (d.skipBytes(length) != length) throw new IllegalArgumentException("class file ends prematurely")

  private def dbg(s: String): Unit = if (debug) println("LNB: " + s)
}
