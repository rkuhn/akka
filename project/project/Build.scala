import sbt._
import Keys._

object B extends Build {
  lazy val p = Project(id = "main", base = file(".")) dependsOn s
  lazy val s = file("/Users/rkuhn/comp/schoir")
}
