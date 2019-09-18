import sbt.Keys._
import sbt._

object Commons {

  lazy val commonDependencies = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.slf4j"      % "slf4j-api"       % "1.7.25",
    "org.scalactic"  %% "scalactic"      % "3.0.4",
    "org.scalatest"  %% "scalatest"      % "3.0.4" % Test
  )

  scalacOptions in Compile ++= Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlog-reflective-calls",
    "-Xlint"
  )

  scalacOptions in Test ++= Seq("-Yrangepos")
}
