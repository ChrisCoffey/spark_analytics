import sbt.Keys.compile
import sbt.Keys.test
import sbt._
import sbt.Keys._

object SparkFun extends Build {

  val sparkCore = "org.apache.spark" %% "spark-core" % "1.3.1"

  val coreDependencies = Seq(sparkCore)

  val coreSettings = Defaults.coreDefaultSettings ++ Seq(
    organization := "ccoffey",
    scalaVersion := "2.11.5",
    libraryDependencies ++= coreDependencies
  )

  lazy val root = Project(
    id="sparkDemo",
    base=file("."),
    settings = coreSettings
  ).aggregate (
    chapter2
  )

  lazy val chapter2 = Project(
    id="intro",
    base=file("./chapter2_deduplication"),
    settings = coreSettings
  )

}
