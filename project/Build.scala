import sbt.Keys.compile
import sbt.Keys.test
import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys._
import sbtassembly.MergeStrategy

object SparkFun extends Build {

  val sparkCore = ("org.apache.spark" %% "spark-core"       % "1.3.1").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog")
  val sparkML   = ("org.apache.spark" %% "spark-mllib" % "1.3.1")
    .exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog")

  val coreDependencies = Seq(
    sparkCore,
    sparkML)

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
    chapter2,
    chapter3
  )

  lazy val chapter2 = Project(
    id="intro",
    base=file("./chapter2_deduplication"),
    settings = coreSettings
  )

  lazy val chapter3 = Project(
    id="recomender",
    base=file("./chapter3_recomender"),
    settings = coreSettings
  )

}
