import sbtrelease.ReleaseStateTransformations._

lazy val commonSettings = Seq(
  name := "fs2-hive-stream",
  scalaVersion := "2.12.8",
  organization := "io.pascals.fs2",
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.11.11", "2.12.8"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-Ypartial-unification", "-feature", "-language:higherKinds"),
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full)
)

lazy val root = (project in file(".")).
  settings(moduleName := "" +
    "hive-stream").
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.0.8" % Test,
      "org.scalatest" %% "scalatest" % "3.0.8" % Test,
      "co.fs2" %% "fs2-core" % "2.0.1",
      "org.apache.hive" % "hive-streaming" % "3.1.0"
        exclude ("org.slf4j", "slf4j-api")
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("log4j", "log4j")
        exclude("org.apache.logging.log4j", "log4j-slf4j-impl"),
      "org.apache.hive.hcatalog" % "hive-hcatalog-core" % "3.1.0"
        exclude ("org.slf4j", "slf4j-api")
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("log4j", "log4j")
        exclude("org.apache.logging.log4j", "log4j-slf4j-impl"),
      "io.circe" %% "circe-core" %  "0.11.1",
      "io.circe" %% "circe-generic" %  "0.11.1",
      "io.circe" %% "circe-parser" %  "0.11.1"
    )
  )

publishMavenStyle := true
publishArtifact in Test := false
publishTo := {
  val nexus = "$nexus_repository_here"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "content/repositories/releases")
}
pomIncludeRepository := { _ => false }

releaseTagComment    := s"* Test Releasing ${(version in ThisBuild).value} [skip ci]"

releaseCommitMessage := s"* Test Setting version to ${(version in ThisBuild).value} [skip ci]"

val runUnitTests = ReleaseStep(
  action = Command.process("testOnly * -- -l \"io.pascals.fs2.hive.tags.IntegrationTest\"", _),
  enableCrossBuild = true
)

val runIntegrationTests = ReleaseStep(
  action = Command.process("testOnly * -- -n \"io.pascals.fs2.hive.tags.IntegrationTest\"", _),
  enableCrossBuild = true
)

val publishJar = ReleaseStep(action = Command.process("publish", _), enableCrossBuild = true)

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runUnitTests,
  setReleaseVersion,
  publishJar
)
