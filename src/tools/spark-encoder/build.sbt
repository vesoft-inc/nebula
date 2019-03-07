import Commons._

organization := "com.vesoft"
// compatible with spark 1.6.2
scalaVersion := "2.11.8"
name := "nebula-spark-sstfile-generator"
version := "1.0-SNAPSHOT"

test in assembly in ThisBuild := {}
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

//not include src and doc when dist
sources in(Compile, doc) in ThisBuild := Seq.empty
publishArtifact in(Compile, packageDoc) in ThisBuild := false
sources in (packageSrc) := Seq.empty


libraryDependencies ++= commonDependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.2" % "provided",
  "org.apache.spark" %% "spark-yarn" % "1.6.2" % "provided",
  "com.databricks" %% "spark-csv" % "1.5.0" % "provided",

  //cmd line parsing
  "commons-cli" % "commons-cli" % "1.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "org.apache.spark" %% "spark-hive" % "1.6.2" % "provided",

  // json parsing
  "com.typesafe.play" %% "play-json" % "2.7.1",
  "joda-time" % "joda-time" % "2.10.1",

//need nebula native client for encoding, need to run mvn install to deploy to local repo before used
  "org.rocksdb" % "rocksdbjni" % "5.17.2",
  "com.vesoft" % "native-client" % "1.0-SNAPSHOT"
)

//CAUTION: when dependency with version of X-SNAPSHOT is updated, you should comment out the following line, and run sbt update
updateOptions := updateOptions.value.withLatestSnapshots(false)

assemblyMergeStrategy in assembly := {
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.discard
  case PathList("org", "apache", "logging", xs@_*) => MergeStrategy.discard
  case PathList("ch", "qos", "logback", xs@_*) => MergeStrategy.discard
  case PathList("org", "scalactic", xs@_*) => MergeStrategy.discard
  case x => val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.commons.cli.**" -> "shadecli.org.apache.commons.cli.@1").inAll
)

assemblyJarName in assembly := s"${name}.jar"

test in assembly := {}
// should not include scala runtime when submitting spark job
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// TODO: for local deployment to artifactory, should be removed in PR
publishTo in ThisBuild := Some("Artifactory Realm" at "http://localhost:8081/artifactory/sbt-dev")
val artifactoryCredentials = Credentials("Artifactory Realm", "localhost", "admin", "admin123")
credentials += artifactoryCredentials

publishTo in ThisBuild := Some("Artifactory Realm" at "http://localhost:8081/artifactory/sbt-dev;build.timestamp=" + new java.util.Date().getTime)
credentials += artifactoryCredentials




