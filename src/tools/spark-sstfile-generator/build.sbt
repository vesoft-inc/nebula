import Commons._

organization := "com.vesoft"
// compatible with spark 1.6.0
scalaVersion := "2.10.5"

// compatible with spark 1.6.2
// scalaVersion := "2.11.8"

name := "nebula-spark-sstfile-generator"
version := "1.0.0-beta"

scalaSource in Compile := baseDirectory.value / "src/main/scala/com/vesoft/tools"
test in assembly in ThisBuild := {}

//not include src and doc when dist
sources in (Compile, doc) in ThisBuild := Seq.empty
publishArtifact in (Compile, packageDoc) in ThisBuild := false
sources in (packageSrc) := Seq.empty

libraryDependencies ++= commonDependencies
libraryDependencies ++= Seq(
  // compatible with spark 1.6.2
  //  "org.apache.spark" %% "spark-core" % "1.6.2" % "provided",
  //  "org.apache.spark" %% "spark-sql" % "1.6.2" % "provided",
  //  "org.apache.spark" %% "spark-yarn" % "1.6.2" % "provided",
  //  "com.databricks" %% "spark-csv" % "1.5.0" % "provided",
  //  "org.apache.spark" %% "spark-hive" % "1.6.2" % "provided",

  // compatible with spark 1.6.0 & hadoop 2.6.0
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
    exclude ("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql"  % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-yarn" % "1.6.0" % "provided"
    exclude ("org.apache.hadoop", "hadoop-yarn-api")
    exclude ("org.apache.hadoop", "hadoop-yarn-common")
    exclude ("org.apache.hadoop", "hadoop-client"),
  "com.databricks"    %% "spark-csv"         % "1.5.0" % "provided",
  "org.apache.spark"  %% "spark-hive"        % "1.6.0" % "provided",
  "org.apache.hadoop" % "hadoop-common"      % "2.6.0" % "provided",
  "org.apache.hadoop" % "hadoop-client"      % "2.6.0" % "provided",
  "org.apache.hadoop" % "hadoop-yarn-api"    % "2.6.0" % "provided",
  "org.apache.hadoop" % "hadoop-yarn-common" % "2.6.0" % "provided",
  //cmd line parsing
  "commons-cli"   % "commons-cli" % "1.4",
  "org.scalatest" %% "scalatest"  % "3.0.4" % Test,
  // json parsing
  "com.typesafe.play" %% "play-json" % "2.6.0",
  "joda-time"         % "joda-time"  % "2.10.1",
  // to eliminate the annoying '[warn] Class org.joda.convert.FromString not found - continuing with a stub.' message
  "org.joda" % "joda-convert" % "2.2.0" % "provided",
  //need nebula native client for encoding, need to run mvn install to deploy to local repo before used
  "org.rocksdb" % "rocksdbjni"    % "5.17.2",
  "com.vesoft"  % "native-client" % "0.1.0"
)

//CAUTION: when dependency with version of X-SNAPSHOT is updated, you should comment out the following line, and run sbt update
updateOptions := updateOptions.value.withLatestSnapshots(false)

assemblyMergeStrategy in assembly := {
  case PathList("org", "slf4j", xs @ _*)             => MergeStrategy.discard
  case PathList("org", "apache", "logging", xs @ _*) => MergeStrategy.discard
  case PathList("ch", "qos", "logback", xs @ _*)     => MergeStrategy.discard
  case PathList("org", "scalactic", xs @ _*)         => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule
    .rename("org.apache.commons.cli.**" -> "shadecli.org.apache.commons.cli.@1")
    .inAll
)

assemblyJarName in assembly := "nebula-spark-sstfile-generator.jar"

test in assembly := {}
// should not include scala runtime when submitting spark job
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
