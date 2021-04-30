organization := "org.hablapps"
name := "doric"

version := "0.0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.apache.spark"    %% "spark-sql"        % "3.1.1"  % "provided",
  "org.typelevel"       %% "cats-core"        % "2.3.1",
  "com.lihaoyi"         %% "sourcecode"       % "0.2.6",
  "com.github.mrpowers" %% "spark-daria"      % "0.39.0" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0"  % "test",
  "org.scalatest"       %% "scalatest"        % "3.2.8"  % "test"
)

// scaladoc settings
Compile / doc / scalacOptions ++= Seq("-groups")

// test suite settings
fork in Test := true
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled"
)
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

fork in Test := true

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/hablapps/doric"))
developers ++= List(
  Developer("AlfonsoRR", "Alfonso Roa", "@saco_pepe", url("https://github.com/alfonsorr"))
)
scmInfo := Some(
  ScmInfo(url("https://github.com/hablapps/doric"), "git@github.com:hablapps/doric.git")
)

updateOptions := updateOptions.value.withLatestSnapshots(false)

publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

Global / useGpgPinentry := true

scalacOptions ++= Seq(
  "-encoding",
  "utf8",             // Option and arguments on same line
  "-Xfatal-warnings", // New lines for each options
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-Ypartial-unification"
)
