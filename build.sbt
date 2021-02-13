organization := "com.github.mrpowers"
name := "bebe"

version := "0.0.1"

scalaVersion := "2.12.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0" % "provided"

libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "0.38.2" % "test"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// scaladoc settings
Compile / doc / scalacOptions ++= Seq("-groups")

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

fork in Test := true

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/MrPowers/bebe"))
developers ++= List(
  Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers"))
)
scmInfo := Some(ScmInfo(url("https://github.com/MrPowers/bebe"), "git@github.com:MrPowers/bebe.git"))

updateOptions := updateOptions.value.withLatestSnapshots(false)

publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

Global/useGpgPinentry := true
