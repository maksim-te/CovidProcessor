
ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "CovidProcessor"
  )

lazy val sparkVersion = "3.5.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion % "provided",
  "com.clickhouse" % "clickhouse-jdbc" % "0.9.5" classifier "all",
  "com.clickhouse.spark" % "clickhouse-spark-runtime-3.5_2.12" % "0.10.0"
)

assembly / assemblyMergeStrategy := {
  case m if m.toLowerCase.endsWith("manifest.mf")       => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")   => MergeStrategy.discard
  case "reference.conf"                                 => MergeStrategy.concat
  case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
  case _                                                => MergeStrategy.first
}