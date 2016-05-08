name := "phoenix-on-spark"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
  "org.apache.phoenix" % "phoenix-core" % "4.7.0-HBase-1.1" % "provided" exclude("sqlline", "sqlline"),
  "org.apache.phoenix" % "phoenix-spark" % "4.7.0-HBase-1.1" % "provided",
  "org.apache.hbase" % "hbase-client" % "1.1.0.1" % "provided",
  "org.apache.hbase" % "hbase-common" % "1.1.0.1" % "provided",
  "org.apache.hbase" % "hbase-server" % "1.1.0.1" % "provided",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.1.0.1" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.0-M15" % "test",
  "sqlline" % "sqlline" % "1.1.9"
)
