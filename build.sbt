import Dependencies._

name := """Kakfa Spark MongoDb"""


spName := "repl/Kakfa-Spark-MongoDb"

sparkVersion := "2.0.0"

sparkComponents ++= Seq("core","streaming", "sql")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

spIncludeMaven := true

credentials += Credentials("Spark Packages Realm",
  "spark-packages.org",
  sys.props.getOrElse("GITHUB_USERNAME", default = ""),
  sys.props.getOrElse("GITHUB_PERSONAL_ACCESS_TOKEN", default = ""))



lazy val commonSettings = Seq(
  organization := "org.repl",
  version := "0.1.0",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "Kafka-Spark-MongoDb",
    libraryDependencies ++= Seq(kafka, sparkCore, sparkStreaming, sparkSql, sparkStreamingKafka, sparkSqlKafka, sparkKafkaWriter, 
      sparkCassandraConnect, cassandraDriver, sparkMongoConnect, logback, json4s, typesafeConfig, akkaActor)
  )
