package org.repl.kafkasparkmongo.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
  *
  * Usage: NameGenerator <hostname> <port>
  * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  * and then run the example
  *    `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
  */
object NameGenerator extends Logging {
  def main(args: Array[String]) {
    setStreamingLogLevels()

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NameGenerator").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val usNamesDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("data/names/us-500.csv")
    val usFirstNamesDF = usNamesDF.select("first_name")
    val usLastNamesDF = usNamesDF.select("last_name")
    val usFullNamesDF = usFirstNamesDF.crossJoin(usLastNamesDF).toDF(Seq("firstname", "lastname"): _*)

    val caNamesDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("data/names/ca-500.csv")
    val caFirstNamesDF = caNamesDF.select("first_name")
    val caLastNamesDF = caNamesDF.select("last_name")
    val caFullNamesDF = caFirstNamesDF.crossJoin(caLastNamesDF).toDF(Seq("firstname", "lastname"): _*)

    val ukNamesDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("data/names/uk-500.csv")
    val ukFirstNamesDF = ukNamesDF.select("first_name")
    val ukLastNamesDF = ukNamesDF.select("last_name")
    val ukFullNamesDF = ukFirstNamesDF.crossJoin(ukLastNamesDF).toDF(Seq("firstname", "lastname"): _*)

    val auNamesDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("data/names/au-500.csv")
    val auFirstNamesDF = auNamesDF.select("first_name")
    val auLastNamesDF = auNamesDF.select("last_name")
    val auFullNamesDF = auFirstNamesDF.crossJoin(auLastNamesDF).toDF(Seq("firstname", "lastname"): _*)

    val combinedNamesDF = usFullNamesDF.union(caFullNamesDF).union(ukFullNamesDF).union(auFullNamesDF).orderBy(rand()).sample(0.30)
    val schema = combinedNamesDF.schema
    val rows = combinedNamesDF.rdd.zipWithUniqueId.map{
      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)
    }
    val dfWithPK = spark.sqlContext.createDataFrame(rows, StructType(StructField("id", LongType, false) +: schema.fields))

    dfWithPK.repartition(1).write.option("quoteAll", true).option("header", "true").csv("data/names/30K-names")

    //val ssc = new StreamingContext(sc, Seconds(1))
    //ssc.start()
    //ssc.awaitTermination()
  }

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
