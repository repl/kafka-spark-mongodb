package org.repl.kafkasparkmongo

import java.util.{Arrays, Properties}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.repl.kafkasparkmongo.util.{EmbeddedKafkaServer, SimpleKafkaClient, SparkKafkaSink}

import scala.util.parsing.json.JSONObject

/**
  * The most basic streaming example: starts a Kafka server, creates a topic, creates a stream
  * to process that topic, and publishes some data using the SparkKafkaSink.
  *
  * Notice there's quite a lot of waiting. It takes some time for streaming to get going,
  * and data published too early tends to be missed by the stream. (No doubt, this is partly
  * because this example uses the simplest method to create the stream, and thus doesn't
  * get an opportunity to set auto.offset.reset to "earliest".
  *
  * Also, data that is published takes some time to propagate to the stream.
  * This seems inevitable, and is almost guaranteed to be slower
  * in a self-contained example like this.
  */
object SimpleStreamingFromRDD {

  /**
    * Publish some data to a topic. Encapsulated here to ensure serializability.
    *
    * @param max
    * @param sc
    * @param topic
    * @param config
    */
  def send(sc: SparkContext, topic: String, config: Properties): Unit = {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val mySchema = StructType(Array(
      StructField("ISBN", StringType),
      StructField("Title", StringType),
      StructField("Author", StringType),
      StructField("YearOfPublication", StringType),
      StructField("Publisher", StringType),
      StructField("ImageUrlS", StringType),
      StructField("ImageUrlM", StringType),
      StructField("ImageUrlL", StringType)
    ))
    val streamingDataFrame = spark.sqlContext
      //.readStream
      .read
      .format("csv")
      //.option("path", "/var/tmp")
      .option("header", "true")
      //.option("mode", "DROPMALFORMED")
      .option("delimiter", ";")
      .option("inferSchema", true)
      //.option("checkpointLocation", "/home/edfx/temp")
      //.schema(mySchema)
      .load("data/BX-Books.csv")
      //.writeStream
      //.format("console")
      //.start()

    println("Book schema")
    streamingDataFrame.printSchema()
    println("Books count: " + streamingDataFrame.count())
    //streamingDataFrame.createOrReplaceTempView("books")
    //val sqlResult= spark.sql("select * from books")
    //sqlResult.show()

    /*val query = streamingDataFrame
      .selectExpr("CAST(ISBN AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("topic", topic)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/home/edfx/temp")
      .start()
    query.awaitTermination()
  */

    val kafkaSink = sc.broadcast(SparkKafkaSink(config))
    println("*** producing data")

    streamingDataFrame.rdd.foreach { row =>
      // NOTE:
      //     1) the keys and values are strings, which is important when receiving them
      //     2) We don't specify which Kafka partition to send to, so a hash of the key
      //        is used to determine this
      kafkaSink.value.send(topic, row.getString(0), JSONObject(row.getValuesMap(row.schema.fieldNames)).toString())
    }
  }

  def main(args: Array[String]) {

    val topic = "TopicBooks"

    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)

    val conf = new SparkConf().setAppName("SimpleStreamingFromRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))
    // Create the stream.
    val props: Properties = SimpleKafkaClient.getBasicStringStringConsumer(kafkaServer)

    val kafkaStream = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Arrays.asList(topic),
          props.asInstanceOf[java.util.Map[String, Object]]
        )
      )

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    kafkaStream.foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())
      r.foreach(s => println(s))
      if (r.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        println("*** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("*** partition size = " + a.size))
      }
    })

    ssc.start()

    println("*** started termination monitor")
    // streams seem to need some time to get going
    Thread.sleep(5000)

    val producerThread = new Thread("Streaming Termination Controller") {
      override def run() {
        val client = new SimpleKafkaClient(kafkaServer)

        send(sc, topic, client.basicStringStringProducer)

        Thread.sleep(10000l)
        println("*** requesting streaming termination")
        ssc.stop(stopSparkContext = false, stopGracefully = true)
      }
    }
    producerThread.start()

    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }

    // stop Spark
    sc.stop()

    // stop Kafka
    kafkaServer.stop()

    println("*** done")
  }
}