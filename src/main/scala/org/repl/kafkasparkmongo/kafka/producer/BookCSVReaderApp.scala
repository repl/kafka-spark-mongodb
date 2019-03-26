package org.repl.kafkasparkmongo.kafka.producer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import com.github.benfradet.spark.kafka.writer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.repl.kafkasparkmongo.kafka.producer.KafkaBookRecordProducer.rnd

import scala.util.parsing.json.JSONObject

case class Book(isbn:String,title:String,author:String,yearOfPublication: String,publisher:String,imageUrlS:String,imageUrlM:String,imageUrlL:String)

object BookCSVReaderApp extends App {

  //val message = write(Tweet(tweetId,status.getCreatedAt.getTime,user.getId,userName,status.getPlace.getCountry,user.getFriendsCount))
  //KafkaTwitterProducer.send("tweets",message)

  val sparkConf = new SparkConf()
    .setAppName("BookCSV_Batch_Loader").setMaster("local[2]").set("spark.sql.streaming.checkpointLocation", "/home/edfx/temp")
  val sc = new SparkContext(sparkConf)
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
  val streamingDataFrame = spark.readStream.format("csv")
    .option("basePath", "/mnt/work/my-projects/kafka-spark-mongodb/data")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .option("inferSchema", true)
    .schema(mySchema)
    .load("data/BX-Books.csv")
  println("Book schema")
  streamingDataFrame.printSchema()

  var query = streamingDataFrame
    .selectExpr("CAST(ISBN AS STRING) AS key", "to_json(struct(*)) AS value")
    //.withColumn("key", streamingDataFrame.col("ISBN"))
    //.withColumn("value", streamingDataFrame.col("Title")) // to_json(struct( streamingDataFrame.columns.map(col(_)):_*  )))
    .writeStream
    .format("kafka")
    .option("topic", "TopicBooks")
    .option("kafka.bootstrap.servers", "localhost:9092")
    //.option("checkpointLocation", "/tmp")
    .start()

  /*streamingDataFrame.rdd.foreachPartition(partition =>
      partition.foreach {
        case row: Row => {
           println(row)
          KafkaBookRecordProducer.send("TopicBooks", JSONObject(row.getValuesMap(row.schema.fieldNames)).toString())
        }
      }
  )*/
  /*val topic = "TopicBooks"
  val producerConfig = Map(
    "bootstrap.servers" -> "127.0.0.1:9092",
    "key.serializer" -> classOf[StringSerializer].getName,
    "value.serializer" -> classOf[StringSerializer].getName
  )
  val partition = rnd.nextInt(4).toString
  streamingDataFrame.rdd.writeToKafka(
    producerConfig,
    row => new ProducerRecord[String, String](topic, partition, JSONObject(row.getValuesMap(row.schema.fieldNames)).toString())
  )*/

  query.awaitTermination()

  Thread.sleep(100000000)
}
