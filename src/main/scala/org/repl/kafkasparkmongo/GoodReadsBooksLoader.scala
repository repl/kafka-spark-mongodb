package org.repl.kafkasparkmongo

import java.util.{Arrays, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.repl.kafkasparkmongo.util.{SimpleKafkaClient, SparkKafkaSink}

import scala.util.parsing.json.JSONObject

object GoodReadsBooksLoader {

  def main(args: Array[String]) {

    val topic = "TopicBooks"

    val conf = new SparkConf().setAppName("SimpleStreamingFromRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._
    val props: Properties = SimpleKafkaClient.getBasicStringStringConsumer("localhost:9092")

    val kafkaStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Arrays.asList(topic),
        props.asInstanceOf[java.util.Map[String, Object]]
      )
    )

    val writeConfig = WriteConfig(Map("uri" -> "mongodb://test:qwerty123@127.0.0.1/test.goodreadsbooks"))

    //UDF to parse JSON array String
    val jsonConverter = udf { jsonString: String =>
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper.readValue(jsonString, classOf[Array[String]])
    }
    val popularShelvesSchema =
      ArrayType(StructType(
        Array(
          StructField("count", StringType),
          StructField("name", StringType)
        )
      ))
    val authorsSchema =
      ArrayType(StructType(
        Array(
          StructField("author_id", StringType),
          StructField("role", StringType)
        )
      ))

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    kafkaStream.map(v => v.value).foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())
      if (r.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        println("*** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("*** partition size = " + a.size))
        //r.foreach(s => println(s))

        //df.withColumn("similar_books_new", from_json($"similar_books", ArrayType(StringType)))
        //df.select(from_json($"similar_books", ArrayType(StringType)) as "similar_books_new").show
        //df.select(jsonConverter($"similar_books") as "similar_books_new").show
        val df = spark.read.json(r)
          .withColumn("similar_books", jsonConverter($"similar_books"))
          .withColumn("series", jsonConverter($"series"))
          .withColumn("authors", from_json($"authors", authorsSchema))
          .withColumn("popular_shelves", from_json($"popular_shelves", popularShelvesSchema))
        //df.printSchema()

        println("Writing to MongoDb")
        MongoSpark.save(df, writeConfig)
      }
    })

    ssc.start()

    println("*** started termination monitor")
    // streams seem to need some time to get going
    Thread.sleep(5000)

    val producerThread = new Thread("Streaming Termination Controller") {
      override def run() {
        val client = new SimpleKafkaClient("localhost:9092")

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

    println("*** done")
  }

  /**
    * Publish some data to a topic. Encapsulated here to ensure serializable.
    *
    * @param max
    * @param sc
    * @param topic
    * @param config
    */
  def send(sc: SparkContext, topic: String, config: Properties): Unit = {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val mySchema = StructType(Array(
      StructField("isbn", StringType),
      StructField("text_reviews_count", StringType),
      StructField("series", StringType), //ArrayType(StringType)
      StructField("country_code", StringType),
      StructField("language_code", StringType),
      StructField("popular_shelves", StringType), //popularShelvesSchema
      StructField("asin", StringType),
      StructField("is_ebook", StringType),
      StructField("average_rating", StringType),
      StructField("kindle_asin", StringType),
      StructField("similar_books", StringType), //ArrayType(StringType)
      StructField("description", StringType),
      StructField("format", StringType),
      StructField("link", StringType),
      StructField("authors", StringType),  //authorsSchema
      StructField("publisher", StringType),
      StructField("num_pages", StringType),
      StructField("publication_day", StringType),
      StructField("isbn13", StringType),
      StructField("publication_month", StringType),
      StructField("edition_information", StringType),
      StructField("publication_year", StringType),
      StructField("url", StringType),
      StructField("image_url", StringType),
      StructField("book_id", StringType),
      StructField("ratings_count", StringType),
      StructField("work_id", StringType)
    ))
    val dataFrame = spark.sqlContext
      .read
      .format("json")
      //.option("header", "true")
      //.option("mode", "DROPMALFORMED")
      //.option("delimiter", ";")
      //.option("inferSchema", true)
      .schema(mySchema)
      .load("data/goodreads/goodreads-onebook.json")

    println("Book schema")
    dataFrame.printSchema()
    println("Books count: " + dataFrame.count())
    println("*** producing data")

    val kafkaSink = sc.broadcast(SparkKafkaSink(config))
    dataFrame.rdd.foreach { row =>
      // NOTE:
      //     1) the keys and values are strings, which is important when receiving them
      //     2) We don't specify which Kafka partition to send to, so a hash of the key
      //        is used to determine this
      kafkaSink.value.send(topic, row.getString(0), JSONObject(row.getValuesMap(row.schema.fieldNames)).toString())
    }

    //streamingDataFrame.createOrReplaceTempView("books")
    //val sqlResult= spark.sql("select * from books")
    //sqlResult.show()
  }
}