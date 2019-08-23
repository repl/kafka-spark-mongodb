package org.repl.kafkasparkmongo

import java.util.{ Arrays, Properties }

import com.mongodb.spark._
import com.mongodb.spark.config.{ ReadConfig, WriteConfig }
import com.mongodb.spark.sql._
import com.mongodb.spark.sql.fieldTypes.ObjectId
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.streaming.kafka010.{ ConsumerStrategies, KafkaUtils, LocationStrategies }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.repl.kafkasparkmongo.util.{ SimpleKafkaClient, SparkKafkaSink }
import org.apache.spark.sql.functions.{ col, concat, lit, lower, split, substring, typedLit, udf }
import org.mindrot.jbcrypt.BCrypt

import scala.util.parsing.json.JSONObject

object BXBookUserRatingsLoader {

  def main(args: Array[String]) {

    val topic = "TopicBookUserRatings"

    val conf = new SparkConf().setAppName("SimpleStreamingFromRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val props: Properties = SimpleKafkaClient.getBasicStringStringConsumer("localhost:9092")

    val kafkaStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Arrays.asList(topic),
        props.asInstanceOf[java.util.Map[String, Object]]))

    val writeConfig = WriteConfig(Map("uri" -> "mongodb://lms:qwerty123@127.0.0.1/lms_db.UserBookRating"))

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    kafkaStream.map(v => v.value).foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())
      if (r.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        println("*** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("*** partition size = " + a.size))
        val toObjectId = udf[ObjectId, String](new ObjectId(_))
        val df = spark.read.json(r).withColumn("uid", toObjectId(col("userId")))
        df.printSchema()
        //r.foreach(s => println(s))
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

        Thread.sleep(20000l)
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
   * @param sc
   * @param topic
   * @param config
   */
  def send(sc: SparkContext, topic: String, config: Properties): Unit = {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    println("*** producing data")
    val readConfig = ReadConfig(Map("uri" -> "mongodb://lms:qwerty123@127.0.0.1/lms_db.User"))
    val usersDF = spark.read.mongo(readConfig)
    println("Users count: " + usersDF.count())
    println("usersDF schema")
    usersDF.printSchema()

    val mySchema = StructType(Array(
      StructField("usernum", StringType),
      StructField("ISBN", StringType),
      StructField("rating", StringType)))
    val dataFrame = spark.sqlContext
      .read
      .format("csv")
      .option("header", "true")
      //.option("mode", "DROPMALFORMED")
      .option("delimiter", ";")
      .option("inferSchema", true)
      .schema(mySchema)
      .load("data/bx/BX-Book-Ratings.csv")
      .toDF(Seq("usernum", "ISBN", "Rating"): _*)

    println("BookRatings count in bx dataframe: " + dataFrame.count())
    println("Dataframe schema")
    dataFrame.printSchema()

    val joinedDF = usersDF.join(dataFrame, Seq("usernum"))
      .withColumn("ratingNum", col("rating").cast(IntegerType))
      .drop("rating")
      .select(
        col("_id").as("userId"),
        col("firstname"),
        col("lastname"),
        col("ISBN"),
        col("ratingNum").as("rating"))
    println("JoinedDF schema")
    joinedDF.printSchema()
    println("BookRatings count in joined dataframe: " + joinedDF.count())

    val kafkaSink = sc.broadcast(SparkKafkaSink(config))
    joinedDF.rdd.foreach { row =>
      // NOTE:
      //     1) the keys and values are strings, which is important when receiving them
      //     2) We don't specify which Kafka partition to send to, so a hash of the key
      //        is used to determine this
      var rowMap: Map[String, Any] = row.getValuesMap(row.schema.fieldNames)
      val mutableRowMap = collection.mutable.Map(rowMap.toSeq: _*)
      mutableRowMap.remove("userId")
      val userId = row.getStruct(0).get(0).toString
      mutableRowMap.put("userId", userId)
      try {
        kafkaSink.value.send(topic, userId, JSONObject(mutableRowMap.toMap).toString())
      } catch {
        case npe: NullPointerException => {
          println("Got NPE for rowMap " + rowMap)
        }
        case e: Throwable => {
          println(e)
        }
      }
    }

    //streamingDataFrame.createOrReplaceTempView("books")
    //val sqlResult= spark.sql("select * from books")
    //sqlResult.show()
  }
}