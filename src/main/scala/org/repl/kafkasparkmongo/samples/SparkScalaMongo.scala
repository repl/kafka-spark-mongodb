package org.repl.kafkasparkmongo.samples

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkScalaMongo {

  case class students_cc(id: Int,
                         year_graduated: String,
                         courses_registered: List[cid_sem],
                         name: String)
  case class cid_sem(cid: String, sem: String)

  def main(args: Array[String]): Unit = {
    //Start the Spark context

    val mongoConnectionURI = "mongodb://test:qwerty123@127.0.0.1/test.students"

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark_Mongo")
      .config("spark.mongodb.input.uri", mongoConnectionURI)
      .config("spark.mongodb.output.uri", mongoConnectionURI)
      .getOrCreate()

    //Read data from MongoDB
    val studentsDF = MongoSpark.load(spark)
    studentsDF.show(false)

    //Print schema
    studentsDF.printSchema()

    //Write data to Mongo
    import spark.implicits._

    val listOfCoursesSem = List(cid_sem("CS003", "Spring_2011"),
      cid_sem("CS006", "Summer_2011"),
      cid_sem("CS009", "Fall_2011"))
    val newStudents =
      Seq(students_cc(12, "2011", listOfCoursesSem, "Black Panther")).toDF()

    MongoSpark.save(newStudents.write.mode(SaveMode.Overwrite))

    //Load data again to check if the insert was successful
    val studentsData = MongoSpark.load(spark)
    studentsData.show(false)
  }

  //https://github.com/mongodb/mongo-spark/tree/master/examples/src/test/scala/tour

  //Read
  //val df3 = sparkSession.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://example.com/database.collection"))) // ReadConfig used for configuration
  //val df4 = sparkSession.read.mongo() // SparkSession used for configuration
  //sqlContext.read.format("mongo").load()
  //usersDF = spark.read.mongo(ReadConfig(Map("uri" -> "mongodb://lms:qwerty123@127.0.0.1/lms_db.User")))
  //MongoSpark.load[Character](sparkSession, ReadConfig(Map("collection" -> "hundredClub"), Some(ReadConfig(sparkSession)))).show()


  //# Write to Spark
  //#1
  //sparkSession.sparkContext.parallelize(docs.map(Document.parse)).saveToMongoDB()
  //#2
  //sparkdf.write.option("collection", "hundredClub").mode("overwrite").mongo()
  //sparkdf.write.option("collection", "hundredClub").format("mongo").save()
  //sparkdf.write.format("com.mongodb.spark.sql.DefaultSource")
  //             .mode("append").option("spark.mongodb.output.uri", "mongodb://host101:27017/dbName.collName")
  //             .option("replaceDocument", "false")
  //             .save()
  //#3
  //MongoSpark.save(df,WriteConfig).mode("overwrite")) //drops collection before writing the results, if the collection already exists.




  //query
  //personDf.select($"_id", $"addresses"(0)("street"), $"country"("name"))
  //val aggregatedRdd = uDF.withPipeline(Seq(Document.parse("{ '$match': { 'Book-Title' : {'$regex' : '^Sherlock' } } }")))

  //mongodb
  //db.zipcodes.aggregate( [
  //{ $group: { _id: "$state", totalPop: { $sum: "$pop" } } },
  //{ $match: { totalPop: { $gte: 10*1000*1000 } } }
  //] )
  //spark equivalent
  //println( "States with Populations above 10 Million" )
  //import zipDf.sqlContext.implicits._ // 1)
  //zipDf.groupBy("state")
  //.sum("pop")
  //.withColumnRenamed("sum(pop)", "count") // 2)
  //.filter($"count" > 10000000)
  //.show()

  //* aggregate with connector
  //val aggregatedRdd = uDF.withPipeline(Seq(Document.parse("{ '$match': { 'Book-Title' : {'$regex' : '^Sherlock' } } }")))

  //BasicDBObject dateRange = new BasicDBObject ("$gte", new Date(current.getYear(), current.getMonth(), current.getDate());
  //dateRange.put("$lt", new Date(current.getYear(), current.getMonth() - 1, current.getDate());
  //BasicDBObject query = new BasicDBObject("created_on", dateRange);
  //OR BasicDBObject query = new BasicDBObject("created_on", new BasicDBObject("$gte", new DateTime().toDate()).append("$lt", new DateTime().toDate()));
  //rdd.withPipeline(singletonList(query));

  //using predicate pushdown (filter and select)
  //zipDf
  //.filter($"pop" > 0)
  //.select("state")
  //.explain(true)

  //using sqlcontext
  //zipDf.createOrReplaceTempView("zips") // 1)
  //zipDf.sqlContext.sql( // 2)
  //"""SELECT state, sum(pop) AS count
  //  FROM zips
  //  GROUP BY state
  //  HAVING sum(pop) > 10000000"""
  //)
  //.show()
}
