package org.repl.kafkasparkmongo.samples

import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import com.mongodb.spark.sql.fieldTypes.ObjectId
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, concat, desc, lit, lower, split, substring, typedLit, udf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.rdd.RDD
import org.bson.Document

object BookRecommendation {

  case class students_cc(id: Int,
                         year_graduated: String,
                         name: String)

  def main(args: Array[String]): Unit = {
    //Start the Spark context
    val conf = new SparkConf().setAppName("SimpleStreamingFromRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    //Ways to read mongodb
    //val usersDF = spark.read.mongo(ReadConfig(Map("uri" -> "mongodb://lms:qwerty123@127.0.0.1/lms_db.User")))
    //println("Users count: " + usersDF.count())
    //println("usersDF schema")
    //usersDF.printSchema()
    //val df = spark.sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri", "mongodb://lms:qwerty123@127.0.0.1/lms_db.User").load()
    //usersDF.printSchema()
               
    //Another way to read data from MongoDB
    val bookDF = MongoSpark.load(spark.sparkContext, ReadConfig(Map("uri" -> "mongodb://lms:qwerty123@127.0.0.1/lms_db.Book"))).toDF()
      .drop("Image-URL-L", "Image-URL-M", "Image-URL-S")
    //uDF.show(false)
    //uDF.printSchema()
    //val aggregatedRdd = uDF.withPipeline(Seq(Document.parse("{ '$match': { 'Book-Title' : {'$regex' : '^Sherlock' } } }")))
    //println(aggregatedRdd.count)

    //Simple Popularity based Recommendation System
    val bookRatingDF = MongoSpark.load(spark.sparkContext, ReadConfig(Map("uri" -> "mongodb://lms:qwerty123@127.0.0.1/lms_db.UserBookRating"))).toDF()
    val raters = bookRatingDF.groupBy(functions.col("ISBN")).agg(functions.count("rating").as("count"))
    val topRaters = raters.sort(desc("count")).toDF().limit(10)
    val joinedDF = topRaters.join(bookDF, Seq("ISBN"))
    joinedDF.show(false)
    joinedDF.printSchema()

    //Collaborative Filtering using ALS (alternating least squares) Spark ML
    import spark.sqlContext.implicits._
    //create DF with userId as integer (ALS requires integer for Ids)
    val stringindexer1 = new StringIndexer().setInputCol("userId").setOutputCol("userIdNum")
    val modelc1 = stringindexer1.fit(bookRatingDF)
    val bookRatingT1DF = modelc1.transform(bookRatingDF)
    val stringindexer2 = new StringIndexer().setInputCol("ISBN").setOutputCol("isbnNum")
    val modelc2 = stringindexer2.fit(bookRatingT1DF)
    val bookRatingNewDF = modelc2.transform(bookRatingT1DF)
    //TODO: save the mapping userId -> userNum and ISBN -> isbnNum mapping in mongodb

    val Array(training, test) = bookRatingNewDF.randomSplit(Array(0.8, 0.2))
    // Build the recommendation model using ALS on the training data
    val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userIdNum").setItemCol("isbnNum").setRatingCol("rating")
    val model = als.fit(training)
    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    //use the model to generate a set of 10 recommended movies for each user in the dataset,
    //val docs  = predictions.map( r => {
    //  ( r.getAs("userId"), r.getAs("ISBN"),  r.getAs("rating") )
    //} )
    //  .toDF( "userId", "ISBN", "rating" )

    //println(s"Generate top 3 movie recommendations for each user")
    //val userRecs = model.recommendForAllUsers(3).show(truncate = false)
    //println(s"Generate top 3 user recommendations for each book")
    //val bookRecs = model.recommendForAllItems(3).show(truncate = false)

    println(s"Generate top 3 movie recommendations for a specified set of users (3)")
    val users = bookRatingNewDF.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 3).show(truncate = false)
    println(s"Generate top 3 user recommendations for a specified set of books (3)")
    val books = bookRatingNewDF.select(als.getItemCol).distinct().limit(3)
    val booksSubSetRecs = model.recommendForItemSubset(books, 3).show(truncate = false)

    // Directly from prediction table: where is for case when we have big DataFrame with many users
    //model.transform (bookRatingNewDF.where('userId === givenUserId))
    //  .select ('isbn, 'prediction)
    //  .orderBy('prediction.desc)
    //  .limit(N)
    //  .map { case Row (isbn: Int, prediction: Double) => (isbn, prediction) }
    //  .collect()
  }
}





