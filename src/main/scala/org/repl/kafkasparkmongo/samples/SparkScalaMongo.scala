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
}
