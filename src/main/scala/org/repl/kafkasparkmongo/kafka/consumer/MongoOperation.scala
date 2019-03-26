package org.repl.kafkasparkmongo.kafka.consumer

import java.util.Date

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

case class TwitterUser(userId: Long, createdAt: Date, friendCount: Long)

case class Response(numberOfUser: Long, data: List[TwitterUser])

object MongoOperation extends MongoConnection {

  def insertBooks(listJson: List[String], dbName: String = "books") = {
    println(":::::::::::::::::::::::::::: batch inserted")
    //listJson.map(json => cassandraConn.execute(s"INSERT INTO $tableName JSON '$json'"))
  }
}
