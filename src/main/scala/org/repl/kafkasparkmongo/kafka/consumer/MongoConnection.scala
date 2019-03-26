package org.repl.kafkasparkmongo.kafka.consumer

import com.datastax.driver.core._
import org.repl.kafkasparkmongo.util.KafkaCassandraConfigUtil._
import org.slf4j.LoggerFactory

trait MongoConnection {

  val logger = LoggerFactory.getLogger(getClass.getName)
  val defaultConsistencyLevel = ConsistencyLevel.valueOf(writeConsistency)
  val cassandraConn: Session = {
    val cluster = new Cluster.Builder().withClusterName("Test Cluster").
      addContactPoints(hosts.toArray: _*).
      withPort(port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
    val session = cluster.connect
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspaces.get(0)} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }")
    session.execute(s"USE ${cassandraKeyspaces.get(0)}")
    val query = s"CREATE TABLE IF NOT EXISTS tweets " +
      s"(tweetId bigint, createdAt bigint, userId bigint, " +
      s"tweetUserName text, countryName text, friendsCount bigint, " +
      s" PRIMARY KEY (tweetId, createdAt)) "

    createTables(session, query)
    session
  }

  def createTables(session: Session, createTableQuery: String): ResultSet = session.execute(createTableQuery)

}

object MongoConnection extends MongoConnection
