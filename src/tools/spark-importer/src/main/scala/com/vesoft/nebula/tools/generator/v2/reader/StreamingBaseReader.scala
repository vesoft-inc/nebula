/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2.reader

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Spark Streaming
  *
  * @param session
  */
abstract class StreamingBaseReader(override val session: SparkSession) extends Reader {

  override def close(): Unit = {
    session.close()
  }
}

/**
  *
  * @param session
  * @param host
  * @param port
  */
class SocketReader(override val session: SparkSession, host: String = "127.0.0.1", port: Int = 8989)
    extends StreamingBaseReader(session) {

  require(host.trim.size != 0 && port > 0)

  override def read(): DataFrame = {
    session.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()
  }
}

/**
  *
  * @param session
  * @param server
  * @param topic
  */
class KafkaReader(override val session: SparkSession, server: String = "127.0.0.1:", topic: String)
    extends StreamingBaseReader(session) {

  require(server.trim.size != 0 && topic.trim.size != 0)

  override def read(): DataFrame = {
    session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", server)
      .option("subscribe", topic)
      .load()
  }
}
