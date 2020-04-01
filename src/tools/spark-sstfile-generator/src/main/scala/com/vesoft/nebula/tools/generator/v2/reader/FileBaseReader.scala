/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2.reader

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * The FileBaseReader is the abstract class for HDFS file reader.
  *
  * @param session
  * @param path
  */
abstract class FileBaseReader(val session: SparkSession, val path: String) extends Reader {

  require(path.trim.size != 0)

  override def close(): Unit = {
    session.close()
  }
}

/**
  * The ParquetReader extend the FileBaseReader and support read parquet file from HDFS.
  *
  * @param session
  * @param path
  */
class ParquetReader(override val session: SparkSession, override val path: String)
    extends FileBaseReader(session, path) {

  override def read(): Dataset[Row] = {
    session.read.parquet(path)
  }
}

/**
  * The ORCReader extend the FileBaseReader and support read orc file from HDFS.
  *
  * @param session
  * @param path
  */
class ORCReader(override val session: SparkSession, override val path: String)
    extends FileBaseReader(session, path) {

  override def read(): Dataset[Row] = {
    session.read.orc(path)
  }
}

/**
  * The JSONReader extend the FileBaseReader and support read json file from HDFS.
  *
  * @param session
  * @param path
  */
class JSONReader(override val session: SparkSession, override val path: String)
    extends FileBaseReader(session, path) {

  override def read(): Dataset[Row] = {
    session.read.json(path)
  }
}

/**
  * The CSVReader extend the FileBaseReader and support read csv file from HDFS.
  *
  * @param session
  * @param path
  */
class CSVReader(override val session: SparkSession, override val path: String)
    extends FileBaseReader(session, path) {

  override def read(): Dataset[Row] = {
    session.read.csv(path)
  }
}

/**
  * The CustomReader extend the FileBaseReader and support read text file from HDFS.
  * Transformation is a function convert a line into Row.
  * The structure of the row should be specified.
  *
  * @param session
  * @param path
  * @param transformation
  * @param structType
  */
class CustomReader(override val session: SparkSession,
                   override val path: String,
                   transformation: String => Row,
                   structType: StructType)
    extends FileBaseReader(session, path) {

  override def read(): Dataset[Row] = {
    val encoder = RowEncoder.apply(structType)
    session.read
      .text(path)
      .map(row => transformation(row.getString(0)))(encoder)
  }
}
