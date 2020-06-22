/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.spark.tools

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import com.typesafe.config.Config
import java.io.File
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import com.google.common.base.Optional
import com.google.common.geometry.{S2CellId, S2LatLng}
import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.{
  FutureCallback,
  Futures,
  ListenableFuture,
  MoreExecutors,
  RateLimiter
}
import com.vesoft.nebula.client.graph.async.AsyncGraphClientImpl
import com.vesoft.nebula.graph.ErrorCode
import com.vesoft.nebula.tools.generator.v2.entry.KeyPolicy
import com.vesoft.nebula.tools.generator.v2.{
  Configs,
  DataSourceConfigEntry,
  FileBaseSourceConfigEntry,
  HiveSourceConfigEntry,
  KafkaSourceConfigEntry,
  SocketSourceConfigEntry,
  SourceCategory
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

final case class Argument(
    config: File = new File("application.conf"),
    hive: Boolean = false,
    directly: Boolean = false,
    dry: Boolean = false,
    reload: String = ""
)

final case class TooManyErrorsException(private val message: String) extends Exception(message)

/**
  * SparkClientGenerator is a simple spark job used to write data into Nebula Graph parallel.
  */
object SparkImporter {
  private[this] val LOG = Logger.getLogger(this.getClass)

  private[this] val BATCH_INSERT_TEMPLATE                           = "INSERT %s %s(%s) VALUES %s"
  private[this] val INSERT_VALUE_TEMPLATE                           = "%s: (%s)"
  private[this] val INSERT_VALUE_TEMPLATE_WITH_POLICY               = "%s(\"%s\"): (%s)"
  private[this] val ENDPOINT_TEMPLATE                               = "%s(\"%s\")"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE             = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE_WITH_POLICY = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE                             = "%s->%s@%d: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE_WITH_POLICY                 = "%s->%s@%d: (%s)"
//  private[this] val USE_TEMPLATE                                    = "USE %s"

  private[this] val DEFAULT_EDGE_RANKING = 0L
  private[this] val DEFAULT_ERROR_TIMES  = 16

  private[this] val NEWLINE = "\n"

  // GEO default config
  private[this] val DEFAULT_MIN_CELL_LEVEL = 10
  private[this] val DEFAULT_MAX_CELL_LEVEL = 18

  private[this] val MAX_CORES = 64

  private object Type extends Enumeration {
    type Type = Value
    val VERTEX = Value("VERTEX")
    val EDGE   = Value("EDGE")
  }

//  private object KeyPolicy extends Enumeration {
//    type POLICY = Value
//    val HASH = Value("hash")
//    val UUID = Value("uuid")
//  }

  def main(args: Array[String]): Unit = {
    val PROGRAM_NAME = "Spark Writer"
    val parser = new scopt.OptionParser[Argument](PROGRAM_NAME) {
      head(PROGRAM_NAME, "1.0.0")

      opt[File]('c', "config")
        .required()
        .valueName("<file>")
        .action((x, c) => c.copy(config = x))
        .text("config file")

      opt[Unit]('h', "hive")
        .action((_, c) => c.copy(hive = true))
        .text("hive supported")

      opt[Unit]('d', "directly")
        .action((_, c) => c.copy(directly = true))
        .text("directly mode")

      opt[Unit]('D', "dry")
        .action((_, c) => c.copy(dry = true))
        .text("dry run")

      opt[String]('r', "reload")
        .valueName("<path>")
        .action((x, c) => c.copy(reload = x))
        .text("reload path")
    }

    val c: Argument = parser.parse(args, Argument()) match {
      case Some(config) => config
      case _ => {
        LOG.error("Argument parse failed")
        sys.exit(-1)
      }
    }

    val configs = Configs.parse(c.config)
    LOG.info(s"Config ${configs}")

    val fs       = FileSystem.get(new Configuration())
    val hdfsPath = new Path(configs.errorConfig.errorPath)
    try {
      if (!fs.exists(hdfsPath)) {
        LOG.info(s"Create HDFS directory: ${configs.errorConfig.errorPath}")
        fs.mkdirs(hdfsPath)
      }
    } finally {
      fs.close()
    }

    val session = SparkSession
      .builder()
      .appName(PROGRAM_NAME)

    //    val sparkConfig = config.getObject("spark")
    //    for (key <- sparkConfig.unwrapped().keySet().asScala) {
    //      val configKey = s"spark.${key}"
    //      if (config.getAnyRef(configKey).isInstanceOf[String]) {
    //        val sparkValue = config.getString(configKey)
    //        LOG.info(s"Spark Config Set ${configKey}:${sparkValue}")
    //        session.config(configKey, sparkValue)
    //      } else {
    //        for (subKey <- config.getObject(configKey).unwrapped().keySet().asScala) {
    //          val sparkKey   = s"${configKey}.${subKey}"
    //          val sparkValue = config.getString(sparkKey)
    //          LOG.info(s"Spark Config Set ${sparkKey}:${sparkValue}")
    //          session.config(sparkKey, sparkValue)
    //        }
    //      }
    //    }

    val spark = if (c.hive) {
      session.enableHiveSupport().getOrCreate()
    } else {
      session.getOrCreate()
    }

    if (!c.reload.isEmpty) {
      val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.reload")
      val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.reload")

      spark.read
        .text(c.reload)
        .foreachPartition { records =>
          val hostAndPorts = configs.databaseConfig.addresses.map(HostAndPort.fromString).asJava
          val client = new AsyncGraphClientImpl(
            hostAndPorts,
            configs.connectionConfig.timeout,
            configs.connectionConfig.retry,
            configs.executionConfig.retry
          )
          client.setUser(configs.userConfig.user)
          client.setPassword(configs.userConfig.user)

          if (isSuccessfully(client.connect())) {
            val rateLimiter = RateLimiter.create(configs.rateConfig.limit)
            records.foreach { row =>
              val exec = row.getString(0)
              if (rateLimiter.tryAcquire(configs.rateConfig.timeout, TimeUnit.MILLISECONDS)) {
                val future = client.execute(exec)
                Futures.addCallback(
                  future,
                  new FutureCallback[Optional[Integer]] {
                    override def onSuccess(result: Optional[Integer]): Unit = {
                      batchSuccess.add(1)
                    }

                    override def onFailure(t: Throwable): Unit = {
                      if (batchFailure.value > DEFAULT_ERROR_TIMES) {
                        throw TooManyErrorsException("too many errors")
                      }
                      batchFailure.add(1)
                    }
                  }
                )
              } else {
                batchFailure.add(1)
              }
            }
            client.close()
          } else {
            LOG.error(
              s"Client connection failed. ${configs.userConfig.user}:${configs.userConfig.password}")
          }
        }
      sys.exit(0)
    }

    if (!configs.tagsConfig.isEmpty) {
      for (tagConfig <- configs.tagsConfig) {
        LOG.info(s"Processing Tag ${tagConfig.name}")

        val properties      = tagConfig.fields.values.map(_.toString).toList
        val valueProperties = tagConfig.fields.keys.toList

        val sourceProperties = if (!tagConfig.fields.contains(tagConfig.vertexField)) {
          (tagConfig.fields.keySet + tagConfig.vertexField).toList
        } else {
          tagConfig.fields.keys.toList
        }

        val vertexIndex      = sourceProperties.indexOf(tagConfig.vertexField)
        val nebulaProperties = properties.mkString(",")
        val data             = createDataSource(spark, tagConfig.dataSourceConfigEntry)

        if (data.isDefined && !c.dry) {
          val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.${tagConfig.name}")
          val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.${tagConfig.name}")

          repartition(data.get, tagConfig.partition)
            .map { row =>
              val values = (for {
                property <- valueProperties if property.trim.length != 0
              } yield extraValue(row, property)).mkString(",")
              (row.getString(vertexIndex), values)
            }(Encoders.tuple(Encoders.STRING, Encoders.STRING))
            .foreachPartition { iterator: Iterator[(String, String)] =>
              val service      = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))
              val hostAndPorts = configs.databaseConfig.addresses.map(HostAndPort.fromString).asJava
              val client = new AsyncGraphClientImpl(
                hostAndPorts,
                configs.connectionConfig.timeout,
                configs.connectionConfig.retry,
                configs.executionConfig.retry
              )
              client.setUser(configs.userConfig.user)
              client.setPassword(configs.userConfig.password)

              if (isSuccessfully(client.connect())) {
                val errorBuffer = ArrayBuffer[String]()
                val switchSpaceCode =
                  client.execute(USE_TEMPLATE.format(configs.databaseConfig.space)).get().get()
                if (isSuccessfully(switchSpaceCode)) {
                  val rateLimiter = RateLimiter.create(configs.rateConfig.limit)
                  val futures     = new ListBuffer[ListenableFuture[Optional[Integer]]]()
                  iterator.grouped(tagConfig.batch).foreach { tags =>
                    val exec = BATCH_INSERT_TEMPLATE.format(
                      Type.VERTEX.toString,
                      tagConfig.name,
                      nebulaProperties,
                      tags
                        .map { tag =>
                          if (tagConfig.vertexPolicy.isEmpty) {
                            INSERT_VALUE_TEMPLATE.format(tag._1, tag._2)
                          } else {
                            tagConfig.vertexPolicy.get match {
                              case KeyPolicy.HASH =>
                                INSERT_VALUE_TEMPLATE_WITH_POLICY
                                  .format(KeyPolicy.HASH.toString, tag._1, tag._2)
                              case KeyPolicy.UUID =>
                                INSERT_VALUE_TEMPLATE_WITH_POLICY
                                  .format(KeyPolicy.UUID.toString, tag._1, tag._2)
                              case _ => throw new IllegalArgumentException
                            }
                          }
                        }
                        .mkString(", ")
                    )

                    LOG.info(s"Exec : ${exec}")
                    if (rateLimiter.tryAcquire(configs.rateConfig.timeout, TimeUnit.MILLISECONDS)) {
                      val future = client.execute(exec)
                      futures += future
                    } else {
                      batchFailure.add(1)
                      errorBuffer += exec
                      if (errorBuffer.size == configs.errorConfig.errorMaxSize) {
                        throw TooManyErrorsException(
                          s"Too Many Errors ${configs.errorConfig.errorMaxSize}")
                      }
                    }
                  }

                  val latch = new CountDownLatch(futures.size)
                  for (future <- futures) {
                    Futures.addCallback(
                      future,
                      new FutureCallback[Optional[Integer]] {
                        override def onSuccess(result: Optional[Integer]): Unit = {
                          latch.countDown()
                          batchSuccess.add(1)
                        }

                        override def onFailure(t: Throwable): Unit = {
                          latch.countDown()
                          if (batchFailure.value > DEFAULT_ERROR_TIMES) {
                            throw TooManyErrorsException("too many errors")
                          } else {
                            batchFailure.add(1)
                          }
                        }
                      },
                      service
                    )
                  }

                  if (!errorBuffer.isEmpty) {
                    val fileSystem = FileSystem.get(new Configuration())
                    val errors =
                      fileSystem.create(
                        new Path(s"${configs.errorConfig.errorPath}/${tagConfig.name}"))

                    try {
                      for (error <- errorBuffer) {
                        errors.writeBytes(error)
                        errors.writeBytes(NEWLINE)
                      }
                    } finally {
                      errors.close()
                      fileSystem.close()
                    }
                  }
                  latch.await()
                } else {
                  LOG.error(s"Switch ${configs.databaseConfig.space} Failed")
                }
              } else {
                LOG.error(
                  s"Client connection failed. ${configs.userConfig.user}:${configs.userConfig.password}")
              }

              service.shutdown()
              while (!service.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                Thread.sleep(10)
              }
              client.close()
            }
        }
      }
    } else {
      LOG.warn("Tag is not defined")
    }

    if (!configs.edgesConfig.isEmpty) {
      for (edgeConfig <- configs.edgesConfig) {

        LOG.info(s"Processing Edge ${edgeConfig.name}")

        val properties      = edgeConfig.fields.values.map(_.toString).toList
        val valueProperties = edgeConfig.fields.keys.toList

        val nebulaProperties = properties.mkString(",")
        val data             = createDataSource(spark, edgeConfig.dataSourceConfigEntry)
        if (data.isDefined && !c.dry) {
          val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.${edgeConfig.name}")
          val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.${edgeConfig.name}")

          repartition(data.get, edgeConfig.partition)
            .map { row =>
              val sourceField = if (!edgeConfig.isGeo) {
                if (edgeConfig.sourcePolicy.isEmpty) {
                  row.getLong(row.schema.fieldIndex(edgeConfig.sourceField.get)).toString
                } else {
                  row.getString(row.schema.fieldIndex(edgeConfig.sourceField.get))
                }
              } else {
                val lat = row.getDouble(row.schema.fieldIndex(edgeConfig.latitude.get))
                val lng = row.getDouble(row.schema.fieldIndex(edgeConfig.longitude.get))
                indexCells(lat, lng).mkString(",")
              }

              val values =
                (for {
                  property <- valueProperties if property.trim.length != 0
                } yield extraValue(row, property)).mkString(",")

              if (edgeConfig.rankingField.isDefined) {
                val ranking = row.getLong(row.schema.fieldIndex(edgeConfig.rankingField.get))
                (edgeConfig.sourceField.get, edgeConfig.targetField, ranking, values)
              } else {
                (edgeConfig.sourceField.get, edgeConfig.targetField, DEFAULT_EDGE_RANKING, values)
              }
            }(
              Encoders
                .tuple(Encoders.STRING, Encoders.STRING, Encoders.scalaLong, Encoders.STRING)
            )
            .foreachPartition { iterator: Iterator[(String, String, Long, String)] =>
              val service      = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))
              val hostAndPorts = configs.databaseConfig.addresses.map(HostAndPort.fromString).asJava
              val client = new AsyncGraphClientImpl(
                hostAndPorts,
                configs.connectionConfig.timeout,
                configs.connectionConfig.retry,
                configs.executionConfig.retry
              )

              client.setUser(configs.userConfig.user)
              client.setPassword(configs.userConfig.password)
              if (isSuccessfully(client.connect())) {
                val switchSpaceCode = client.switchSpace(configs.databaseConfig.space).get().get()
                if (isSuccessfully(switchSpaceCode)) {
                  val errorBuffer = ArrayBuffer[String]()
                  val rateLimiter = RateLimiter.create(configs.rateConfig.limit)
                  val futures     = new ListBuffer[ListenableFuture[Optional[Integer]]]()
                  iterator.grouped(edgeConfig.batch).foreach { edges =>
                    val values =
                      if (edgeConfig.rankingField.isEmpty) {
                        edges
                          .map { edge =>
                            // TODO: (darion.yaphet) dataframe.explode() would be better ?
                            (for (source <- edge._1.split(","))
                              yield
                                if (edgeConfig.sourcePolicy.isEmpty &&
                                    edgeConfig.targetPolicy.isEmpty) {
                                  EDGE_VALUE_WITHOUT_RANKING_TEMPLATE
                                    .format(source, edge._2, edge._4)
                                } else {
                                  val source = if (edgeConfig.sourcePolicy.isDefined) {
                                    edgeConfig.sourcePolicy.get match {
                                      case KeyPolicy.HASH =>
                                        ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge._1)
                                      case KeyPolicy.UUID =>
                                        ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge._1)
                                      case _ =>
                                        throw new IllegalArgumentException()
                                    }
                                  } else {
                                    edge._1
                                  }

                                  val target = if (edgeConfig.targetPolicy.isDefined) {
                                    edgeConfig.targetPolicy.get match {
                                      case KeyPolicy.HASH =>
                                        ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge._2)
                                      case KeyPolicy.UUID =>
                                        ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge._2)
                                      case _ =>
                                        throw new IllegalArgumentException()
                                    }
                                  } else {
                                    edge._2
                                  }

                                  EDGE_VALUE_WITHOUT_RANKING_TEMPLATE_WITH_POLICY
                                    .format(source, target, edge._4)
                                }).mkString(", ")
                          }
                          .toList
                          .mkString(", ")
                      } else {
                        edges
                          .map { edge =>
                            // TODO: (darion.yaphet) dataframe.explode() would be better ?
                            (for (source <- edge._1.split(","))
                              yield
                                if (edgeConfig.sourcePolicy.isEmpty && edgeConfig.targetPolicy.isEmpty) {
                                  EDGE_VALUE_TEMPLATE
                                    .format(source, edge._2, edge._3, edge._4)
                                } else {
                                  val source = edgeConfig.sourcePolicy.get match {
                                    case KeyPolicy.HASH =>
                                      ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge._1)
                                    case KeyPolicy.UUID =>
                                      ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge._1)
                                    case _ =>
                                      edge._1
                                  }

                                  val target = edgeConfig.targetPolicy.get match {
                                    case KeyPolicy.HASH =>
                                      ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge._2)
                                    case KeyPolicy.UUID =>
                                      ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge._2)
                                    case _ =>
                                      edge._2
                                  }

                                  EDGE_VALUE_TEMPLATE_WITH_POLICY
                                    .format(source, target, edge._3, edge._4)
                                })
                              .mkString(", ")
                          }
                          .toList
                          .mkString(", ")
                      }

                    val exec = BATCH_INSERT_TEMPLATE
                      .format(Type.EDGE.toString, edgeConfig.name, nebulaProperties, values)
                    LOG.info(s"Exec : ${exec}")
                    if (rateLimiter.tryAcquire(configs.rateConfig.timeout, TimeUnit.MILLISECONDS)) {
                      val future = client.execute(exec)
                      futures += future
                    } else {
                      batchFailure.add(1)
                      LOG.debug("Save the error execution sentence into buffer")
                      errorBuffer += exec
                      if (errorBuffer.size == configs.errorConfig.errorMaxSize) {
                        throw TooManyErrorsException(
                          s"Too Many Errors ${configs.errorConfig.errorMaxSize}")
                      }
                    }
                  }

                  val latch = new CountDownLatch(futures.size)
                  for (future <- futures) {
                    Futures.addCallback(
                      future,
                      new FutureCallback[Optional[Integer]] {
                        override def onSuccess(result: Optional[Integer]): Unit = {
                          latch.countDown()
                          batchSuccess.add(1)
                        }

                        override def onFailure(t: Throwable): Unit = {
                          latch.countDown()
                          if (batchFailure.value > DEFAULT_ERROR_TIMES) {
                            throw TooManyErrorsException("too many errors")
                          } else {
                            batchFailure.add(1)
                          }
                        }
                      },
                      service
                    )
                  }

                  if (!errorBuffer.isEmpty) {
                    val fileSystem = FileSystem.get(new Configuration())
                    val errors =
                      fileSystem.create(
                        new Path(s"${configs.errorConfig.errorPath}/${edgeConfig.name}"))
                    try {
                      for (error <- errorBuffer) {
                        errors.writeBytes(error)
                        errors.writeBytes(NEWLINE)
                      }
                    } finally {
                      errors.close()
                      fileSystem.close()
                    }
                  }
                  latch.await()
                } else {
                  LOG.error(s"Switch ${configs.databaseConfig.space} Failed")
                }
              } else {
                LOG.error(
                  s"Client connection failed. ${configs.userConfig.user}:${configs.userConfig.password}")
              }
              service.shutdown()
              while (!service.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                Thread.sleep(10)
              }
              client.close()
            }
        } else {
          LOG.warn("Edge is not defined")
        }
      }
    }
  }

  /**
    * Create data source for different data type.
    *
    * @param session The Spark Session.
    * @param config  The config.
    * @return
    */
  private[this] def createDataSource(
      session: SparkSession,
      config: DataSourceConfigEntry
  ): Option[DataFrame] = {
    config.category match {
      case SourceCategory.PARQUET => {
        val parquetConfig = config.asInstanceOf[FileBaseSourceConfigEntry]
        LOG.info(s"""Loading Parquet files from ${parquetConfig.path}""")
        Some(session.read.parquet(parquetConfig.path))
      }
      case SourceCategory.ORC => {
        val orcConfig = config.asInstanceOf[FileBaseSourceConfigEntry]
        LOG.info(s"""Loading ORC files from ${orcConfig.path}""")
        Some(session.read.orc(orcConfig.path))
      }
      case SourceCategory.JSON => {
        val jsonConfig = config.asInstanceOf[FileBaseSourceConfigEntry]
        LOG.info(s"""Loading JSON files from ${jsonConfig.path}""")
        Some(session.read.orc(jsonConfig.path))
      }
      case SourceCategory.CSV => {
        val csvConfig = config.asInstanceOf[FileBaseSourceConfigEntry]
        LOG.info(s"""Loading CSV files from ${csvConfig.path}""")
        Some(session.read.orc(csvConfig.path))
      }
      case SourceCategory.HIVE => {
        val hiveConfig = config.asInstanceOf[HiveSourceConfigEntry]
        LOG.info(s"""Loading from Hive and exec ${hiveConfig.exec}""")
        Some(session.sql(hiveConfig.exec))
      }
      // TODO: (darion.yaphet) Support Structured Streaming
      case SourceCategory.SOCKET => {
        val socketConfig = config.asInstanceOf[SocketSourceConfigEntry]
        LOG.warn("Socket streaming mode is not suitable for production environment")
        LOG.info(s"""Reading data stream from Socket ${socketConfig.host}:${socketConfig.port}""")
        Some(
          session.readStream
            .format("socket")
            .option("host", socketConfig.host)
            .option("port", socketConfig.port)
            .load()
        )
      }
      case SourceCategory.KAFKA => {
        val kafkaConfig = config.asInstanceOf[KafkaSourceConfigEntry]
        LOG.info(s"""Loading from Kafka ${kafkaConfig.server} and subscribe ${kafkaConfig.topic}""")
        Some(
          session.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaConfig.server)
            .option("subscribe", kafkaConfig.topic)
            .load()
        )
      }
      case _ => {
        LOG.error(s"Data source ${config.category} not supported")
        None
      }
    }
  }

  /**
    * Extra value from the row by field name.
    * When the field is null, we will fill it with default value.
    *
    * @param row   The row value.
    * @param field The field name.
    * @return
    */
  private[this] def extraValue(row: Row, field: String): Any = {
    val index = row.schema.fieldIndex(field)
    row.schema.fields(index).dataType match {
      case StringType =>
        if (!row.isNullAt(index)) {
          row.getString(index).mkString("\"", "", "\"")
        } else {
          "\"\""
        }
      case ShortType =>
        if (!row.isNullAt(index)) {
          row.getShort(index).toString
        } else {
          "0"
        }
      case IntegerType =>
        if (!row.isNullAt(index)) {
          row.getInt(index).toString
        } else {
          "0"
        }
      case LongType =>
        if (!row.isNullAt(index)) {
          row.getLong(index).toString
        } else {
          "0"
        }
      case FloatType =>
        if (!row.isNullAt(index)) {
          row.getFloat(index).toString
        } else {
          "0.0"
        }
      case DoubleType =>
        if (!row.isNullAt(index)) {
          row.getDouble(index).toString
        } else {
          "0.0"
        }
      case _: DecimalType =>
        if (!row.isNullAt(index)) {
          row.getDecimal(index).toString
        } else {
          "0.0"
        }
      case BooleanType =>
        if (!row.isNullAt(index)) {
          row.getBoolean(index).toString
        } else {
          "false"
        }
      case TimestampType =>
        if (!row.isNullAt(index)) {
          row.getTimestamp(index).getTime
        } else {
          "0"
        }
      case _: DateType =>
        if (!row.isNullAt(index)) {
          row.getDate(index).toString
        } else {
          "\"\""
        }
      case _: ArrayType =>
        if (!row.isNullAt(index)) {
          row.getSeq(index).mkString("\"[", ",", "]\"")
        } else {
          "\"[]\""
        }
      case _: MapType =>
        if (!row.isNullAt(index)) {
          row.getMap(index).mkString("\"{", ",", "}\"")
        } else {
          "\"{}\""
        }
    }
  }

  /**
    * Repartition the data frame using the specified partition number.
    *
    * @param frame
    * @param partition
    * @return
    */
  private[this] def repartition(frame: DataFrame, partition: Int): DataFrame = {
    if (partition > 0) {
      frame.repartition(partition).toDF
    } else {
      frame
    }
  }

  /**
    * Check the statement execution result.
    *
    * @param code The statement's execution result code.
    * @return
    */
  private[this] def isSuccessfully(code: Int) = code == ErrorCode.SUCCEEDED

  /**
    * Get the config by the path.
    *
    * @param config
    * @param path
    * @return
    */
  private[this] def getConfigOrNone(config: Config, path: String) = {
    if (config.hasPath(path)) {
      Some(config.getConfig(path))
    } else {
      None
    }
  }

  /**
    * Get the value from config by the path. If the path not exist, return the default value.
    *
    * @param config       The config.
    * @param path         The path of the config.
    * @param defaultValue The default value for the path.
    * @return
    */
  private[this] def getOrElse[T](config: Config, path: String, defaultValue: T): T = {
    if (config.hasPath(path)) {
      config.getAnyRef(path).asInstanceOf[T]
    } else {
      defaultValue
    }
  }

  /**
    * Get the value from config by the path which is optional.
    * If the path not exist, return the default value.
    *
    * @param config
    * @param path
    * @param defaultValue
    * @tparam T
    * @return
    */
  private[this] def getOptOrElse[T](config: Option[Config], path: String, defaultValue: T): T = {
    if (!config.isEmpty && config.get.hasPath(path)) {
      config.get.getAnyRef(path).asInstanceOf[T]
    } else {
      defaultValue
    }
  }

  /**
    * Calculate the coordinate's correlation id list.
    *
    * @param lat The latitude of coordinate.
    * @param lng The longitude of coordinate.
    * @return
    */
  private[this] def indexCells(lat: Double, lng: Double): IndexedSeq[Long] = {
    val coordinate = S2LatLng.fromDegrees(lat, lng)
    val s2CellId   = S2CellId.fromLatLng(coordinate)
    for (index <- DEFAULT_MIN_CELL_LEVEL to DEFAULT_MAX_CELL_LEVEL)
      yield s2CellId.parent(index).id()
  }
}
