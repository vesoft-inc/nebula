/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.col
import com.typesafe.config.{Config, ConfigFactory}
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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import util.control.Breaks._

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
object SparkClientGenerator {
  private[this] val LOG = Logger.getLogger(this.getClass)

  private[this] val BATCH_INSERT_TEMPLATE                           = "INSERT %s %s(%s) VALUES %s"
  private[this] val INSERT_VALUE_TEMPLATE                           = "%s: (%s)"
  private[this] val INSERT_VALUE_TEMPLATE_WITH_POLICY               = "%s(\"%s\"): (%s)"
  private[this] val ENDPOINT_TEMPLATE                               = "%s(\"%s\")"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE             = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE_WITH_POLICY = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE                             = "%s->%s@%d: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE_WITH_POLICY                 = "%s->%s@%d: (%s)"
  private[this] val USE_TEMPLATE                                    = "USE %s"

  private[this] val DEFAULT_BATCH                = 512
  private[this] val DEFAULT_PARTITION            = -1
  private[this] val DEFAULT_CONNECTION_TIMEOUT   = 3000
  private[this] val DEFAULT_CONNECTION_RETRY     = 3
  private[this] val DEFAULT_EXECUTION_RETRY      = 3
  private[this] val DEFAULT_EXECUTION_INTERVAL   = 3000
  private[this] val DEFAULT_EDGE_RANKING         = 0L
  private[this] val DEFAULT_ERROR_TIMES          = 16
  private[this] val DEFAULT_ERROR_OUTPUT_PATH    = "/tmp/nebula.writer.errors/"
  private[this] val DEFAULT_ERROR_MAX_BATCH_SIZE = Int.MaxValue
  private[this] val DEFAULT_RATE_LIMIT           = 1024
  private[this] val DEFAULT_RATE_TIMEOUT         = 100
  private[this] val NEWLINE                      = "\n"

  // GEO default config
  private[this] val DEFAULT_MIN_CELL_LEVEL = 10
  private[this] val DEFAULT_MAX_CELL_LEVEL = 18

  private[this] val MAX_CORES = 64

  private object Type extends Enumeration {
    type Type = Value
    val VERTEX = Value("VERTEX")
    val EDGE   = Value("EDGE")
  }

  private object KeyPolicy extends Enumeration {
    type POLICY = Value
    val HASH = Value("hash")
    val UUID = Value("uuid")
  }

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

    val config       = ConfigFactory.parseFile(c.config)
    val nebulaConfig = config.getConfig("nebula")
    val addresses    = nebulaConfig.getStringList("addresses").asScala.toList
    val user         = nebulaConfig.getString("user")
    val pswd         = nebulaConfig.getString("pswd")
    val space        = nebulaConfig.getString("space")

    val connectionConfig  = getConfigOrNone(nebulaConfig, "connection")
    val connectionTimeout = getOptOrElse(connectionConfig, "timeout", DEFAULT_CONNECTION_TIMEOUT)
    val connectionRetry   = getOptOrElse(connectionConfig, "retry", DEFAULT_CONNECTION_RETRY)

    val executionConfig   = getConfigOrNone(nebulaConfig, "execution")
    val executionRetry    = getOptOrElse(executionConfig, "retry", DEFAULT_EXECUTION_RETRY)
    val executionInterval = getOptOrElse(executionConfig, "interval", DEFAULT_EXECUTION_INTERVAL)

    val errorConfig  = getConfigOrNone(nebulaConfig, "error")
    val errorPath    = getOptOrElse(errorConfig, "output", DEFAULT_ERROR_OUTPUT_PATH)
    val errorMaxSize = getOptOrElse(errorConfig, "max", DEFAULT_ERROR_MAX_BATCH_SIZE)

    val rateConfig  = getConfigOrNone(nebulaConfig, "rate")
    val rateLimit   = getOptOrElse(rateConfig, "limit", DEFAULT_RATE_LIMIT)
    val rateTimeout = getOptOrElse(rateConfig, "timeout", DEFAULT_RATE_TIMEOUT)

    LOG.info(s"Nebula Addresses ${addresses} for ${user}:${pswd}")
    LOG.info(s"Connection Timeout ${connectionTimeout} Retry ${connectionRetry}")
    LOG.info(s"Execution Retry ${executionRetry} Interval Base ${executionInterval}")
    LOG.info(s"Error Path ${errorPath} Max Size ${errorMaxSize}")
    LOG.info(s"Switch to ${space}")

    val fs       = FileSystem.get(new Configuration())
    val hdfsPath = new Path(errorPath)
    try {
      if (!fs.exists(hdfsPath)) {
        LOG.info(s"Create HDFS directory: ${errorPath}")
        fs.mkdirs(hdfsPath)
      }
    } finally {
      fs.close()
    }

    if (config.hasPath("spark.cores.max") &&
        config.getInt("spark.cores.max") > MAX_CORES) {
      LOG.warn(s"Concurrency is higher than ${MAX_CORES}")
    }

    val session = SparkSession
      .builder()
      .appName(PROGRAM_NAME)

    val sparkConfig = config.getObject("spark")
    for (key <- sparkConfig.unwrapped().keySet().asScala) {
      val configKey = s"spark.${key}"
      if (config.getAnyRef(configKey).isInstanceOf[String]) {
        val sparkValue = config.getString(configKey)
        LOG.info(s"Spark Config Set ${configKey}:${sparkValue}")
        session.config(configKey, sparkValue)
      } else {
        for (subKey <- config.getObject(configKey).unwrapped().keySet().asScala) {
          val sparkKey   = s"${configKey}.${subKey}"
          val sparkValue = config.getString(sparkKey)
          LOG.info(s"Spark Config Set ${sparkKey}:${sparkValue}")
          session.config(sparkKey, sparkValue)
        }
      }
    }

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
          val hostAndPorts = addresses.map(HostAndPort.fromString).asJava
          val client = new AsyncGraphClientImpl(
            hostAndPorts,
            connectionTimeout,
            connectionRetry,
            executionRetry
          )
          client.setUser(user)
          client.setPassword(pswd)

          if (isSuccessfully(client.connect())) {
            val rateLimiter = RateLimiter.create(rateLimit)
            records.foreach { row =>
              val exec = row.getString(0)
              if (rateLimiter.tryAcquire(rateTimeout, TimeUnit.MILLISECONDS)) {
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
            LOG.error(s"Client connection failed. ${user}:${pswd}")
          }
        }
      sys.exit(0)
    }

    val tagConfigs = getConfigsOrNone(config, "tags")
    if (tagConfigs.isDefined) {
      for (tagConfig <- tagConfigs.get.asScala) {
        if (!tagConfig.hasPath("name") ||
            !tagConfig.hasPath("type")) {
          LOG.error("The `name` and `type` must be specified")
          break()
        }

        val tagName = tagConfig.getString("name")
        LOG.info(s"Processing Tag ${tagName}")

        val pathOpt = if (tagConfig.hasPath("path")) {
          Some(tagConfig.getString("path"))
        } else {
          None
        }

        val fields = tagConfig.getObject("fields").unwrapped

        // You can specified the vertex field name via the config item `vertex`
        // If you want to qualified the key policy, you can wrap them into a block.
        val vertex = if (tagConfig.hasPath("vertex.field")) {
          tagConfig.getString("vertex.field")
        } else {
          tagConfig.getString("vertex")
        }

        val policyOpt = if (tagConfig.hasPath("vertex.policy")) {
          val policy = tagConfig.getString("vertex.policy").toLowerCase
          Some(KeyPolicy.withName(policy))
        } else {
          None
        }

        val batch     = getOrElse(tagConfig, "batch", DEFAULT_BATCH)
        val partition = getOrElse(tagConfig, "partition", DEFAULT_PARTITION)

        val properties      = fields.asScala.values.map(_.toString).toList
        val valueProperties = fields.asScala.keys.toList

        val sourceProperties = if (!fields.containsKey(vertex)) {
          (fields.asScala.keySet + vertex).toList
        } else {
          fields.asScala.keys.toList
        }

        val nebulaProperties = properties.mkString(",")
        val data             = createDataSource(spark, pathOpt, tagConfig)

        if (data.isDefined && !c.dry) {
          val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.${tagName}")
          val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.${tagName}")

          repartition(data.get, partition)
            .map { row =>
              val values = (for {
                property <- valueProperties if property.trim.length != 0
              } yield extraValue(row, property)).mkString(",")
              (String.valueOf(extraIndexValue(row, vertex)), values)
            }(Encoders.tuple(Encoders.STRING, Encoders.STRING))
            .foreachPartition { iterator: Iterator[(String, String)] =>
              val service      = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))
              val hostAndPorts = addresses.map(HostAndPort.fromString).asJava
              val client = new AsyncGraphClientImpl(
                hostAndPorts,
                connectionTimeout,
                connectionRetry,
                executionRetry
              )
              client.setUser(user)
              client.setPassword(pswd)

              if (isSuccessfully(client.connect())) {
                val errorBuffer     = ArrayBuffer[String]()
                val switchSpaceCode = client.execute(USE_TEMPLATE.format(space)).get().get()
                if (isSuccessfully(switchSpaceCode)) {
                  val rateLimiter = RateLimiter.create(rateLimit)
                  val futures     = new ListBuffer[ListenableFuture[Optional[Integer]]]()
                  iterator.grouped(batch).foreach { tags =>
                    val exec = BATCH_INSERT_TEMPLATE.format(
                      Type.VERTEX.toString,
                      tagName,
                      nebulaProperties,
                      tags
                        .map { tag =>
                          if (policyOpt.isEmpty) {
                            INSERT_VALUE_TEMPLATE.format(tag._1, tag._2)
                          } else {
                            policyOpt.get match {
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
                    if (rateLimiter.tryAcquire(rateTimeout, TimeUnit.MILLISECONDS)) {
                      val future = client.execute(exec)
                      futures += future
                    } else {
                      batchFailure.add(1)
                      errorBuffer += exec
                      if (errorBuffer.size == errorMaxSize) {
                        throw TooManyErrorsException(s"Too Many Errors ${errorMaxSize}")
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
                          if (result.get == ErrorCode.SUCCEEDED) {
                            batchSuccess.add(1)
                          } else {
                            if (batchFailure.value > DEFAULT_ERROR_TIMES) {
                              throw TooManyErrorsException("too many errors")
                            } else {
                              batchFailure.add(1)
                            }
                          }
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
                    val errors     = fileSystem.create(new Path(s"${errorPath}/${tagName}"))

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
                  LOG.error(s"Switch ${space} Failed")
                }
              } else {
                LOG.error(s"Client connection failed. ${user}:${pswd}")
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

    val edgeConfigs = getConfigsOrNone(config, "edges")
    if (edgeConfigs.isDefined) {
      for (edgeConfig <- edgeConfigs.get.asScala) {
        if (!edgeConfig.hasPath("name") ||
            !edgeConfig.hasPath("type")) {
          LOG.error("The `name` and `type`must be specified")
          break()
        }

        val edgeName = edgeConfig.getString("name")
        LOG.info(s"Processing Edge ${edgeName}")

        val pathOpt = if (edgeConfig.hasPath("path")) {
          Some(edgeConfig.getString("path"))
        } else {
          None
        }

        val fields = edgeConfig.getObject("fields").unwrapped
        val isGeo  = checkGeoSupported(edgeConfig)
        val target = if (edgeConfig.hasPath("target.field")) {
          edgeConfig.getString("target.field")
        } else {
          edgeConfig.getString("target")
        }

        val targetPolicyOpt = if (edgeConfig.hasPath("target.policy")) {
          val policy = edgeConfig.getString("target.policy").toLowerCase
          Some(KeyPolicy.withName(policy))
        } else {
          None
        }

        val rankingOpt = if (edgeConfig.hasPath("ranking")) {
          Some(edgeConfig.getString("ranking"))
        } else {
          None
        }

        val batch           = getOrElse(edgeConfig, "batch", DEFAULT_BATCH)
        val partition       = getOrElse(edgeConfig, "partition", DEFAULT_PARTITION)
        val properties      = fields.asScala.values.map(_.toString).toList
        val valueProperties = fields.asScala.keys.toList

        val sourcePolicyOpt = if (edgeConfig.hasPath("source.policy")) {
          val policy = edgeConfig.getString("source.policy").toLowerCase
          Some(KeyPolicy.withName(policy))
        } else {
          None
        }

        val nebulaProperties = properties.mkString(",")
        val data             = createDataSource(spark, pathOpt, edgeConfig)
        if (data.isDefined && !c.dry) {
          val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.${edgeName}")
          val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.${edgeName}")

          repartition(data.get, partition)
            .map { row =>
              val sourceField = if (!isGeo) {
                val source = if (edgeConfig.hasPath("source.field")) {
                  edgeConfig.getString("source.field")
                } else {
                  edgeConfig.getString("source")
                }

                if (sourcePolicyOpt.isEmpty) {
                  row.getLong(row.schema.fieldIndex(source)).toString
                } else {
                  row.getString(row.schema.fieldIndex(source))
                }
              } else {
                val latitude  = edgeConfig.getString("latitude")
                val longitude = edgeConfig.getString("longitude")
                val lat       = row.getDouble(row.schema.fieldIndex(latitude))
                val lng       = row.getDouble(row.schema.fieldIndex(longitude))
                indexCells(lat, lng).mkString(",")
              }

              val targetField =
                if (targetPolicyOpt.isEmpty) {
                  row.getLong(row.schema.fieldIndex(target)).toString
                } else {
                  row.getString(row.schema.fieldIndex(target))
                }

              val values =
                (for {
                  property <- valueProperties if property.trim.length != 0
                } yield extraValue(row, property)).mkString(",")

              if (rankingOpt.isDefined) {
                val ranking = row.getLong(row.schema.fieldIndex(rankingOpt.get))
                (sourceField, targetField, ranking, values)
              } else {
                (sourceField, targetField, DEFAULT_EDGE_RANKING, values)
              }
            }(
              Encoders
                .tuple(Encoders.STRING, Encoders.STRING, Encoders.scalaLong, Encoders.STRING)
            )
            .foreachPartition { iterator: Iterator[(String, String, Long, String)] =>
              val service      = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))
              val hostAndPorts = addresses.map(HostAndPort.fromString).asJava
              val client = new AsyncGraphClientImpl(
                hostAndPorts,
                connectionTimeout,
                connectionRetry,
                executionRetry
              )

              client.setUser(user)
              client.setPassword(pswd)
              if (isSuccessfully(client.connect())) {
                val switchSpaceCode = client.switchSpace(space).get().get()
                if (isSuccessfully(switchSpaceCode)) {
                  val errorBuffer = ArrayBuffer[String]()
                  val rateLimiter = RateLimiter.create(rateLimit)
                  val futures     = new ListBuffer[ListenableFuture[Optional[Integer]]]()
                  iterator.grouped(batch).foreach { edges =>
                    val values =
                      if (rankingOpt.isEmpty) {
                        edges
                          .map { edge =>
                            // TODO: (darion.yaphet) dataframe.explode() would be better ?
                            (for (source <- edge._1.split(","))
                              yield
                                if (sourcePolicyOpt.isEmpty && targetPolicyOpt.isEmpty) {
                                  EDGE_VALUE_WITHOUT_RANKING_TEMPLATE
                                    .format(source, edge._2, edge._4)
                                } else {
                                  val source = if (sourcePolicyOpt.isDefined) {
                                    sourcePolicyOpt.get match {
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

                                  val target = if (targetPolicyOpt.isDefined) {
                                    targetPolicyOpt.get match {
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
                                if (sourcePolicyOpt.isEmpty && targetPolicyOpt.isEmpty) {
                                  EDGE_VALUE_TEMPLATE
                                    .format(source, edge._2, edge._3, edge._4)
                                } else {
                                  val source = sourcePolicyOpt.get match {
                                    case KeyPolicy.HASH =>
                                      ENDPOINT_TEMPLATE.format(KeyPolicy.HASH.toString, edge._1)
                                    case KeyPolicy.UUID =>
                                      ENDPOINT_TEMPLATE.format(KeyPolicy.UUID.toString, edge._1)
                                    case _ =>
                                      edge._1
                                  }

                                  val target = targetPolicyOpt.get match {
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
                      .format(Type.EDGE.toString, edgeName, nebulaProperties, values)
                    LOG.info(s"Exec : ${exec}")
                    if (rateLimiter.tryAcquire(rateTimeout, TimeUnit.MILLISECONDS)) {
                      val future = client.execute(exec)
                      futures += future
                    } else {
                      batchFailure.add(1)
                      LOG.debug("Save the error execution sentence into buffer")
                      errorBuffer += exec
                      if (errorBuffer.size == errorMaxSize) {
                        throw TooManyErrorsException(s"Too Many Errors ${errorMaxSize}")
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
                          if (result.get == ErrorCode.SUCCEEDED) {
                            batchSuccess.add(1)
                          } else {
                            if (batchFailure.value > DEFAULT_ERROR_TIMES) {
                              throw TooManyErrorsException("too many errors")
                            } else {
                              batchFailure.add(1)
                            }
                          }
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
                    val errors     = fileSystem.create(new Path(s"${errorPath}/${edgeName}"))
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
                  LOG.error(s"Switch ${space} Failed")
                }
              } else {
                LOG.error(s"Client connection failed. ${user}:${pswd}")
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
    * @param pathOpt The path for config.
    * @param config  The config.
    * @return
    */
  private[this] def createDataSource(
      session: SparkSession,
      pathOpt: Option[String],
      config: Config
  ): Option[DataFrame] = {
    val `type` = config.getString("type")

    pathOpt match {
      case Some(path) =>
        LOG.info(s"""Loading ${`type`} from ${path}""")
        `type`.toLowerCase match {
          case "parquet" => Some(session.read.parquet(path))
          case "orc"     => Some(session.read.orc(path))
          case "json"    => Some(session.read.json(path))
          case "csv"     => Some(session.read.csv(path))
          case _ => {
            LOG.error(s"Data source ${`type`} not supported")
            None
          }
        }
      case None => {
        `type`.toLowerCase match {
          case "hive" => {
            if (!config.hasPath("exec")) {
              LOG.error("Reading from hive should setting SQL command")
              None
            }

            val exec = config.getString("exec")
            LOG.info(s"""Loading from Hive and exec ${exec}""")
            Some(session.sql(exec))
          }
          // TODO: (darion.yaphet) Support Structured Streaming
          case "socket" => {
            LOG.warn("Socket streaming mode is not suitable for production environment")
            if (!config.hasPath("host") || !config.hasPath("port")) {
              LOG.error("Reading socket source should specify host and port")
              None
            }

            val host = config.getString("host")
            val port = config.getInt("port")
            LOG.info(s"""Reading data stream from Socket ${host}:${port}""")
            Some(
              session.readStream
                .format("socket")
                .option("host", host)
                .option("port", port)
                .load()
            )
          }
          case "kafka" => {
            if (!config.hasPath("servers") || !config.hasPath("topic")) {
              LOG.error("Reading kafka source should specify servers and topic")
              None
            }

            val server = config.getString("server")
            val topic  = config.getString("kafka")
            LOG.info(s"""Loading from Kafka ${server} and subscribe ${topic}""")
            Some(
              session.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", topic)
                .load()
            )
          }
          case _ => {
            LOG.error(s"Data source ${`type`} not supported")
            None
          }
        }
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
          row.getString(index)
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .mkString("\"", "", "\"")
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
    * Extra Index value from the row by field name.
    * When the field is null, we will fill it with default value.
    *
    * @param row   The row value.
    * @param field The field name.
    * @return
    */
  private[this] def extraIndexValue(row: Row, field: String): Any = {
    val index = row.schema.fieldIndex(field)
    row.schema.fields(index).dataType match {
      case StringType =>
        if (!row.isNullAt(index)) {
          row.getString(index)
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
        } else {
          ""
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
          ""
        }
      case _: ArrayType =>
        if (!row.isNullAt(index)) {
          row.getSeq(index).mkString("[", ",", "]")
        } else {
          "[]"
        }
      case _: MapType =>
        if (!row.isNullAt(index)) {
          row.getMap(index).mkString("{", ",", "}")
        } else {
          "{}"
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
    * Whether the edge is Geo supported.
    *
    * @param edgeConfig The config of edge.
    * @return
    */
  private[this] def checkGeoSupported(edgeConfig: Config): Boolean = {
    !edgeConfig.hasPath("source") &&
    edgeConfig.hasPath("latitude") &&
    edgeConfig.hasPath("longitude")
  }

  /**
    * Get the config list by the path.
    *
    * @param config The config.
    * @param path   The path of the config.
    * @return
    */
  private[this] def getConfigsOrNone(config: Config, path: String) = {
    if (config.hasPath(path)) {
      Some(config.getConfigList(path))
    } else {
      None
    }
  }

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

