/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2

import java.io.File

import com.google.common.geometry.{S2CellId, S2LatLng}
import com.google.common.net.HostAndPort
import com.typesafe.config.{Config, ConfigFactory}
import com.vesoft.nebula.graph.ErrorCode
import com.vesoft.nebula.graph.client.GraphClientImpl
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.Breaks._

case class Argument(
    config: File = new File("application.conf"),
    hive: Boolean = false,
    directly: Boolean = false,
    dry: Boolean = false
)

/**
  * SparkClientGenerator is a simple spark job used to write data into Nebula Graph parallel.
  */
object SparkClientGenerator {
  private[this] val LOG = LoggerFactory.getLogger(this.getClass)

  //nSQL reference:
  //https://github.com/vesoft-inc/nebula/blob/master/docs/manual-EN/1.overview/2.quick-start/1.get-started.md
  private[this] val BATCH_INSERT_TEMPLATE               = "INSERT %s %s(%s) VALUES %s"
  private[this] val INSERT_VALUE_TEMPLATE               = "%s: (%s)"
  private[this] val EDGE_VALUE_WITHOUT_RANKING_TEMPLATE = "%s->%s: (%s)"
  private[this] val EDGE_VALUE_TEMPLATE                 = "%s->%s@%d: (%s)"
  private[this] val USE_TEMPLATE                        = "USE %s"

  private[this] val DEFAULT_BATCH              = 16
  private[this] val DEFAULT_PARTITION          = -1
  private[this] val DEFAULT_CONNECTION_TIMEOUT = 3000
  private[this] val DEFAULT_CONNECTION_RETRY   = 3
  private[this] val DEFAULT_EXECUTION_RETRY    = 3
  private[this] val DEFAULT_EXECUTION_INTERVAL = 3000
  private[this] val DEFAULT_EDGE_RANKING       = 0L

  // GEO default config
  private[this] val DEFAULT_MIN_CELL_LEVEL = 5
  private[this] val DEFAULT_MAX_CELL_LEVEL = 24

  private[this] val MAX_CORES = 64

  private object Type extends Enumeration {
    type Type = Value
    val Vertex = Value("Vertex")
    val Edge   = Value("Edge")
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

    val connectionTimeout =
      getOrElse(nebulaConfig, "connection.timeout", DEFAULT_CONNECTION_TIMEOUT)
    val connectionRetry = getOrElse(nebulaConfig, "connection.retry", DEFAULT_CONNECTION_RETRY)

    val executionRetry = getOrElse(nebulaConfig, "execution.retry", DEFAULT_EXECUTION_RETRY)
    val executionInterval =
      getOrElse(nebulaConfig, "execution.interval", DEFAULT_EXECUTION_INTERVAL)

    LOG.info(s"Nebula Addresses ${addresses} for ${user}:${pswd}")
    LOG.info(s"Connection Timeout ${connectionTimeout} Retry ${connectionRetry}")
    LOG.info(s"Execution Retry ${executionRetry} Interval Base ${executionInterval}")
    LOG.info(s"Switch to ${space}")

    val session = SparkSession
      .builder()
      .appName(PROGRAM_NAME)

    if (config.hasPath("spark.cores.max") &&
        config.getInt("spark.cores.max") > MAX_CORES) {
      LOG.warn(s"Concurrency is higher than ${MAX_CORES}")
    }

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

    val spark =
      if (c.hive) {
        session.enableHiveSupport().getOrCreate()
      } else session.getOrCreate()

    val tagConfigs = getConfigOrNone(config, "tags")
    if (tagConfigs.isEmpty) {
      LOG.warn("Tag is not defined")
    } else {
      for (tagName <- tagConfigs.get.unwrapped.keySet.asScala) {
        LOG.info(s"Processing Tag `${tagName}`")
        val tagConfig = config.getConfig(s"tags.${tagName}")
        if (!tagConfig.hasPath("type")) {
          LOG.error(s"Tag:`${tagName}`'s type must be specified")
          break()
        }

        val pathOpt = if (tagConfig.hasPath("path")) {
          Some(tagConfig.getString("path"))
        } else {
          None
        }

        val fields = tagConfig.getObject("fields").unwrapped
        val isUseUuidOpt = if (tagConfig.hasPath("use_uuid")) {
          Some(tagConfig.getBoolean("use_uuid"))
        } else {
          None
        }

        val discriminatorPrefixOpt = if (tagConfig.hasPath("discriminator_prefix")) {
          Some(tagConfig.getString("discriminator_prefix"))
        } else {
          None
        }

        val vertex    = tagConfig.getString("vertex")
        val batch     = getOrElse(tagConfig, "batch", DEFAULT_BATCH)
        val partition = getOrElse(tagConfig, "partition", DEFAULT_PARTITION)

        val tagPropertyNames = fields.asScala.values.map(_.toString).toList
        val tableColumnNames = fields.asScala.keys.filterNot(_.trim.isEmpty).toList

        val sourceProperties = if (!fields.containsKey(vertex)) {
          (fields.asScala.keySet + vertex).toList
        } else {
          fields.asScala.keys.toList
        }

        val vertexIndex      = sourceProperties.indexOf(vertex)
        val nebulaProperties = tagPropertyNames.mkString(",")

        val dataSource        = createDataSource(spark, pathOpt, tagConfig)
        val idGeneratorFormat = isUseUuidOpt.fold("%s")(_ => "uuid(\"%s\")")

        if (dataSource.isDefined && !c.dry) {
          repartition(dataSource.get, partition)
            .select(sourceProperties.map(col): _*)
            .map { row =>
              (
                idGeneratorFormat.format(
                  discriminatorPrefixOpt
                    .fold(row.getString(vertexIndex))(prefix => prefix + row.getString(vertexIndex))
                ),
                tableColumnNames
                  .map(extractValue(row, _))
                  .mkString(",")
              )
            }(Encoders.tuple(Encoders.STRING, Encoders.STRING))
            .foreachPartition { iterator: Iterator[(String, String)] =>
              val hostAndPorts = addresses.map(HostAndPort.fromString).asJava
              val client =
                new GraphClientImpl(
                  hostAndPorts,
                  connectionTimeout,
                  connectionRetry,
                  executionRetry
                )

              if (isSuccessfully(client.connect(user, pswd))) {
                if (isSuccessfully(client.execute(USE_TEMPLATE.format(space)))) {
                  iterator.grouped(batch).foreach { tags =>
                    val exec = BATCH_INSERT_TEMPLATE.format(
                      Type.Vertex.toString,
                      tagName,
                      nebulaProperties,
                      tags
                        .map { tag =>
                          INSERT_VALUE_TEMPLATE.format(tag._1, tag._2)
                        }
                        .mkString(", ")
                    )

                    LOG.debug(s"Exec: ${exec}")

                    breakable {
                      for (time <- 1 to executionRetry
                           if isSuccessfullyWithSleep({
                             client.execute(exec)
                           }, time * executionInterval + Random.nextInt(10) * 100L)(exec)) {
                        break
                      }
                    }
                  }
                } else {
                  LOG.error(s"Switch ${space} Failed")
                }
                client.close()
              } else {
                LOG.error(s"Client connection failed. ${user}:${pswd}")
              }
            }
        }
      }
    }

    val edgeConfigsOpt = getConfigOrNone(config, "edges")
    if (edgeConfigsOpt.isEmpty) {
      LOG.warn("Edge is not defined")
    } else
      for (edgeName <- edgeConfigsOpt.get.unwrapped.keySet.asScala) {
        LOG.info(s"Processing Edge ${edgeName}")
        val edgeConfig = config.getConfig(s"edges.${edgeName}")
        if (!edgeConfig.hasPath("type")) {
          LOG.error(s"Edge:`${edgeName}`s type must be specified")
          break()
        }

        val pathOpt = if (edgeConfig.hasPath("path")) {
          Some(edgeConfig.getString("path"))
        } else {
          LOG.warn(s"Edge:`${edgeName}`s path is not setting")
          None
        }

        val fields = edgeConfig.getObject("fields").unwrapped
        val isGeo  = checkGeoSupported(edgeConfig)
        val target = edgeConfig.getString("target")
        val rankingOpt = if (edgeConfig.hasPath("ranking")) {
          Some(edgeConfig.getString("ranking"))
        } else {
          None
        }

        val batch     = getOrElse(edgeConfig, "batch", DEFAULT_BATCH)
        val partition = getOrElse(edgeConfig, "partition", DEFAULT_PARTITION)
        val isUseUuid = if (edgeConfig.hasPath("use_uuid")) {
          edgeConfig.getBoolean("use_uuid")
        } else {
          false
        }

        val discriminatorPrefixSrcOpt = if (edgeConfig.hasPath("discriminator_prefix_src")) {
          Some(edgeConfig.getString("discriminator_prefix_src"))
        } else {
          None
        }

        val discriminatorPrefixDestOpt = if (edgeConfig.hasPath("discriminator_prefix_dest")) {
          Some(edgeConfig.getString("discriminator_prefix_dest"))
        } else {
          None
        }

        val edgeProperties   = fields.asScala.values.map(_.toString).toList
        val tableColumnNames = fields.asScala.keys.filterNot(_.trim.isEmpty).toList

        val sourceProperties = if (!isGeo) {
          val source = edgeConfig.getString("source")
          if (!fields.containsKey(source) ||
              !fields.containsKey(target)) {
            (fields.asScala.keySet + source + target).toList
          } else {
            fields.asScala.keys.toList
          }
        } else {
          val latitude  = edgeConfig.getString("latitude")
          val longitude = edgeConfig.getString("longitude")
          if (!fields.containsKey(latitude) ||
              !fields.containsKey(longitude) ||
              !fields.containsKey(target)) {
            (fields.asScala.keySet + latitude + longitude + target).toList
          } else {
            fields.asScala.keys.toList
          }
        }

        val nebulaProperties = edgeProperties.mkString(",")

        val dataSource = createDataSource(spark, pathOpt, edgeConfig)
        val encoder =
          Encoders.tuple(Encoders.STRING, Encoders.STRING, Encoders.scalaLong, Encoders.STRING)

        if (dataSource.isDefined && !c.dry) {
          repartition(dataSource.get, partition)
            .select(sourceProperties.map(col): _*)
            .map { row =>
              val sourceField = if (!isGeo) {
                val source = edgeConfig.getString("source")
                row.getString(row.schema.fieldIndex(source))
              } else {
                val latitude  = edgeConfig.getString("latitude")
                val longitude = edgeConfig.getString("longitude")
                val lat       = row.getDouble(row.schema.fieldIndex(latitude))
                val lng       = row.getDouble(row.schema.fieldIndex(longitude))
                indexCells(lat, lng).mkString(",")
              }

              val targetField = row.getString(row.schema.fieldIndex(target)) // Could be non-integer
              val values      = tableColumnNames.map(extractValue(row, _)).mkString(",")

              rankingOpt.fold((sourceField, targetField, DEFAULT_EDGE_RANKING, values)) { rank =>
                val ranking = row.getLong(row.schema.fieldIndex(rank))
                (sourceField, targetField, ranking, values)
              }
            }(encoder)
            .foreachPartition { iterator: Iterator[(String, String, Long, String)] =>
              {
                val hostAndPorts = addresses.map(HostAndPort.fromString).asJava
                val client =
                  new GraphClientImpl(
                    hostAndPorts,
                    connectionTimeout,
                    connectionRetry,
                    executionRetry
                  )

                if (isSuccessfully(client.connect(user, pswd))) {
                  if (isSuccessfully(client.execute(USE_TEMPLATE.format(space)))) {
                    iterator.grouped(batch).foreach { edges =>
                      val values =
                        if (rankingOpt.isEmpty)
                          edges
                            .map { edge =>
                              // TODO: (darion.yaphet) dataframe.explode() would be better ?
                              edge._1
                                .split(",")
                                .map { source =>
                                  val sourceIdPrefixDecorated = discriminatorPrefixSrcOpt
                                    .fold(source)(srcPrefix => srcPrefix + source)

                                  val destIdPrefixDecorated = discriminatorPrefixDestOpt
                                    .fold(edge._2.toString)(
                                      destPrefix => destPrefix + edge._2.toString
                                    )

                                  val (sourceIdUuidDecorated, destIdUuidDecorated) =
                                    if (isUseUuid)
                                      (
                                        s"""uuid("${sourceIdPrefixDecorated}")""",
                                        s"""uuid("${destIdPrefixDecorated}")"""
                                      )
                                    else (sourceIdPrefixDecorated, destIdPrefixDecorated)

                                  EDGE_VALUE_WITHOUT_RANKING_TEMPLATE
                                    .format(sourceIdUuidDecorated, destIdUuidDecorated, edge._4)
                                }
                                .mkString(", ")
                            }
                            .toList
                            .mkString(", ")
                        else
                          edges
                            .map { edge =>
                              // TODO: (darion.yaphet) dataframe.explode() would be better ?
                              edge._1
                                .split(",")
                                .map { source =>
                                  val sourceIdPrefixDecorated = discriminatorPrefixSrcOpt
                                    .fold(source)(srcPrefix => srcPrefix + source)

                                  val destIdPrefixDecorated = discriminatorPrefixDestOpt
                                    .fold(edge._2.toString)(
                                      destPrefix => destPrefix + edge._2.toString
                                    )

                                  val (sourceIdUuidDecorated, destIdUuidDecorated) =
                                    if (isUseUuid)
                                      (
                                        s"""uuid("${sourceIdPrefixDecorated}")""",
                                        s"""uuid("${destIdPrefixDecorated}")"""
                                      )
                                    else (sourceIdPrefixDecorated, destIdPrefixDecorated)
                                  EDGE_VALUE_WITHOUT_RANKING_TEMPLATE
                                    .format(sourceIdUuidDecorated, destIdUuidDecorated, edge._4)
                                }
                                .mkString(", ")
                            }
                            .toList
                            .mkString(", ")

                      val exec = BATCH_INSERT_TEMPLATE
                        .format(Type.Edge.toString, edgeName, nebulaProperties, values)

                      LOG.debug(s"Exec: ${exec}")
                      breakable {
                        for (time <- 1 to executionRetry
                             if isSuccessfullyWithSleep(
                               client.execute(exec),
                               time * executionInterval + Random.nextInt(10) * 100L
                             )(exec)) {
                          break
                        }
                      }
                    }
                  } else {
                    LOG.error(s"Switch ${space} Failed")
                  }
                  client.close()
                } else {
                  LOG.error(s"Client connection failed. ${user}:${pswd}")
                }
              }
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
  ) = {
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
            } else {
              val exec = config.getString("exec")
              LOG.info(s"""Loading from Hive and exec ${exec}""")
              Some(session.sql(exec))
            }
          }
          // TODO: (darion.yaphet) Support Structured Streaming
          case "socket" => {
            LOG.warn("Socket streaming mode is not suitable for production environment")
            if (!config.hasPath("host") || !config.hasPath("port")) {
              LOG.error("Reading socket source should specify host and port")
              None
            } else {
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
          }

          case "kafka" => {
            if (!config.hasPath("servers") || !config.hasPath("topic")) {
              LOG.error("Reading kafka source should specify servers and topic")
              None
            } else {
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

          }
          case _ => {
            LOG.error(s"Data source ${`type`} not supported")
            None
          }
        }
      }
    }
  }

  //TODO: Is it reasonable to default every null field? Must support nullable property
  /**
    * Extract value from the row by field name.
    * When the field is null, we will fill it with default value.
    *
    * @param row   The row value.
    * @param field The field name.
    * @return
    */
  private[this] def extractValue(row: Row, field: String) = {
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
  @inline
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
  @inline
  private[this] def isSuccessfully(code: Int) = code == ErrorCode.SUCCEEDED

  /**
    * Check the statement execution result.
    * If the result code is not SUCCEEDED, will sleep a little while.
    *
    * @param code     The sentence execute's result code.
    * @param interval The sleep interval.
    * @return
    */
  @inline
  private[this] def isSuccessfullyWithSleep(code: Int, interval: Long)(
      implicit exec: String
  ): Boolean = {
    val result = isSuccessfully(code)
    if (!result) {
      LOG.error(s"Exec Failed: ${exec} retry interval ${interval}")
      Thread.sleep(interval)
    }
    result
  }

  /**
    * Whether the edge is Geo supported.
    *
    * @param edgeConfig The config of edge.
    * @return
    */
  @inline
  private[this] def checkGeoSupported(edgeConfig: Config) = {
    !edgeConfig.hasPath("source") &&
    edgeConfig.hasPath("latitude") &&
    edgeConfig.hasPath("longitude")
  }

  /**
    * Get the config by the path.
    *
    * @param config The config.
    * @param path   The path of the config.
    * @return
    */
  @inline
  private[this] def getConfigOrNone(config: Config, path: String) = {
    if (config.hasPath(path)) {
      Some(config.getObject(path))
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
  @inline
  private[this] def getOrElse[T](config: Config, path: String, defaultValue: T) = {
    if (config.hasPath(path)) {
      config.getAnyRef(path).asInstanceOf[T]
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
  private[this] def indexCells(lat: Double, lng: Double) = {
    val coordinate = S2LatLng.fromDegrees(lat, lng)
    val s2CellId   = S2CellId.fromLatLng(coordinate)
    (DEFAULT_MIN_CELL_LEVEL to DEFAULT_MAX_CELL_LEVEL).map(s2CellId.parent(_).id())
  }
}
