package com.vesoft.tools

import java.sql.Date

import com.vesoft.client.NativeClient
import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options, ParseException, Option => CliOption}
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

/**
  * use spark to generate SstFiles in batch, which will be used by `load`.
  * We support following scenarios:
  *
  * <p>
  * hive: provide a mapping files which mapping hive tables to vertex's and edge's schema,
  * 1 hive table per vertex type OR edge type, while the columns will be used as tags,
  * which belong to the vertex OR edge
  * </p>
  */
object SparkSstFileGenerator {
  private[this] val log = LoggerFactory.getLogger(this.getClass)

  /**
    * configuration key for sst file output
    */
  val SSF_OUTPUT_DIR_CONF_KEY = "nebula.graph.spark.sst.file.dir"

  /**
    * cmd line's options, which's name following the convention: input will suffix "i", output will suffix "o"
    */
  lazy val options: Options = {
    val dataSourceTypeInput = CliOption.builder("di").longOpt("datasource_type_input")
      .hasArg()
      .desc("data source types support, must be among [hive|hbase|csv] for now, default=hive")
      .build

    val defaultColumnMapPolicy = CliOption.builder("ci").longOpt("default_column_mapping_policy")
      .hasArg()
      .desc("if mapping is missing, what policy to use when mapping column to property," +
        "all columns except primary_key's column will be mapped to tag's property with same name by default")
      .build

    val mappingFileInput = CliOption.builder("mi").longOpt("mapping_file_input")
      .required()
      .hasArg()
      .desc("hive tables to nebula graph schema mapping file")
      .build

    val sstFileOutput = CliOption.builder("so").longOpt("sst_file_output")
      .required()
      .hasArg()
      .desc("where the generated sst files will be put")
      .build

    val opts = new Options()
    opts.addOption(defaultColumnMapPolicy)
    opts.addOption(dataSourceTypeInput)
    opts.addOption(mappingFileInput)
    opts.addOption(sstFileOutput)
  }

  // cmd line formatter when something is wrong with options
  lazy val formatter = {
    val format = new HelpFormatter
    format.setWidth(200)
    format
  }

  val DefaultVersion = 1

  def main(args: Array[String]): Unit = {
    val parser = new DefaultParser

    var cmd: CommandLine = null
    try {
      cmd = parser.parse(options, args)
    }
    catch {
      case e: ParseException => {
        log.error("illegal args", e.getMessage)
        formatter.printHelp("nebula spark sst file generator", options)
        System.exit(-1)
      }
    }

    var dataSourceTypeInput: String = cmd.getOptionValue("di")
    if (dataSourceTypeInput == null) {
      dataSourceTypeInput = "hive"
    }

    var columnMapPolicy: String = cmd.getOptionValue("ci")
    if (columnMapPolicy == null) {
      columnMapPolicy = "hash_primary_key"
    }

    val mappingFileInput: String = cmd.getOptionValue("mi")
    var sstFileOutput: String = cmd.getOptionValue("so")
    while (sstFileOutput.endsWith("/")) {
      sstFileOutput = sstFileOutput.stripSuffix("/")
    }

    // parse mapping file
    val mappingConfiguration: MappingConfiguration = MappingConfiguration(mappingFileInput)

    val sparkConf = new SparkConf().setAppName("nebula-graph-sstFileGenerator")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    // to pass sst file dir to SstFileOutputFormat
    sc.getConf.set(SSF_OUTPUT_DIR_CONF_KEY, sstFileOutput)

    // all tags' data encoded,RDD[(PartitionID,(KeyEncoded,ValuesEncoded))]
    val tagsKVEncoded: RDD[(BytesWritable, (Int, String))] = mappingConfiguration.tags.zipWithIndex.map {
      //tag index used as tagId
      case (tag, tagId) => {
        //all column w/o PK column
        val allColumns: Seq[Column] = checkAndPopulateColumns(hiveContext, tag)

        val columnExpression =
          if (allColumns.size == 1) { // only PK columns defined, so fetch all cols
            "*"
          } else {
            s"${tag.primaryKey}," + allColumns.mkString(",")
          }

        val tagDF = hiveContext.sql(s"SELECT ${columnExpression} FROM ${tag.tableName}")
        //RDD[(businessKey->values)]
        val bizKeyAndValues: RDD[(String, Seq[Any])] = tagDF.map(row => {
          (row.getAs[String](tag.primaryKey) + "_" + tag.tableName, //businessId_tableName used as key before HASH
            allColumns.filter(_.columnName.equalsIgnoreCase(tag.primaryKey)).map(col => {
              col.`type` match {
                case "INTEGER" => row.getAs[Int](col.columnName)
                case "STRING" => row.getAs[String](col.columnName).getBytes("UTF-8") //native client can't decide string's charset
                case "FLOAT" => row.getAs[Float](col.columnName)
                case "LONG" => row.getAs[Long](col.columnName)
                case "DOUBLE" => row.getAs[Double](col.columnName)
                case "BOOL" => row.getAs[Boolean](col.columnName)
                case "DATE" => row.getAs[Date](col.columnName) //TODO: not support Date type yet
                case a@_ => throw new IllegalStateException(s"unsupported data type ${a}")
              }
            })
          )
        })

        bizKeyAndValues.map {
          case (key, values) => {
            val vertexId: Long = FNVHash.hash64(key)
            val partitionId: Int = (vertexId % mappingConfiguration.partitions).asInstanceOf[Int]
            val keyEncoded: Array[Byte] = NativeClient.createVertexKey(partitionId, vertexId, tagId, DefaultVersion)
            // TODO: need static NativeClient.encode here
            // val valuesEncoded:Array[Byte] = NativeClient.encode(values)
            val valuesEncoded: String = ""
            (new BytesWritable(keyEncoded), (partitionId, valuesEncoded)) //TODO:valuesEncoded should be of type BytesWritable
          }
        }
      }
    }.fold(sc.emptyRDD[(BytesWritable, (Int, String))])(_ ++ _)

    // should generate a sub dir per partitionId in each worker node, to allow a partition shuffles to every worker
    tagsKVEncoded.saveAsNewAPIHadoopFile(sstFileOutput, classOf[BytesWritable], classOf[PartitionIdAndValueBinaryWritable], classOf[SstFileOutputFormat])

    // TODO: handle edges


  }

  /**
    * check columns required in mapping configuration file should be defined in db(hive)
    * and its type is compatible, return all required column definitions, when not, throw exception
    *
    * @return all explicitly defined column or all columns(default), except PK column
    */
  private def checkAndPopulateColumns(hiveContext: HiveContext, tag: Tag): Seq[Column] = {
    val descriptionDF = hiveContext.sql(s"DESC ${tag.tableName}")
    // all columns' name ---> type mapping in db
    val allColumnsMapInDB: Map[String, String] = descriptionDF.map {
      case Row(colName: String, colType: String, _) => {
        (colName.toUpperCase, colType.toUpperCase)
      }
    }.collect.toMap


    //always check PK column
    if (allColumnsMapInDB.get(tag.primaryKey.toUpperCase).isEmpty) {
      throw new IllegalStateException(s"Tag=${tag.tagName}'s PK column: ${tag.primaryKey} not defined in db")
    }

    if (tag.columnMappings.isEmpty) { //only PK column checked, but all columns should be returned
      allColumnsMapInDB.filter(_._1.equalsIgnoreCase(tag.primaryKey)).map {
        case (colName, colType) => {
          Column(colName, colName, colType) // propertyName default=colName
        }
      }.toSeq
    }
    else { // PK + tag.columnMappings should be checked and returned
      val columnMappings = tag.columnMappings.get
      val notValid = columnMappings.filter(
        col => {
          val typeInDb = allColumnsMapInDB.get(col.columnName.toUpperCase)
          !typeInDb.isEmpty || !DataTypeCompatibility.isCompatible(col.`type`, typeInDb.get)
        }
      ).map {
        case col => s"name=${col.columnName},type=${col.`type`}"
      }

      if (notValid.nonEmpty) {
        throw new IllegalStateException(s"Tag=${tag.tagName}'s columns: ${notValid.mkString("\t")} not defined in or compatible with db's definitions")
      }
      else {
        columnMappings
      }
    }
  }
}
