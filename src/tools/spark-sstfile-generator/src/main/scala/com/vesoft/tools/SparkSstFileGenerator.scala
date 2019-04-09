package com.vesoft.tools

import com.vesoft.client.NativeClient
import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options, ParseException, Option => CliOption}
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * use spark to generate SstFiles in batch, which will be used by `load`.
  * We support following cases:
  *
  * <p>
  * hive: provide a mapping files which mapping hive tables to vertex's and edge's schema,
  * one hive table per Vertex type or Edge type, which has a single Tag with all remaining columns as
  * tag's properties, while the columns will be used as properties,
  * which belong to the Tag or Edge.
  *
  * after job complete, following dir structure will be generated；
  * worker_node1
  *       |
  *       |-sstFileOutput
  *       |
  *       |--1
  *       |  |
  *       |  |——vertex.data
  *       |  |--edge.data
  *       |
  *       |--2
  *       |
  *       |——vertex.data
  *       |--edge.data
  * worker_node2
  *       |
  *       |-sstFileOutput
  *       |
  *       |--1
  *       |  |
  *       |  |——vertex.data
  *       |  |--edge.data
  *       |
  *       |--2
  *       |
  *       |——vertex.data
  *       |--edge.data
  * </p>
  */
object SparkSstFileGenerator {
  private[this] val log = LoggerFactory.getLogger(this.getClass)

  /**
    * configuration key for sst file output
    */
  val SSF_OUTPUT_DIR_CONF_KEY = "nebula.graph.spark.sst.file.dir"

  /**
    * cmd line's options, which's name following the convention: input will suffix with "i", output will suffix with "o"
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

    val datePartitionKey = CliOption.builder("pi").longOpt("date_partition_input")
      .required()
      .hasArg()
      .desc("table partitioned by a date field")
      .build

    // when the newest data arrive
    val latestDate = CliOption.builder("li").longOpt("latest_date_input")
      .required()
      .hasArg()
      .desc("latest date to query")
      .build

    val opts = new Options()
    opts.addOption(defaultColumnMapPolicy)
    opts.addOption(dataSourceTypeInput)
    opts.addOption(mappingFileInput)
    opts.addOption(sstFileOutput)
    opts.addOption(datePartitionKey)
    opts.addOption(latestDate)
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
        log.error("illegal arguments", e)
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

    //when date partition is used, we should use the LATEST data
    val datePartitionKey: String = cmd.getOptionValue("pi")
    val latestDate = cmd.getOptionValue("li")

    // parse mapping file
    val mappingConfiguration: MappingConfiguration = MappingConfiguration(mappingFileInput)

    val sparkConf = new SparkConf().setAppName("nebula-graph-sstFileGenerator")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    // to pass sst file dir to SstFileOutputFormat
    sc.getConf.set(SSF_OUTPUT_DIR_CONF_KEY, sstFileOutput)
    // disable hadoop output compression, cause rocksdb can't recognize it
    sc.getConf.set(FileOutputFormat.COMPRESS, "false")

    // id generator lambda, use FNV hash for now
    //TODO: support id generator function other than FNV hash
    //TODO: what need to be done when hash collision, might cause data corruption
    val idGeneratorFunction = mappingConfiguration.keyPolicy.map(_.toLowerCase) match {
      case Some("hash_primary_key") => (key: String) => FNVHash.hash64(key)
      case Some(a@_) => throw new IllegalStateException(s"not supported key generator=${a}")
      case None => (key: String) => FNVHash.hash64(key)
    }

    //1) handle vertex, encode all column except PK column as a single Tag's properties,RDD[(PartitionID,(KeyEncoded,ValuesEncoded))]
    val tagsKVEncoded: RDD[(BytesWritable, PartitionIdAndValueBinaryWritable)] = mappingConfiguration.tags.zipWithIndex.map {
      //tag index used as tagId
      case (tag, tagType) => {
        //all column w/o PK column
        val (allColumns, partitionCols) = validateColumns(sqlContext, tag, Seq(tag.primaryKey), Seq(tag.primaryKey), mappingConfiguration.databaseName)
        val columnExpression = {
          assert(allColumns.size > 0) // should have columns defined
          s"${tag.primaryKey}," + allColumns.map(_.columnName).mkString(",")
        }

        val whereClause = tag.typePartitionKey.map(key => s"${key}='${tag.name}' AND ${datePartitionKey}='${latestDate}'").getOrElse(s"${datePartitionKey}='${latestDate}'")
        val tagDF = sqlContext.sql(s"SELECT ${columnExpression} FROM ${mappingConfiguration.databaseName}.${tag.tableName} WHERE ${whereClause}") //TODO:to handle multiple partition columns' Cartesian product
        //RDD[(businessKey->values)]
        val bizKeyAndValues: RDD[(String, Seq[AnyRef])] = tagDF.map(row => {
          (row.getAs[String](tag.primaryKey) + "_" + tag.tableName, //businessId_tableName used as key before HASH
            allColumns.filter(!_.columnName.equalsIgnoreCase(tag.primaryKey)).map(col => {
              col.`type`.toUpperCase match {
                case "INTEGER" => Int.box(row.getAs[Int](col.columnName))
                case "STRING" => row.getAs[String](col.columnName).getBytes("UTF-8") //native client don't know string's charset
                case "FLOAT" => Float.box(row.getAs[Float](col.columnName))
                case "LONG" => Long.box(row.getAs[Long](col.columnName))
                case "DOUBLE" => Double.box(row.getAs[Double](col.columnName))
                case "BOOL" => Boolean.box(row.getAs[Boolean](col.columnName))
                //case "DATE" => row.getAs[Date](col.columnName) //TODO: not support Date type yet
                case a@_ => throw new IllegalStateException(s"unsupported tag data type ${a}")
              }
            })
          )
        })

        bizKeyAndValues.map {
          case (key, values) => {
            val vertexId: Long = idGeneratorFunction.apply(key) //apply id generator function
            val partitionId: Int = (vertexId % mappingConfiguration.partitions).asInstanceOf[Int]
            val keyEncoded: Array[Byte] = NativeClient.createVertexKey(partitionId, vertexId, tagType, DefaultVersion)
            // use native client
            val valuesEncoded: Array[Byte] = NativeClient.encoded(values.toArray)
            (new BytesWritable(keyEncoded), new PartitionIdAndValueBinaryWritable(partitionId, new BytesWritable(valuesEncoded)))
          }
        }
      }
    }.fold(sc.emptyRDD[(BytesWritable, PartitionIdAndValueBinaryWritable)])(_ ++ _) // concat all vertex data

    // should generate a sub dir per partitionId in each worker node, to allow that a partition is shuffled to every worker
    tagsKVEncoded.saveAsNewAPIHadoopFile(sstFileOutput, classOf[BytesWritable], classOf[PartitionIdAndValueBinaryWritable], classOf[SstFileOutputFormat])

    //2)  handle edges
    val edgesKVEncoded: RDD[(BytesWritable, PartitionIdAndValueBinaryWritable)] = mappingConfiguration.edges.zipWithIndex.map {
      //edge index used as edge_type
      case (edge, edgeType) => {
        //all column w/o PK column
        val (allColumns, partitionColumns) = validateColumns(sqlContext, edge, Seq(edge.fromForeignKeyColumn), Seq(edge.fromForeignKeyColumn, edge.toForeignKeyColumn), mappingConfiguration.databaseName)

        val columnExpression = {
          assert(allColumns.size > 0)
          s"${edge.fromForeignKeyColumn},${edge.toForeignKeyColumn}" + allColumns.mkString(",")
        }

        val whereClause = edge.typePartitionKey.map(key => s"${key}='${edge.name}' AND ${datePartitionKey}='${latestDate}'").getOrElse(s"${datePartitionKey}='${latestDate}'")

        //TODO: join FROM_COLUMN and join TO_COLUMN from the table where this columns referencing, to make sure that the claimed id really exists in the reference table.BUT with HUGE Perf penalty
        val edgeDf = sqlContext.sql(s"SELECT ${columnExpression} FROM ${mappingConfiguration.databaseName}.${edge.tableName} WHERE ${whereClause}")
        //RDD[(businessKey->values)]
        val bizKeyAndValues: RDD[(String, String, Seq[AnyRef])] = edgeDf.map(row => {
          (row.getAs[String](edge.fromForeignKeyColumn), //consistent with vertexId generation logic, to make sure that vertex and its' outbound edges are in the same partition
            row.getAs[String](edge.toForeignKeyColumn), //consistent with vertexId generation logic
            allColumns.filterNot(col => (col.columnName.equalsIgnoreCase(edge.fromForeignKeyColumn) || col.columnName.equalsIgnoreCase(edge.toForeignKeyColumn))).map(col => {
              col.`type`.toUpperCase match {
                case "INTEGER" => Int.box(row.getAs[Int](col.columnName))
                case "STRING" => row.getAs[String](col.columnName).getBytes("UTF-8")
                case "FLOAT" => Float.box(row.getAs[Float](col.columnName))
                case "LONG" => Long.box(row.getAs[Long](col.columnName))
                case "DOUBLE" => Double.box(row.getAs[Double](col.columnName))
                case "BOOL" => Boolean.box(row.getAs[Boolean](col.columnName))
                //case "DATE" => row.getAs[Date](col.columnName) //TODO: not support Date type yet
                case a@_ => throw new IllegalStateException(s"unsupported edge data type ${a}")
              }
            }
            )
          )
        })

        bizKeyAndValues.map {
          case (srcIDString, dstIdString, values) => {
            val id = idGeneratorFunction.apply(srcIDString)
            val partitionId: Int = (id % mappingConfiguration.partitions).asInstanceOf[Int]

            val srcId = idGeneratorFunction.apply(srcIDString)
            val dstId = idGeneratorFunction.apply(dstIdString)
            // use NativeClient to generate key and encode values
            val keyEncoded = NativeClient.createEdgeKey(partitionId, srcId, edgeType.asInstanceOf[Int], -1L, dstId, DefaultVersion) //TODO: support edge ranking,like create_time desc
            val valuesEncoded: Array[Byte] = NativeClient.encoded(values.toArray)
            (new BytesWritable(keyEncoded), new PartitionIdAndValueBinaryWritable(partitionId, new BytesWritable(valuesEncoded), false))
          }
        }
      }

    }.fold(sc.emptyRDD[(BytesWritable, PartitionIdAndValueBinaryWritable)])(_ ++ _)


    edgesKVEncoded.saveAsNewAPIHadoopFile(sstFileOutput, classOf[BytesWritable], classOf[PartitionIdAndValueBinaryWritable], classOf[SstFileOutputFormat])
  }

  /**
    * check that columns claimed in mapping configuration file are defined in db(hive)
    * and its type is compatible, when not, throw exception, return all required column definitions,
    */
  private def validateColumns(sqlContext: HiveContext, edge: WithColumnMapping, colsMustCheck: Seq[String], colsMustFilter: Seq[String], databaseName: String): (Seq[Column], Seq[String]) = {
    val descriptionDF = sqlContext.sql(s"DESC ${databaseName}.${edge.tableName}")
    // all columns' name ---> type mapping in db
    val allColumnsMapInDB: Seq[(String, String)] = descriptionDF.map {
      case Row(colName: String, colType: String, _) => {
        (colName.toUpperCase, colType.toUpperCase)
      }
    }.collect.toSeq

    val nonPartitionColumnIndex = allColumnsMapInDB.indexWhere(_._1.startsWith("#"))
    val partitionColumnIndex = allColumnsMapInDB.lastIndexWhere(_._1.startsWith("#"))
    var partitionColumns: Seq[(String, String)] = Seq.empty[(String, String)]
    if (nonPartitionColumnIndex != -1 && ((partitionColumnIndex + 1) < allColumnsMapInDB.size)) {
      partitionColumns = allColumnsMapInDB.slice(partitionColumnIndex + 1, allColumnsMapInDB.size)
    }

    //all columns except partition columns
    val allColumnMap = allColumnsMapInDB.slice(0, nonPartitionColumnIndex).toMap

    // check the claimed columns really exist in db
    colsMustCheck.map(_.toUpperCase).foreach {
      col =>
        if (allColumnMap.get(col).isEmpty) {
          throw new IllegalStateException(s"${edge.name}'s from column: ${col} not defined in table=${edge.tableName}")
        }
    }

    if (edge.columnMappings.isEmpty) {
      //only (from,to) columns are checked, but all columns should be returned
      (allColumnMap.filter(!partitionColumns.contains(_)).filter(!colsMustFilter.contains(_)).map {
        case (colName, colType) => {
          Column(colName, colName, colType) // propertyName default=colName
        }
      }.toSeq, partitionColumns.map(_._1))
    }
    else {
      // tag/edge's columnMappings should be checked and returned
      val columnMappings = edge.columnMappings.get
      val notValid = columnMappings.filter(
        col => {
          val typeInDb = allColumnMap.get(col.columnName.toUpperCase)
          typeInDb.isEmpty || !DataTypeCompatibility.isCompatible(col.`type`, typeInDb.get)
        }
      ).map {
        case col => s"name=${col.columnName},type=${col.`type`}"
      }

      if (notValid.nonEmpty) {
        throw new IllegalStateException(s"${edge.name}'s columns: ${notValid.mkString("\t")} not defined in or compatible with db's definitions")
      }
      else {
        (columnMappings, partitionColumns.map(_._1))
      }
    }
  }
}
