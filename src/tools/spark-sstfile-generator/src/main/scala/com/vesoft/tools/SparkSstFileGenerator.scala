package com.vesoft.tools

import java.sql.Date

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
  * |
  * |-sstFileOutput
  * |
  * |--1
  * |  |
  * |  |——vertex.data
  * |  |--edge.data
  * |
  * |--2
  * |
  * |——vertex.data
  * |--edge.data
  * worker_node2
  * |
  * |-sstFileOutput
  * |
  * |--1
  * |  |
  * |  |——vertex.data
  * |  |--edge.data
  * |
  * |--2
  * |
  * |——vertex.data
  * |--edge.data
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
        log.error("illegal args", e)
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
    // disable hadoop output compression, cause rocksdb can't recognize it
    sc.getConf.set(FileOutputFormat.COMPRESS, "false")

    // id generator lambda, use FNV hash for now
    //TODO: support id generator function other than FNV hash
    //TODO: what need to be done when hash collision
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
        val allColumns: Seq[Column] = checkAndPopulateColumns(hiveContext, Left(tag))

        val columnExpression =
          if (allColumns.size == 0) { // only PK columns defined, so fetch all columns
            "*"
          } else {
            s"${tag.primaryKey}," + allColumns.mkString(",")
          }

        val tagDF = hiveContext.sql(s"SELECT ${columnExpression} FROM ${tag.tableName}")
        //RDD[(businessKey->values)]
        val bizKeyAndValues: RDD[(String, Seq[Any])] = tagDF.map(row => {
          (row.getAs[String](tag.primaryKey) + "_" + tag.tableName, //businessId_tableName used as key before HASH
            allColumns.filter(!_.columnName.equalsIgnoreCase(tag.primaryKey)).map(col => {
              col.`type` match {
                case "INTEGER" => row.getAs[Int](col.columnName)
                case "STRING" => row.getAs[String](col.columnName).getBytes("UTF-8") //native client can't decide string's charset
                case "FLOAT" => row.getAs[Float](col.columnName)
                case "LONG" => row.getAs[Long](col.columnName)
                case "DOUBLE" => row.getAs[Double](col.columnName)
                case "BOOL" => row.getAs[Boolean](col.columnName)
                case "DATE" => row.getAs[Date](col.columnName) //TODO: not support Date type yet
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
            // TODO: need static NativeClient.encode here
            // we need to wrap AnyVal in AnyRef to be compatible with NativeClient.encoded(Array[AnyRef])
            val anyArray: Array[AnyRef] = Array(values)
            val valuesEncoded: Array[Byte] = NativeClient.encoded(anyArray)
            (new BytesWritable(keyEncoded), new PartitionIdAndValueBinaryWritable(partitionId, new BytesWritable(valuesEncoded))) //TODO:valuesEncoded should be of type BytesWritable
          }
        }
      }
    }.fold(sc.emptyRDD[(BytesWritable, PartitionIdAndValueBinaryWritable)])(_ ++ _) // concat all vertex data

    // should generate a sub dir per partitionId in each worker node, to allow a partition shuffles to every worker
    tagsKVEncoded.saveAsNewAPIHadoopFile(sstFileOutput, classOf[BytesWritable], classOf[PartitionIdAndValueBinaryWritable], classOf[SstFileOutputFormat])

    //2)  handle edges
    val edgesKVEncoded: RDD[(BytesWritable, PartitionIdAndValueBinaryWritable)] = mappingConfiguration.edges.zipWithIndex.map {
      //edge index used as edge_type
      case (edge, edgeType) => {
        //all column w/o PK column
        val allColumns: Seq[Column] = checkAndPopulateColumns(hiveContext, Right(edge))

        val columnExpression =
          if (allColumns.size == 0) { // only FROM and TO column defined，so fetch all other column as properties
            "*"
          } else {
            s"${edge.fromForeignKeyColumn},${edge.toForeignKeyColumn}" + allColumns.mkString(",")
          }

        //TODO: join FROM_COLUMN and join TO_COLUMN from the table where this columns referencing, to make sure that the claimed id really exist in the reference table.BUT with HUGE Perf penalty
        val tagDF = hiveContext.sql(s"SELECT ${columnExpression} FROM ${edge.name}")
        //RDD[(businessKey->values)]
        val bizKeyAndValues: RDD[(String, String, Seq[Any])] = tagDF.map(row => {
          (row.getAs[String](edge.fromForeignKeyColumn), //consistent with vertexId generation logic, to make sure that vertex and its' outbound edges are in the same partition
            row.getAs[String](edge.toForeignKeyColumn), //consistent with vertexId generation logic
            allColumns.filter(col => !col.columnName.equalsIgnoreCase(edge.fromForeignKeyColumn) && !col.columnName.equalsIgnoreCase(edge.toForeignKeyColumn)).map(col => {
              col.`type` match {
                case "INTEGER" => row.getAs[Int](col.columnName)
                case "STRING" => row.getAs[String](col.columnName).getBytes("UTF-8") //native client can't decide string's charset
                case "FLOAT" => row.getAs[Float](col.columnName)
                case "LONG" => row.getAs[Long](col.columnName)
                case "DOUBLE" => row.getAs[Double](col.columnName)
                case "BOOL" => row.getAs[Boolean](col.columnName)
                case "DATE" => row.getAs[Date](col.columnName) //TODO: not support Date type yet
                case a@_ => throw new IllegalStateException(s"unsupported edge data type ${a}")
              }
            })
          )
        })

        bizKeyAndValues.map {
          case (srcIDString, dstIdString, values) => {
            val id = idGeneratorFunction.apply(srcIDString)
            val partitionId: Int = (id % mappingConfiguration.partitions).asInstanceOf[Int]

            val srcId = idGeneratorFunction.apply(srcIDString)
            val dstId = idGeneratorFunction.apply(dstIdString)
            val keyEncoded = NativeClient.createEdgeKey(partitionId, srcId, edgeType.asInstanceOf[Int], -1L, dstId, DefaultVersion) //TODO: support edge ranking,like create_time desc
            // TODO: need static NativeClient.encode here
            // val valuesEncoded:Array[Byte] = NativeClient.encode(values)
            val valuesEncoded: Array[Byte] = Array.empty[Byte]
            (new BytesWritable(keyEncoded), new PartitionIdAndValueBinaryWritable(partitionId, new BytesWritable(valuesEncoded), false)) //TODO:valuesEncoded should be of type BytesWritable
          }
        }
      }
    }.fold(sc.emptyRDD[(BytesWritable, PartitionIdAndValueBinaryWritable)])(_ ++ _)


    edgesKVEncoded.saveAsNewAPIHadoopFile(sstFileOutput, classOf[BytesWritable], classOf[PartitionIdAndValueBinaryWritable], classOf[SstFileOutputFormat])
  }

  /**
    * check that columns required in mapping configuration file should be defined in db(hive)
    * and its type is compatible, when not, throw exception, return all required column definitions,
    *
    * @return all explicitly defined column or all columns(default), except PK column
    */
  private def checkAndPopulateColumns(hiveContext: HiveContext, tagEdgeEither: Either[Tag, Edge]): Seq[Column] = {
    tagEdgeEither match {
      case Left(tag) => {
        handleTag(hiveContext, tag)
      }
      case Right(edge) => {
        handleEdge(hiveContext, edge)
      }
    }
  }

  private def handleEdge(hiveContext: HiveContext, edge: Edge): Seq[Column] = {
    val descriptionDF = hiveContext.sql(s"DESC ${edge.tableName}")
    // all columns' name ---> type mapping in db
    val allColumnsMapInDB: Map[String, String] = descriptionDF.map {
      case Row(colName: String, colType: String, _) => {
        (colName.toUpperCase, colType.toUpperCase)
      }
    }.collect.toMap


    // check FK column and the reference column
    if (allColumnsMapInDB.get(edge.fromForeignKeyColumn.toUpperCase).isEmpty) {
      throw new IllegalStateException(s"Edge=${edge.name}'s from column: ${edge.fromForeignKeyColumn} not defined in table=${edge.tableName}")
    }

    // check FROM_COLUMN foreignkey must reference an existing column
    val fromColumnExist = checkColumnExistInTable(hiveContext, edge.fromReferenceTable, edge.fromReferenceColumn)
    if (!fromColumnExist) {
      throw new IllegalStateException(s"Edge=${edge.name}'s from reference column: ${edge.fromReferenceColumn} not defined in table=${edge.fromReferenceTable}")
    }

    if (allColumnsMapInDB.get(edge.toForeignKeyColumn.toUpperCase).isEmpty) {
      throw new IllegalStateException(s"Edge=${edge.name}'s to column: ${edge.fromForeignKeyColumn} not defined in table=${edge.tableName}")
    }

    // check TO_COLUMN foreignkey must reference an existing column
    val toColumnExist = checkColumnExistInTable(hiveContext, edge.toReferenceTable, edge.toReferenceColumn)
    if (!toColumnExist) {
      throw new IllegalStateException(s"Edge=${edge.name}'s to reference column: ${edge.toReferenceColumn} not defined in table=${edge.toReferenceTable}")
    }

    if (edge.columnMappings.isEmpty) { //only (from,to) columns are checked, but all columns should be returned
      allColumnsMapInDB.filter(col => !col._1.equalsIgnoreCase(edge.fromForeignKeyColumn) && !col._1.equalsIgnoreCase(edge.toForeignKeyColumn)).map {
        case (colName, colType) => {
          Column(colName, colName, colType) // propertyName default=colName
        }
      }.toSeq
    }
    else { // tag.columnMappings should be checked and returned
      val columnMappings = edge.columnMappings.get
      //just check
      val notValid = columnMappings.filter(
        col => {
          val typeInDb = allColumnsMapInDB.get(col.columnName.toUpperCase)
          !typeInDb.isEmpty || !DataTypeCompatibility.isCompatible(col.`type`, typeInDb.get)
        }
      ).map {
        case col => s"name=${col.columnName},type=${col.`type`}"
      }

      if (notValid.nonEmpty) {
        throw new IllegalStateException(s"Edge=${edge.name}'s columns: ${notValid.mkString("\t")} not defined in or compatible with db's definitions")
      }
      else {
        columnMappings
      }
    }
  }


  private def handleTag(hiveContext: HiveContext, tag: Tag): Seq[Column] = {
    val descriptionDF = hiveContext.sql(s"DESC ${tag.tableName}")
    // all columns' name ---> type in db
    val allColumnsMapInDB: Map[String, String] = descriptionDF.map {
      case Row(colName: String, colType: String, _) => {
        (colName.toUpperCase, colType.toUpperCase)
      }
    }.collect.toMap


    //always check PK column
    if (allColumnsMapInDB.get(tag.primaryKey.toUpperCase).isEmpty) {
      throw new IllegalStateException(s"Tag=${tag.name}'s PK column: ${tag.primaryKey} not defined in db")
    }

    if (tag.columnMappings.isEmpty) { //only PK column checked, but all columns should be returned
      allColumnsMapInDB.filter(!_._1.equalsIgnoreCase(tag.primaryKey)).map {
        case (colName, colType) => {
          Column(colName, colName, colType) // propertyName default=colName
        }
      }.toSeq
    }
    else { // tag.columnMappings should be checked and returned
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
        throw new IllegalStateException(s"Tag=${tag.name}'s columns: ${notValid.mkString("\t")} not defined in or compatible with db's definitions")
      }
      else {
        columnMappings
      }
    }
  }

  /**
    * check whether a column exist in a table
    */
  private def checkColumnExistInTable(hiveContext: HiveContext, tableName: String, columnName: String): Boolean = {
    val fromDF = hiveContext.sql(s"DESC ${tableName}")
    val fromTableCols: Map[String, String] = fromDF.map {
      case Row(colName: String, colType: String, _) => {
        (colName.toUpperCase, colType.toUpperCase)
      }
    }.collect.toMap

    fromTableCols.get(columnName.toUpperCase).isEmpty
  }
}
