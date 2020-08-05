/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.tools

import java.io.{File, IOException}

import com.vesoft.tools.SparkSstFileGenerator.GraphPartitionIdAndKeyValueEncoded
import com.vesoft.tools.VertexOrEdgeEnum.VertexOrEdgeEnum
import javax.xml.bind.DatatypeConverter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.security.TokenCache
import org.rocksdb.{EnvOptions, Options, SstFileWriter}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.sys.process._

/**
  * Custom outputFormat, which generates a sub dir per partition per worker, the local dir structure of EACH worker node is:
  *
  * ${LOCAL_ROOT} (local dir will be stripped off when `hdfs -copyFromLocal`, specified by user through cmd line)
  *    |---1 (this is partition number)
  *    |        | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst
  *    |        | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst
  *    |        | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst
  *    |        | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst
  *    |---2
  *             | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst
  *             | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst
  *             | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst
  *      ....
  *
  * Sst file name convention is {TYPE}-${FIRST_KEY_IN_THIS_FILE}.sst, where type=vertex OR edge, FIRST_KEY_IN_THIS_FILE is the first key the file sees.
  * This combination will make the sst file name unique among all worker nodes.
  *
  * After hdfs -copyFromLocal ,the final hdfs dir layout is:
  *
  * ${HDFS_ROOT} (specified by user through cmd line)
  *    |---1 (this is the partition number, and it will hold all sst files from every single worker node with the same partition number)
  *    |        | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst(may be from worker node#1)
  *    |        | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst(may be from worker node#2)
  *    |        | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst(may be from worker node#1)
  *    |        | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst(may be from worker node#2)
  *    |---2 (same as above)
  *             | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst
  *
  **/
class SstFileOutputFormat
    extends FileOutputFormat[
      GraphPartitionIdAndKeyValueEncoded,
      PropertyValueAndTypeWritable
    ] {
  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[
    GraphPartitionIdAndKeyValueEncoded,
    PropertyValueAndTypeWritable
  ] = {
    if (FileOutputFormat.getCompressOutput(job)) {
      job.getConfiguration.setBoolean(FileOutputFormat.COMPRESS, false)
    }

    val sstFileOutput = job.getConfiguration.get(
      SparkSstFileGenerator.SSF_OUTPUT_LOCAL_DIR_CONF_KEY
    )
    new SstRecordWriter(sstFileOutput, job.getConfiguration)
  }

  // do not check output dir existence, for edge&vertex data coexist in the same partition dir
  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null)
      throw new InvalidJobConfException("Output directory not set.")
    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(
      job.getCredentials,
      Array[Path](outDir),
      job.getConfiguration
    )
  }
}

/**
  * custom outputFormat, which generates a sub dir per partition per worker
  *
  * @param localSstFileOutput spark conf item,which points to the local dir where rocksdb sst files will be put
  * @param configuration      hadoop configuration
  */
class SstRecordWriter(localSstFileOutput: String, configuration: Configuration)
    extends RecordWriter[
      GraphPartitionIdAndKeyValueEncoded,
      PropertyValueAndTypeWritable
    ] {
  private[this] val log = LoggerFactory.getLogger(this.getClass)

  /**
    * all sst files opened, (vertexOrEdge,partitionId)->(SstFileWriter,localSstFilePath,hdfsParentPath)
    */
  private var sstFilesMap =
    mutable.Map.empty[(VertexOrEdgeEnum, Int), (SstFileWriter, String, String)]

  /**
    * need local file system only, for rocksdb sstFileWriter can't write to remote file system(only can back up to HDFS) for now
    */
  private val localFileSystem = FileSystem.get(new Configuration(false))

  /**
    * hdfs file system creates destination sst file dir
    */
  private val hdfsFileSystem = FileSystem.get(configuration)

  if (!hdfsFileSystem.getScheme.equalsIgnoreCase("hdfs")) {
    throw new IllegalStateException("File system is not hdfs")
  }

  /**
    * dir hdfs puts sst files
    */
  private val hdfsParentDir =
    configuration.get(SparkSstFileGenerator.SSF_OUTPUT_HDFS_DIR_CONF_KEY)

  log.debug(s"SstRecordWriter read hdfs dir:${hdfsParentDir}")

  // all RocksObject should be closed
  private var env: EnvOptions  = _
  private var options: Options = _

  override def write(
      key: GraphPartitionIdAndKeyValueEncoded,
      value: PropertyValueAndTypeWritable
  ): Unit = {
    var sstFileWriter: SstFileWriter = null

    //cache per partition per vertex/edge type's sstfile writer
    val sstWriterOptional =
      sstFilesMap.get((value.vertexOrEdgeEnum, key.partitionId))
    if (sstWriterOptional.isEmpty) {

      /**
        * https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files
        *
        * Please note:
        *
        *1. Options passed to SstFileWriter will be used to figure out the table type, compression options, etc. that will be used to create the SST file.
        *2. The Comparator passed to the SstFileWriter must be exactly the same as the Comparator used in the DB that this file will be ingested into.
        *3. Rows must be inserted in a strict increasing order.
        */
      //TODO: how to make sure the options used here is consistent with the server's
      // What's the fastest way to load data into RocksDB: https://rocksdb.org.cn/doc/RocksDB-FAQ.html
      env = new EnvOptions
      // TODO: prove setWritableFileMaxBufferSize is working?
      options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setWritableFileMaxBufferSize(1024 * 100)
        .setMaxBackgroundFlushes(5)
        .prepareForBulkLoad()
      sstFileWriter = new SstFileWriter(env, options)

      // TODO: rolling to another file when file size > some THRESHOLD, or some other criteria

      // Each partition can generated multiple sst files, among which keys will be ordered, and keys could overlap between different sst files.
      // All these sst files will be  `hdfs -copyFromLocal` to the same HDFS dir(and consumed by subsequent nebula `IMPORT` command), so we need different suffixes to distinguish between them.
      val hdfsSubDirectory =
        s"${File.separator}${key.partitionId}${File.separator}"

      val localDir = s"${localSstFileOutput}${hdfsSubDirectory}"
      val sstFileName =
        s"${value.vertexOrEdgeEnum}-${key.`type`}-${DatatypeConverter
          .printHexBinary(key.valueEncoded.getBytes)}.sst".toLowerCase

      val localDirPath = new Path(localDir)
      if (!localFileSystem.exists(localDirPath)) {
        localFileSystem.mkdirs(localDirPath)
      } else {
        if (localFileSystem.isFile(localDirPath)) {
          localFileSystem.delete(localDirPath, true)
        }
      }

      var localSstFile = s"${localDir}${sstFileName}"
      localFileSystem.create(new Path(localSstFile))

      if (localSstFile.startsWith("file:///")) {
        localSstFile = localSstFile.substring(7)
      }

      sstFileWriter.open(localSstFile)
      sstFilesMap += (value.vertexOrEdgeEnum, key.partitionId) -> (sstFileWriter, localSstFile, hdfsSubDirectory)
    } else {
      sstFileWriter = sstWriterOptional.get._1
    }

    //TODO: could be batched?
    sstFileWriter.put(key.valueEncoded.getBytes(), value.values.getBytes)
  }

  override def close(context: TaskAttemptContext): Unit = {
    if (env != null) {
      env.close()
    }

    if (options != null) {
      options.close()
    }

    sstFilesMap.values.foreach {
      case (sstFile, localSstFile, hdfsDirectory) =>
        try {
          sstFile.finish()
          sstFile.close()

          runHdfsCopyFromLocal(localSstFile, hdfsDirectory)
        } catch {
          case e: Exception => {
            log.error("Error when closing a sst file", e)
          }
        }

        try {
          // There could be multiple containers on a single host, parent dir are shared between multiple containers,
          // so should not delete parent dir but delete individual file
          localFileSystem.delete(new Path(localSstFile), true)
        } catch {
          case e: Exception => {
            log.error(s"Error when deleting local dir:${localSstFileOutput}", e)
          }

        }
    }
  }

  /**
    * assembly a hdfs dfs -copyFromLocal command line to put local sst file to hdfs, and $HADOOP_HOME env needs to be set
    */
  private def runHdfsCopyFromLocal(
      localSstFile: String,
      hdfsDirectory: String
  ) = {
    var hadoopHome = System.getenv("HADOOP_HOME")
    while (hadoopHome.endsWith("/")) {
      hadoopHome = hadoopHome.stripSuffix("/")
    }

    val destinationHdfsDir = s"${hdfsParentDir}${hdfsDirectory}"
    val destinationPath    = new Path(destinationHdfsDir)

    try {
      if (!hdfsFileSystem.exists(destinationPath)) {
        hdfsFileSystem.mkdirs(destinationPath)
      }
    } catch {
      case e: IOException => {
        log.error(s"Error when making hdfs dir ${destinationPath}", e)
        throw e
      }
    }

    val command = List(
      s"${hadoopHome}/bin/hdfs",
      "dfs",
      "-copyFromLocal",
      s"${localSstFile}",
      destinationHdfsDir
    )
    val exitCode = command.!
    log.debug(s"Running command:${command.mkString(" ")}, exitCode=${exitCode}")

    if (exitCode != 0) {
      throw new IllegalStateException(
        s"Can't put local file `${localSstFile}` to hdfs dir: `${destinationHdfsDir}`," +
          "sst files will reside on each worker's local file system only! Need to run `hdfs dfs -copyFromLocal` manually"
      )
    }
  }
}
