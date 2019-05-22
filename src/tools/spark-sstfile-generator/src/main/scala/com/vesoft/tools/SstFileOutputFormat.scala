package com.vesoft.tools

import java.io.File

import com.vesoft.tools.VertexOrEdgeEnum.VertexOrEdgeEnum
import javax.xml.bind.DatatypeConverter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.security.TokenCache
import org.rocksdb.{EnvOptions, Options, SstFileWriter}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Custom outputFormat, which generate a sub dir per partition per worker:
  *
  * worker_node1
  * |
  * |-sstFileOutput
  *     |
  *     |--1
  *     |  |
  *     |  |——vertex-${FIRST_KEY}.data
  *     |  |--edge-${FIRST_KEY}.data
  *     |
  *     |--2
  *        |
  *        |——vertex-${FIRST_KEY}.data
  *        |--edge-${FIRST_KEY}.data
  * worker_node2
  * |
  * |-sstFileOutput
  *     |
  *     |--1
  *     |  |
  *     |  |——vertex-${FIRST_KEY}.data
  *     |  |--edge-${FIRST_KEY}.data
  *     |
  *     |--2
  *        |
  *        |——vertex-${FIRST_KEY}.data
  *        |--edge-${FIRST_KEY}.data
  */
class SstFileOutputFormat extends FileOutputFormat[BytesWritable, PartitionIdAndValueBinaryWritable] {
  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[BytesWritable, PartitionIdAndValueBinaryWritable] = {
    if (FileOutputFormat.getCompressOutput(job)) {
      job.getConfiguration.setBoolean(FileOutputFormat.COMPRESS, false)
    }

    val sstFileOutput = job.getConfiguration.get(SparkSstFileGenerator.SSF_OUTPUT_DIR_CONF_KEY)
    new SstRecordWriter(sstFileOutput, job.getConfiguration)
  }

  // not check output dir exist, for edge&vertex data are coexist in the same partition dir
  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) throw new InvalidJobConfException("Output directory not set.")
    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }
}

/**
  * custom outputFormat, which generate a sub dir per partition per worker
  *
  * @param sstFileOutput spark conf item,which points to the location where rocksdb sst files will be put
  * @param configuration hadoop configuration
  */
class SstRecordWriter(sstFileOutput: String, configuration: Configuration) extends RecordWriter[BytesWritable, PartitionIdAndValueBinaryWritable] {
  private[this] val log = LoggerFactory.getLogger(this.getClass)

  /**
    * all sst files opened, (vertexOrEdge,partitionId)->SstFileWriter
    */
  private var sstFiles = mutable.Map.empty[(VertexOrEdgeEnum, Int), SstFileWriter]

  /**
    * need local file system only, for rocksdb sstFileWriter can't write to remote file system(only can back up to HDFS) for now
    */
  private val fileSystem = FileSystem.get(new Configuration(false))

  // all RocksObject should be closed
  private var env: EnvOptions = _
  private var options: Options = _

  override def write(key: BytesWritable, value: PartitionIdAndValueBinaryWritable): Unit = {
    var sstFileWriter: SstFileWriter = null

    //cache per partition per vertex/edge type sstfile writer
    val sstWriterOptional = sstFiles.get((value.vertexOrEdgeEnum, value.partitionId))
    if (sstWriterOptional.isEmpty) {
      /**
        * https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files
        *
        * Please note that:
        *
        *1. Options passed to SstFileWriter will be used to figure out the table type, compression options, etc that will be used to create the SST file.
        *2. The Comparator that is passed to the SstFileWriter must be exactly the same as the Comparator used in the DB that this file will be ingested into.
        *3. Rows must be inserted in a strictly increasing order.
        */
      //TODO: how to make sure the options used here to be consistent with the server's
      // What's the fastest way to load data into RocksDB: https://rocksdb.org.cn/doc/RocksDB-FAQ.html
      env = new EnvOptions
      // TODO: prove setWritableFileMaxBufferSize is working?
      options = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true).setWritableFileMaxBufferSize(1024 * 100).setMaxBackgroundFlushes(5).prepareForBulkLoad()
      sstFileWriter = new SstFileWriter(env, options)

      //TODO: rolling to another file when file size > some THRESHOLD, or some other criteria
      // Each partition can generated multiple sst files, among which keys will be ordered, and keys could overlap between different sst files.
      // All these sst files will be  `hdfs -copyFromLocal` to the same HDFS dir(and consumed by subsequent nebula `load` command), so we need different suffixes to distinguish between them.
      var sstFile = s"${sstFileOutput}${File.separator}${value.partitionId}${File.separator}${value.vertexOrEdgeEnum}-${DatatypeConverter.printHexBinary(key.getBytes)}.data".toLowerCase
      val path = new Path(sstFile)
      if (!fileSystem.exists(path)) {
        fileSystem.create(path)
      }
      else {
        if (fileSystem.isDirectory(path)) {
          fileSystem.delete(path, true)
          fileSystem.create(path)
        }
      }

      // sstfile is pre-checked by SparkSstFileGenerator, so no bother to recheck
      if (sstFile.startsWith("file://")) {
        sstFile = sstFile.substring(7)
      }

      sstFileWriter.open(sstFile)
      sstFiles += (value.vertexOrEdgeEnum, value.partitionId) -> sstFileWriter
    } else {
      assert(sstWriterOptional.isDefined)
      sstFileWriter = sstWriterOptional.get
    }

    //TODO: could be batched?
    sstFileWriter.put(key.getBytes(), value.values.getBytes)
  }

  override def close(context: TaskAttemptContext): Unit = {
    sstFiles.values.foreach { sstFile =>
      try {
        sstFile.finish()
        sstFile.close()
      }
      catch {
        case e: Exception => {
          log.error("Error when close a sst file", e)
        }
      }
    }

    if (env != null) {
      env.close()
    }

    if (options != null) {
      options.close()
    }
  }
}
