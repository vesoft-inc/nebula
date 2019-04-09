package com.vesoft.tools

import java.io.File

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.rocksdb.{EnvOptions, Options, SstFileWriter}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * custom outputFormat, which generate a sub dir per partition per worker
  */
class SstFileOutputFormat extends FileOutputFormat[BytesWritable, PartitionIdAndValueBinaryWritable] {
  private[this] val log = LoggerFactory.getLogger(this.getClass)

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[BytesWritable, PartitionIdAndValueBinaryWritable] = {
    // make sure to disable "mapreduce.output.fileoutputformat.compress"
    if (FileOutputFormat.getCompressOutput(job)) {
      job.getConfiguration.setBoolean(FileOutputFormat.COMPRESS, false)
    }

    val sstFileOutput = job.getConfiguration.get(SparkSstFileGenerator.SSF_OUTPUT_DIR_CONF_KEY)
    log.debug(s"SstFileOutputFormat read sstFileOutput=${sstFileOutput}")
    new SstRecordWriter(sstFileOutput)
  }
}

/**
  * custom outputFormat, which generate a sub dir per partition per worker
  *
  * @param sstFileOutput spark conf item,which points to the location where rocksdb sst files will be put
  */
class SstRecordWriter(sstFileOutput: String) extends RecordWriter[BytesWritable, PartitionIdAndValueBinaryWritable] {
  private[this] val log = LoggerFactory.getLogger(this.getClass)

  /**
    * all sst files opened, (vertexOrEdge ->(partitionId,SstFileWriter))
    */
  private var sstFiles = mutable.Map.empty[Boolean, (Int, SstFileWriter)]

  override def write(key: BytesWritable, value: PartitionIdAndValueBinaryWritable): Unit = {
    var sstFileWriter: SstFileWriter = null
    val sstWriterOption = sstFiles.get(value.vertexOrEdge)
    if (sstWriterOption.isEmpty) {
      // initialize a rocksdb
      val env = new EnvOptions
      val options = new Options
      options.setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
      sstFileWriter = new SstFileWriter(env, options)

      /*
       * create the following dir structure:
       *
       * ${sstFileOutput}
       *       |
       *       |--1
       *       |  |
       *       |  |——vertex.data
       *       |  |--edge.data
       *       |
       *       |--2
       *          |
       *          |——vertex.data
       *          |--edge.data
       *
       */
      val subDir = if (value.vertexOrEdge) "vertex" else "edge"
      //TODO: rolling to another file when file size > some THRESHOLD, or some other criteria
      sstFileWriter.open(s"${sstFileOutput}${File.separator}${value.partitionId}${File.separator}${subDir}.data")
      sstFiles += value.vertexOrEdge -> (value.partitionId, sstFileWriter)
    } else {
      val tmpSstWriterOption = sstWriterOption.filter(_._1 == value.partitionId).map(_._2)
      assert(tmpSstWriterOption.isDefined)
      sstFileWriter = tmpSstWriterOption.get
    }

    //TODO: use batch write to improve performance
    sstFileWriter.put(key.getBytes(), value.values.getBytes)

  }

  override def close(context: TaskAttemptContext): Unit = {
    sstFiles.values.foreach(sst => {
      try {
        sst._2.close()
      }
      catch {
        case e: Exception => {
          log.error("error when close a sst file", e)
        }
      }
    })
  }
}
