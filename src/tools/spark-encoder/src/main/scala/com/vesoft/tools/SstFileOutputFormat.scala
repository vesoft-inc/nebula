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

  //TODO: disable hadoop compression
  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[BytesWritable, PartitionIdAndValueBinaryWritable] = {
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

  // all sst files opened
  var sstFiles = mutable.Map.empty[Int, SstFileWriter]

  override def write(key: BytesWritable, value: PartitionIdAndValueBinaryWritable): Unit = {
    var sstFileWriter: SstFileWriter = null
    val sstWriterOption = sstFiles.get(value.partitionId)
    if (sstWriterOption.isEmpty) {
      // initialize a rocksdb
      val env = new EnvOptions
      val options = new Options
      options.setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
      sstFileWriter = new SstFileWriter(env, options)
      //TODO: excepted to generate a sub dir per partition
      //TODO: rolling to another file when file size > some THRESHOLD, or some other criteria
      sstFileWriter.open(s"${sstFileOutput}${File.separator}${value.partitionId}.data")
      sstFiles += value.partitionId -> sstFileWriter
    } else {
      sstFileWriter = sstWriterOption.get
    }

    //TODO: use batch write to improve perf
    sstFileWriter.put(key.getBytes(), value.values.getBytes) //TODO: native client should return bytes

  }

  override def close(context: TaskAttemptContext): Unit = {
    sstFiles.values.foreach(sst => {
      try {
        sst.close()
      }
      catch {
        case e: Exception => {
          log.error("error when close a sst file", e)
        }
      }
    })
  }
}
