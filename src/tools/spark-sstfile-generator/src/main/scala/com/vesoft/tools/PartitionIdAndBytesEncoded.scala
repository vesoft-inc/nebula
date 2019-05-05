package com.vesoft.tools

import org.apache.hadoop.io.BytesWritable

/**
  * composite key for vertex/edge RDD
  *
  * @param partitionId  partition number
  * @param valueEncoded vertex/edge key encoded by native client
  */
case class PartitionIdAndBytesEncoded(partitionId: Long, valueEncoded: BytesWritable)