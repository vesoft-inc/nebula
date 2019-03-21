package com.vesoft.tools

import com.vesoft.client.NativeClient
import org.scalatest.{BeforeAndAfter, FlatSpec}

class SparkSstFileGeneratorTest extends FlatSpec with BeforeAndAfter {

  "an Seq[Any]" should "be encoded by NativeClient" in {
    val values = Seq("Hello World".getBytes("UTF-8"), false, 1024L, 7, 3.1415f, 9.12)
    val anyArray: Array[AnyRef] = Array(values)
    val bytes: Array[Byte] = NativeClient.encoded(anyArray)
    println(bytes)
  }


}
