/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.tools

import com.vesoft.client.NativeClient
import org.scalatest.{BeforeAndAfter, FlatSpec}

class SparkSstFileGeneratorTest extends FlatSpec with BeforeAndAfter {
  // need to put the dir which contains nebula_native_client.so in java.library.path before run this test
  "an Seq[Any]" should "be encoded by NativeClient" in {
    val values = Seq(
      "Hello World".getBytes("UTF-8"),
      Boolean.box(false),
      Long.box(1024L),
      Int.box(7),
      Float.box(3.1415f),
      Double.box(9.12)
    )
    val bytes: Array[Byte] = NativeClient.encode(values.toArray)
    assert(
      bytes == Array(0, 11, 96, -28, -108, -21, 0, 0, 0, 0, -72, 65, 20, 7, 86, 14, 73, 64, 61, 10,
        -41, -93, 112, 61, 34, 64)
    )
  }
}
