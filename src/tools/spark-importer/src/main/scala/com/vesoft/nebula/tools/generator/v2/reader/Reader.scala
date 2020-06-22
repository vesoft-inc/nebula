/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  */
trait Reader {
  def session: SparkSession

  def read(): DataFrame

  def close(): Unit
}
