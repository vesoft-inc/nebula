/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.tools.generator.v2.entry

object KeyPolicy extends Enumeration {
  type POLICY = Value
  val HASH = Value("hash")
  val UUID = Value("uuid")
  val NONE = Value("")
}
