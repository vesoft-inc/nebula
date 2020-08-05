/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.tools

/**
  * FNV hash util
  *
  * @link http://www.isthe.com/chongo/tech/comp/fnv/index.html
  */
object FNVHash {

  private val FNV_64_INIT  = 0xCBF29CE484222325L
  private val FNV_64_PRIME = 0x100000001B3L
  private val FNV_32_INIT  = 0x811c9dc5

  private val FNV_32_PRIME = 0x01000193

  /**
    * hash to int32
    */
  def hash32(value: String): Int = {
    var rv  = FNV_32_INIT
    val len = value.length
    var i   = 0
    while (i < len) {
      rv ^= value.charAt(i)
      rv *= FNV_32_PRIME
      i += 1
    }

    rv
  }

  /**
    * hash to int64
    */
  def hash64(value: String): Long = {
    var rv  = FNV_64_INIT
    val len = value.length
    var i   = 0
    while (i < len) {
      rv ^= value.charAt(i)
      rv *= FNV_64_PRIME

      i += 1
    }

    rv
  }
}
