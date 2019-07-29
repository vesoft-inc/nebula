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

  private val INIT64 = BigInt("cbf29ce484222325", 16)
  private val PRIME64 = BigInt("100000001b3", 16)
  private val MOD64 = BigInt("2").pow(64)
  private val MASK = 0xff

  @inline
  private final def calc(prime: BigInt, mod: BigInt)(hash: BigInt, b: Byte): BigInt = ((hash * prime) % mod) ^ (b & MASK)
  @inline private final def calcA(prime: BigInt, mod: BigInt)(hash: BigInt, b: Byte): BigInt = ((hash ^ (b & MASK)) * prime) % mod

  /**
    * Calculates 64bit FNV-1 hash
    * @param data the data to be hashed
    * @return a 64bit hash value
    */
  @inline final def hash64(data: Array[Byte]): BigInt = data.foldLeft(INIT64)(calc(PRIME64, MOD64))

  /**
    * Calculates 64bit FNV-1a hash
    * @param data the data to be hashed
    * @return a 64bit hash value
    */
  @inline final def hash64a(data: Array[Byte]): BigInt = data.foldLeft(INIT64)(calcA(PRIME64, MOD64))


}
