// SPDX-License-Identifier: Apache-2.0
//===-- wasmedge/int128.h - WasmEdge C API --------------------------------===//
//
// Part of the WasmEdge Project.
//
//===----------------------------------------------------------------------===//
///
/// \file
/// This file contains the int128 definitions of the WasmEdge C API.
///
//===----------------------------------------------------------------------===//

#ifndef WASMEDGE_C_API_INT128_H
#define WASMEDGE_C_API_INT128_H

#if defined(__x86_64__) || defined(__aarch64__)
typedef unsigned __int128 uint128_t;
typedef __int128 int128_t;
#else
typedef struct uint128_t {
  uint64_t Low;
  uint64_t High;
} uint128_t;

typedef struct int128_t {
  uint64_t Low;
  int64_t High;
} int128_t;
#endif

#endif /// WASMEDGE_C_API_INT128_H
