// SPDX-License-Identifier: Apache-2.0
//===-- wasmedge/common/enum_types.h - WASM types related enumerations ----===//
//
// Part of the WasmEdge Project.
//
//===----------------------------------------------------------------------===//
///
/// \file
/// This file contains the definitions of WASM types related enumerations.
///
//===----------------------------------------------------------------------===//

#ifndef WASMEDGE_C_API_ENUM_TYPES_H
#define WASMEDGE_C_API_ENUM_TYPES_H

#if (defined(__cplusplus) && __cplusplus > 201402L) ||                         \
    (defined(_MSVC_LANG) && _MSVC_LANG > 201402L)
#include <cstdint>
#include <string>
#include <unordered_map>
#endif

/// WASM Value type enumeration.
#if (defined(__cplusplus) && __cplusplus > 201402L) ||                         \
    (defined(_MSVC_LANG) && _MSVC_LANG > 201402L)
namespace WasmEdge {
enum class ValType : uint8_t {
  None = 0x40,
  I32 = 0x7F,
  I64 = 0x7E,
  F32 = 0x7D,
  F64 = 0x7C,
  V128 = 0x7B,
  FuncRef = 0x70,
  ExternRef = 0x6F
};

static inline std::unordered_map<ValType, std::string> ValTypeStr = {
    {ValType::None, "none"},       {ValType::I32, "i32"},
    {ValType::I64, "i64"},         {ValType::F32, "f32"},
    {ValType::F64, "f64"},         {ValType::V128, "v128"},
    {ValType::FuncRef, "funcref"}, {ValType::ExternRef, "externref"}};
} // namespace WasmEdge
#endif

enum WasmEdge_ValType {
  WasmEdge_ValType_I32 = 0x7FU,
  WasmEdge_ValType_I64 = 0x7EU,
  WasmEdge_ValType_F32 = 0x7DU,
  WasmEdge_ValType_F64 = 0x7CU,
  WasmEdge_ValType_V128 = 0x7BU,
  WasmEdge_ValType_FuncRef = 0x70U,
  WasmEdge_ValType_ExternRef = 0x6FU
};

/// WASM Number type enumeration.
#if (defined(__cplusplus) && __cplusplus > 201402L) ||                         \
    (defined(_MSVC_LANG) && _MSVC_LANG > 201402L)
namespace WasmEdge {
enum class NumType : uint8_t {
  I32 = 0x7F,
  I64 = 0x7E,
  F32 = 0x7D,
  F64 = 0x7C,
  V128 = 0x7B
};
} // namespace WasmEdge
#endif

enum WasmEdge_NumType {
  WasmEdge_NumType_I32 = 0x7FU,
  WasmEdge_NumType_I64 = 0x7EU,
  WasmEdge_NumType_F32 = 0x7DU,
  WasmEdge_NumType_F64 = 0x7CU,
  WasmEdge_NumType_V128 = 0x7BU
};

/// WASM Reference type enumeration.
#if (defined(__cplusplus) && __cplusplus > 201402L) ||                         \
    (defined(_MSVC_LANG) && _MSVC_LANG > 201402L)
namespace WasmEdge {
enum class RefType : uint8_t { ExternRef = 0x6F, FuncRef = 0x70 };
} // namespace WasmEdge
#endif

enum WasmEdge_RefType {
  WasmEdge_RefType_FuncRef = 0x70U,
  WasmEdge_RefType_ExternRef = 0x6FU
};

/// WASM Mutability enumeration.
#if (defined(__cplusplus) && __cplusplus > 201402L) ||                         \
    (defined(_MSVC_LANG) && _MSVC_LANG > 201402L)
namespace WasmEdge {
enum class ValMut : uint8_t { Const = 0x00, Var = 0x01 };

static inline std::unordered_map<ValMut, std::string> ValMutStr = {
    {ValMut::Const, "const"}, {ValMut::Var, "var"}};
} // namespace WasmEdge
#endif

enum WasmEdge_Mutability {
  WasmEdge_Mutability_Const = 0x00U,
  WasmEdge_Mutability_Var = 0x01U
};

/// WASM External type enumeration.
#if (defined(__cplusplus) && __cplusplus > 201402L) ||                         \
    (defined(_MSVC_LANG) && _MSVC_LANG > 201402L)
namespace WasmEdge {
enum class ExternalType : uint8_t {
  Function = 0x00U,
  Table = 0x01U,
  Memory = 0x02U,
  Global = 0x03U
};

static inline std::unordered_map<ExternalType, std::string> ExternalTypeStr = {
    {ExternalType::Function, "function"},
    {ExternalType::Table, "table"},
    {ExternalType::Memory, "memory"},
    {ExternalType::Global, "global"}};
} // namespace WasmEdge
#endif

enum WasmEdge_ExternalType {
  WasmEdge_ExternalType_Function = 0x00U,
  WasmEdge_ExternalType_Table = 0x01U,
  WasmEdge_ExternalType_Memory = 0x02U,
  WasmEdge_ExternalType_Global = 0x03U
};

#endif /// WASMEDGE_C_API_ENUM_TYPES_H
