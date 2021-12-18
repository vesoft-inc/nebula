// SPDX-License-Identifier: Apache-2.0
//===-- wasmedge/common/enum_configure.h - Configure related enumerations -===//
//
// Part of the WasmEdge Project.
//
//===----------------------------------------------------------------------===//
///
/// \file
/// This file contains the definitions of configure related enumerations.
///
//===----------------------------------------------------------------------===//

#ifndef WASMEDGE_C_API_ENUM_CONFIGURE_H
#define WASMEDGE_C_API_ENUM_CONFIGURE_H

#if (defined(__cplusplus) && __cplusplus > 201402L) ||                         \
    (defined(_MSVC_LANG) && _MSVC_LANG > 201402L)
#include <cstdint>
#include <string>
#include <unordered_map>
#endif

/// Wasm Proposal enum class.
#if (defined(__cplusplus) && __cplusplus > 201402L) ||                         \
    (defined(_MSVC_LANG) && _MSVC_LANG > 201402L)
namespace WasmEdge {
enum class Proposal : uint8_t {
  ImportExportMutGlobals = 0,
  NonTrapFloatToIntConversions,
  SignExtensionOperators,
  MultiValue,
  BulkMemoryOperations,
  ReferenceTypes,
  SIMD,
  TailCall,
  Annotations,
  Memory64,
  Threads,
  ExceptionHandling,
  FunctionReferences,
  Max
};

namespace detail {
using namespace std::literals::string_view_literals;
static inline std::unordered_map<Proposal, std::string_view> ProposalStr = {
    {Proposal::ImportExportMutGlobals, "Import/Export of mutable globals"sv},
    {Proposal::NonTrapFloatToIntConversions,
     "Non-trapping float-to-int conversions"sv},
    {Proposal::SignExtensionOperators, "Sign-extension operators"sv},
    {Proposal::MultiValue, "Multi-value returns"sv},
    {Proposal::BulkMemoryOperations, "Bulk memory operations"sv},
    {Proposal::ReferenceTypes, "Reference types"sv},
    {Proposal::SIMD, "Fixed-width SIMD"sv},
    {Proposal::TailCall, "Tail call"sv},
    {Proposal::Annotations, "Custom Annotation Syntax in the Text Format"sv},
    {Proposal::Memory64, "Memory64"sv},
    {Proposal::Threads, "Threads"sv},
    {Proposal::ExceptionHandling, "Exception handling"sv},
    {Proposal::FunctionReferences, "Typed Function References"sv},
};
} // namespace detail
using detail::ProposalStr;
} // namespace WasmEdge
#endif

enum WasmEdge_Proposal {
  WasmEdge_Proposal_ImportExportMutGlobals = 0,
  WasmEdge_Proposal_NonTrapFloatToIntConversions,
  WasmEdge_Proposal_SignExtensionOperators,
  WasmEdge_Proposal_MultiValue,
  WasmEdge_Proposal_BulkMemoryOperations,
  WasmEdge_Proposal_ReferenceTypes,
  WasmEdge_Proposal_SIMD,
  WasmEdge_Proposal_TailCall,
  WasmEdge_Proposal_Annotations,
  WasmEdge_Proposal_Memory64,
  WasmEdge_Proposal_Threads,
  WasmEdge_Proposal_ExceptionHandling,
  WasmEdge_Proposal_FunctionReferences
};

/// Host Module Registration enum class.
#if (defined(__cplusplus) && __cplusplus > 201402L) ||                         \
    (defined(_MSVC_LANG) && _MSVC_LANG > 201402L)
namespace WasmEdge {
enum class HostRegistration : uint8_t { Wasi = 0, WasmEdge_Process, Max };
} // namespace WasmEdge
#endif

enum WasmEdge_HostRegistration {
  WasmEdge_HostRegistration_Wasi = 0,
  WasmEdge_HostRegistration_WasmEdge_Process
};

/// AOT compiler optimization level enumeration.
enum WasmEdge_CompilerOptimizationLevel {
  /// Disable as many optimizations as possible.
  WasmEdge_CompilerOptimizationLevel_O0 = 0,
  /// Optimize quickly without destroying debuggability.
  WasmEdge_CompilerOptimizationLevel_O1,
  /// Optimize for fast execution as much as possible without triggering
  /// significant incremental compile time or code size growth.
  WasmEdge_CompilerOptimizationLevel_O2,
  /// Optimize for fast execution as much as possible.
  WasmEdge_CompilerOptimizationLevel_O3,
  /// Optimize for small code size as much as possible without triggering
  /// significant incremental compile time or execution time slowdowns.
  WasmEdge_CompilerOptimizationLevel_Os,
  /// Optimize for small code size as much as possible.
  WasmEdge_CompilerOptimizationLevel_Oz
};

/// AOT compiler output binary format enumeration.
enum WasmEdge_CompilerOutputFormat {
  /// Native dynamic library format.
  WasmEdge_CompilerOutputFormat_Native = 0,
  /// WebAssembly with AOT compiled codes in custom sections.
  WasmEdge_CompilerOutputFormat_Wasm
};

#endif /// WASMEDGE_C_API_ENUM_CONFIGURE_H
