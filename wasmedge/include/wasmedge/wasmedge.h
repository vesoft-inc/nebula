// SPDX-License-Identifier: Apache-2.0
//===-- wasmedge/wasmedge.h - WasmEdge C API ------------------------------===//
//
// Part of the WasmEdge Project.
//
//===----------------------------------------------------------------------===//
///
/// \file
/// This file contains the function declarations of WasmEdge C API.
///
//===----------------------------------------------------------------------===//

#ifndef WASMEDGE_C_API_H
#define WASMEDGE_C_API_H

#if defined(_WIN32) || defined(_WIN64) || defined(__WIN32__) ||                \
    defined(__TOS_WIN__) || defined(__WINDOWS__)
#ifdef WASMEDGE_COMPILE_LIBRARY
#define WASMEDGE_CAPI_EXPORT __declspec(dllexport)
#else
#define WASMEDGE_CAPI_EXPORT __declspec(dllimport)
#endif /// WASMEDGE_COMPILE_LIBRARY
#else
#define WASMEDGE_CAPI_EXPORT __attribute__((visibility("default")))
#endif /// _WIN32

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "wasmedge/enum_configure.h"
#include "wasmedge/enum_errcode.h"
#include "wasmedge/enum_types.h"
#include "wasmedge/int128.h"
#include "wasmedge/version.h"

/// WasmEdge WASM value struct.
typedef struct WasmEdge_Value {
  uint128_t Value;
  /// The value type is used in the parameters of invoking functions. For the
  /// return values of invoking functions, this member will always be
  /// `WasmEdge_ValType_I32`. Users should use APIs to retrieve the WASM
  /// function's `WasmEdge_FunctionTypeContext` to get the actual return list
  /// value types, and then use the corresponding `WasmEdge_ValueGet` functions
  /// to retrieve the value.
  enum WasmEdge_ValType Type;
} WasmEdge_Value;

/// WasmEdge string struct.
typedef struct WasmEdge_String {
  uint32_t Length;
  const char *Buf;
} WasmEdge_String;

/// Opaque struct of WASM execution result.
typedef struct WasmEdge_Result {
  uint8_t Code;
} WasmEdge_Result;
#define WasmEdge_Result_Success ((WasmEdge_Result){.Code = 0x00})
#define WasmEdge_Result_Terminate ((WasmEdge_Result){.Code = 0x01})
#define WasmEdge_Result_Fail ((WasmEdge_Result){.Code = 0x02})

/// Struct of WASM limit.
typedef struct WasmEdge_Limit {
  /// Boolean to describe has max value or not.
  bool HasMax;
  /// Minimum value.
  uint32_t Min;
  /// Maximum value. Will be ignored if the `HasMax` is false.
  uint32_t Max;
} WasmEdge_Limit;

/// Opaque struct of WasmEdge configure.
typedef struct WasmEdge_ConfigureContext WasmEdge_ConfigureContext;

/// Opaque struct of WasmEdge statistics.
typedef struct WasmEdge_StatisticsContext WasmEdge_StatisticsContext;

/// Opaque struct of WasmEdge AST module.
typedef struct WasmEdge_ASTModuleContext WasmEdge_ASTModuleContext;

/// Opaque struct of WasmEdge function type.
typedef struct WasmEdge_FunctionTypeContext WasmEdge_FunctionTypeContext;

/// Opaque struct of WasmEdge memory type.
typedef struct WasmEdge_MemoryTypeContext WasmEdge_MemoryTypeContext;

/// Opaque struct of WasmEdge table type.
typedef struct WasmEdge_TableTypeContext WasmEdge_TableTypeContext;

/// Opaque struct of WasmEdge global type.
typedef struct WasmEdge_GlobalTypeContext WasmEdge_GlobalTypeContext;

/// Opaque struct of WasmEdge import type.
typedef struct WasmEdge_ImportTypeContext WasmEdge_ImportTypeContext;

/// Opaque struct of WasmEdge export type.
typedef struct WasmEdge_ExportTypeContext WasmEdge_ExportTypeContext;

/// Opaque struct of WasmEdge AOT compiler.
typedef struct WasmEdge_CompilerContext WasmEdge_CompilerContext;

/// Opaque struct of WasmEdge loader.
typedef struct WasmEdge_LoaderContext WasmEdge_LoaderContext;

/// Opaque struct of WasmEdge validator.
typedef struct WasmEdge_ValidatorContext WasmEdge_ValidatorContext;

/// Opaque struct of WasmEdge executor.
typedef struct WasmEdge_ExecutorContext WasmEdge_ExecutorContext;

/// Opaque struct of WasmEdge store.
typedef struct WasmEdge_StoreContext WasmEdge_StoreContext;

/// Opaque struct of WasmEdge function instance.
typedef struct WasmEdge_FunctionInstanceContext
    WasmEdge_FunctionInstanceContext;

/// Opaque struct of WasmEdge table instance.
typedef struct WasmEdge_TableInstanceContext WasmEdge_TableInstanceContext;

/// Opaque struct of WasmEdge memory instance.
typedef struct WasmEdge_MemoryInstanceContext WasmEdge_MemoryInstanceContext;

/// Opaque struct of WasmEdge global instance.
typedef struct WasmEdge_GlobalInstanceContext WasmEdge_GlobalInstanceContext;

/// Opaque struct of WasmEdge import object.
typedef struct WasmEdge_ImportObjectContext WasmEdge_ImportObjectContext;

/// Opaque struct of WasmEdge VM.
typedef struct WasmEdge_VMContext WasmEdge_VMContext;

#ifdef __cplusplus
extern "C" {
#endif

/// >>>>>>>> WasmEdge version functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Get the version string of the WasmEdge C API.
///
/// The returned string must __NOT__ be freed.
///
/// \returns NULL-terminated C string of version.
WASMEDGE_CAPI_EXPORT extern const char *WasmEdge_VersionGet(void);

/// Get the major version value of the WasmEdge C API.
///
/// \returns Value of the major version.
WASMEDGE_CAPI_EXPORT extern uint32_t WasmEdge_VersionGetMajor(void);

/// Get the minor version value of the WasmEdge C API.
///
/// \returns Value of the minor version.
WASMEDGE_CAPI_EXPORT extern uint32_t WasmEdge_VersionGetMinor(void);

/// Get the patch version value of the WasmEdge C API.
///
/// \returns Value of the patch version.
WASMEDGE_CAPI_EXPORT extern uint32_t WasmEdge_VersionGetPatch(void);

/// <<<<<<<< WasmEdge version functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge logging functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Set the logging system to filter to error level.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_LogSetErrorLevel(void);

/// Set the logging system to filter to debug level.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_LogSetDebugLevel(void);

/// <<<<<<<< WasmEdge logging functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge value functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Generate the I32 WASM value.
///
/// \param Val the I32 value.
///
/// \returns WasmEdge_Value struct with the I32 value.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Value
WasmEdge_ValueGenI32(const int32_t Val);

/// Generate the I64 WASM value.
///
/// \param Val the I64 value.
///
/// \returns WasmEdge_Value struct with the I64 value.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Value
WasmEdge_ValueGenI64(const int64_t Val);

/// Generate the F32 WASM value.
///
/// \param Val the F32 value.
///
/// \returns WasmEdge_Value struct with the F32 value.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Value
WasmEdge_ValueGenF32(const float Val);

/// Generate the F64 WASM value.
///
/// \param Val the F64 value.
///
/// \returns WasmEdge_Value struct with the F64 value.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Value
WasmEdge_ValueGenF64(const double Val);

/// Generate the V128 WASM value.
///
/// \param Val the V128 value.
///
/// \returns WasmEdge_Value struct with the V128 value.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Value
WasmEdge_ValueGenV128(const int128_t Val);

/// Generate the NULL reference WASM value.
///
/// The values generated by this function are only meaningful when the
/// `WasmEdge_Proposal_BulkMemoryOperations` or the
/// `WasmEdge_Proposal_ReferenceTypes` turns on in configuration.
///
/// \param T the reference type.
///
/// \returns WasmEdge_Value struct with the NULL reference.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Value
WasmEdge_ValueGenNullRef(const enum WasmEdge_RefType T);

/// Generate the function reference WASM value.
///
/// The values generated by this function are only meaningful when the
/// `WasmEdge_Proposal_BulkMemoryOperations` or the
/// `WasmEdge_Proposal_ReferenceTypes` turns on in configuration.
///
/// \param Index the function index.
///
/// \returns WasmEdge_Value struct with the function reference.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Value
WasmEdge_ValueGenFuncRef(const uint32_t Index);

/// Generate the function reference WASM value.
///
/// The values generated by this function are only meaningful when the
/// `WasmEdge_Proposal_ReferenceTypes` turns on in configuration.
///
/// \param Ref the reference to the external object.
///
/// \returns WasmEdge_Value struct with the external reference.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Value
WasmEdge_ValueGenExternRef(void *Ref);

/// Retrieve the I32 value from the WASM value.
///
/// \param Val the WasmEdge_Value struct.
///
/// \returns I32 value in the input struct.
WASMEDGE_CAPI_EXPORT extern int32_t
WasmEdge_ValueGetI32(const WasmEdge_Value Val);

/// Retrieve the I64 value from the WASM value.
///
/// \param Val the WasmEdge_Value struct.
///
/// \returns I64 value in the input struct.
WASMEDGE_CAPI_EXPORT extern int64_t
WasmEdge_ValueGetI64(const WasmEdge_Value Val);

/// Retrieve the F32 value from the WASM value.
///
/// \param Val the WasmEdge_Value struct.
///
/// \returns F32 value in the input struct.
WASMEDGE_CAPI_EXPORT extern float
WasmEdge_ValueGetF32(const WasmEdge_Value Val);

/// Retrieve the F64 value from the WASM value.
///
/// \param Val the WasmEdge_Value struct.
///
/// \returns F64 value in the input struct.
WASMEDGE_CAPI_EXPORT extern double
WasmEdge_ValueGetF64(const WasmEdge_Value Val);

/// Retrieve the V128 value from the WASM value.
///
/// \param Val the WasmEdge_Value struct.
///
/// \returns V128 value in the input struct.
WASMEDGE_CAPI_EXPORT extern int128_t
WasmEdge_ValueGetV128(const WasmEdge_Value Val);

/// Specify the WASM value is a null reference or not.
///
/// \param Val the WasmEdge_Value struct.
///
/// \returns true if the value is a null reference, false if not.
WASMEDGE_CAPI_EXPORT extern bool
WasmEdge_ValueIsNullRef(const WasmEdge_Value Val);

/// Retrieve the function index from the WASM value.
///
/// \param Val the WasmEdge_Value struct.
///
/// \returns function index in the input struct.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_ValueGetFuncIdx(const WasmEdge_Value Val);

/// Retrieve the external reference from the WASM value.
///
/// \param Val the WasmEdge_Value struct.
///
/// \returns external reference in the input struct.
WASMEDGE_CAPI_EXPORT extern void *
WasmEdge_ValueGetExternRef(const WasmEdge_Value Val);

/// <<<<<<<< WasmEdge value functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// <<<<<<<< WasmEdge string functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// Creation of the WasmEdge_String with the C string.
///
/// The caller owns the object and should call `WasmEdge_StringDelete` to free
/// it. This function only supports the C string with NULL termination. If the
/// input string may have `\0` character, please use the
/// `WasmEdge_StringCreateByBuffer` instead.
///
/// \param Str the NULL-terminated C string to copy into the WasmEdge_String
/// object.
///
/// \returns string object. Length will be 0 and Buf will be NULL if failed or
/// the input string is a NULL.
WASMEDGE_CAPI_EXPORT extern WasmEdge_String
WasmEdge_StringCreateByCString(const char *Str);

/// Creation of the WasmEdge_String with the buffer and its length.
///
/// The caller owns the object and should call `WasmEdge_StringDelete` to free
/// it.
///
/// \param Buf the buffer to copy into the WasmEdge_String object.
/// \param Len the buffer length.
///
/// \returns string object. Length will be 0 and Buf will be NULL if failed or
/// the input buffer is a NULL.
WASMEDGE_CAPI_EXPORT extern WasmEdge_String
WasmEdge_StringCreateByBuffer(const char *Buf, const uint32_t Len);

/// Create the WasmEdge_String wraps to the buffer.
///
/// This function creates a `WasmEdge_String` object which wraps to the input
/// buffer. The caller should guarantee the life cycle of the input buffer, and
/// should __NOT__ call the `WasmEdge_StringDelete`.
///
/// \param Buf the buffer to copy into the WasmEdge_String object.
/// \param Len the buffer length.
///
/// \returns string object refer to the input buffer with its length.
WASMEDGE_CAPI_EXPORT extern WasmEdge_String
WasmEdge_StringWrap(const char *Buf, const uint32_t Len);

/// Compare the two WasmEdge_String objects.
///
/// \param Str1 the first WasmEdge_String object to compare.
/// \param Str2 the second WasmEdge_String object to compare.
///
/// \returns true if the content of two WasmEdge_String objects are the same,
/// false if not.
WASMEDGE_CAPI_EXPORT extern bool
WasmEdge_StringIsEqual(const WasmEdge_String Str1, const WasmEdge_String Str2);

/// Copy the content of WasmEdge_String object to the buffer.
///
/// This function copy at most `Len` characters from the `WasmEdge_String`
/// object to the destination buffer. If the string length is less than `Len`
/// characters long, the remainder of the buffer is filled with `\0' characters.
/// Otherwise, the destination is not terminated.
///
/// \param Str the source WasmEdge_String object to copy.
/// \param Buf the buffer to fill the string content.
/// \param Len the buffer length.
///
/// \returns the copied length of string.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StringCopy(const WasmEdge_String Str, char *Buf, const uint32_t Len);

/// Deletion of the WasmEdge_String.
///
/// After calling this function, the resources in the WasmEdge_String object
/// will be released and the object should __NOT__ be used.
///
/// \param Str the WasmEdge_String object to delete.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_StringDelete(WasmEdge_String Str);

/// >>>>>>>> WasmEdge string functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// >>>>>>>> WasmEdge result functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Check the result is a success or not.
///
/// \param Res the WasmEdge_Result struct.
///
/// \returns true if the error code is WasmEdge_Result_Success or
/// WasmEdge_Result_Terminate, false for others.
WASMEDGE_CAPI_EXPORT extern bool WasmEdge_ResultOK(const WasmEdge_Result Res);

/// Get the result code.
///
/// \param Res the WasmEdge_Result struct.
///
/// \returns corresponding result code.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_ResultGetCode(const WasmEdge_Result Res);

/// Get the result message.
///
/// The returned string must __NOT__ be freed.
///
/// \param Res the WasmEdge_Result struct.
///
/// \returns NULL-terminated C string of the corresponding error message.
WASMEDGE_CAPI_EXPORT extern const char *
WasmEdge_ResultGetMessage(const WasmEdge_Result Res);

/// <<<<<<<< WasmEdge result functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge limit functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Compare the two WasmEdge_Limit objects.
///
/// \param Lim1 the first WasmEdge_Limit object to compare.
/// \param Lim2 the second WasmEdge_Limit object to compare.
///
/// \returns true if the content of two WasmEdge_Limit objects are the same,
/// false if not.
WASMEDGE_CAPI_EXPORT extern bool
WasmEdge_LimitIsEqual(const WasmEdge_Limit Lim1, const WasmEdge_Limit Lim2);

/// <<<<<<<< WasmEdge limit functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge configure functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_ConfigureContext.
///
/// The caller owns the object and should call `WasmEdge_ConfigureDelete` to
/// free it.
///
/// \returns pointer to the context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_ConfigureContext *
WasmEdge_ConfigureCreate(void);

/// Add a proposal setting into the WasmEdge_ConfigureContext.
///
/// For turning on a specific WASM proposal in WasmEdge_VMContext, you can set
/// the proposal value into the WasmEdge_ConfigureContext and create VM with
/// this context.
/// ```c
/// WasmEdge_ConfigureContext *Conf = WasmEdge_ConfigureCreate();
/// WasmEdge_ConfigureAddProposal(Conf, WasmEdge_Proposal_BulkMemoryOperations);
/// WasmEdge_ConfigureAddProposal(Conf, WasmEdge_Proposal_ReferenceTypes);
/// WasmEdge_ConfigureAddProposal(Conf, WasmEdge_Proposal_SIMD);
/// WasmEdge_VMContext *VM = WasmEdge_VMCreate(Conf, NULL);
/// ```
///
/// \param Cxt the WasmEdge_ConfigureContext to add the proposal value.
/// \param Prop the proposal value.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ConfigureAddProposal(WasmEdge_ConfigureContext *Cxt,
                              const enum WasmEdge_Proposal Prop);

/// Remove a proposal setting in the WasmEdge_ConfigureContext.
///
/// \param Cxt the WasmEdge_ConfigureContext to remove the proposal.
/// \param Prop the proposal value.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ConfigureRemoveProposal(WasmEdge_ConfigureContext *Cxt,
                                 const enum WasmEdge_Proposal Prop);

/// Check if a proposal setting exists in the WasmEdge_ConfigureContext or not.
///
/// \param Cxt the WasmEdge_ConfigureContext to check the proposal value.
/// \param Prop the proposal value.
///
/// \returns true if the proposal setting exists, false if not.
WASMEDGE_CAPI_EXPORT extern bool
WasmEdge_ConfigureHasProposal(const WasmEdge_ConfigureContext *Cxt,
                              const enum WasmEdge_Proposal Prop);

/// Add a host pre-registration setting into WasmEdge_ConfigureContext.
///
/// For turning on the Wasi support in `WasmEdge_VMContext`, you can set the
/// host pre-registration value into the `WasmEdge_ConfigureContext` and create
/// VM with this context.
/// ```c
/// WasmEdge_ConfigureContext *Conf = WasmEdge_ConfigureCreate();
/// WasmEdge_ConfigureAddHostRegistration(Conf, WasmEdge_HostRegistration_Wasi);
/// WasmEdge_VMContext *VM = WasmEdge_VMCreate(Conf, NULL);
/// ```
///
/// \param Cxt the WasmEdge_ConfigureContext to add host pre-registration.
/// \param Host the host pre-registration value.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_ConfigureAddHostRegistration(
    WasmEdge_ConfigureContext *Cxt, const enum WasmEdge_HostRegistration Host);

/// Remove a host pre-registration setting in the WasmEdge_ConfigureContext.
///
/// \param Cxt the WasmEdge_ConfigureContext to remove the host
/// pre-registration.
/// \param Host the host pre-registration value.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_ConfigureRemoveHostRegistration(
    WasmEdge_ConfigureContext *Cxt, const enum WasmEdge_HostRegistration Host);

/// Check if a host pre-registration setting exists in the
/// WasmEdge_ConfigureContext or not.
///
/// \param Cxt the WasmEdge_ConfigureContext to check the host pre-registration.
/// \param Host the host pre-registration value.
///
/// \returns true if the host pre-registration setting exists, false if not.
WASMEDGE_CAPI_EXPORT extern bool WasmEdge_ConfigureHasHostRegistration(
    const WasmEdge_ConfigureContext *Cxt,
    const enum WasmEdge_HostRegistration Host);

/// Set the page limit of memory instances.
///
/// Limit the page count (64KiB per page) in memory instances.
///
/// \param Cxt the WasmEdge_ConfigureContext to set the maximum page count.
/// \param Page the maximum page count.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ConfigureSetMaxMemoryPage(WasmEdge_ConfigureContext *Cxt,
                                   const uint32_t Page);

/// Get the page limit of memory instances.
///
/// \param Cxt the WasmEdge_ConfigureContext to get the maximum page count
/// setting.
///
/// \returns the page count limitation value.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_ConfigureGetMaxMemoryPage(const WasmEdge_ConfigureContext *Cxt);

/// Set the optimization level of AOT compiler.
///
/// \param Cxt the WasmEdge_ConfigureContext to set the optimization level.
/// \param Level the AOT compiler optimization level.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_ConfigureCompilerSetOptimizationLevel(
    WasmEdge_ConfigureContext *Cxt,
    const enum WasmEdge_CompilerOptimizationLevel Level);

/// Get the optimization level of AOT compiler.
///
/// \param Cxt the WasmEdge_ConfigureContext to get the optimization level.
///
/// \returns the AOT compiler optimization level.
WASMEDGE_CAPI_EXPORT extern enum WasmEdge_CompilerOptimizationLevel
WasmEdge_ConfigureCompilerGetOptimizationLevel(
    const WasmEdge_ConfigureContext *Cxt);

/// Set the output binary format of AOT compiler.
///
/// \param Cxt the WasmEdge_ConfigureContext to set the output binary format.
/// \param Format the AOT compiler output binary format.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_ConfigureCompilerSetOutputFormat(
    WasmEdge_ConfigureContext *Cxt,
    const enum WasmEdge_CompilerOutputFormat Format);

/// Get the output binary format of AOT compiler.
///
/// \param Cxt the WasmEdge_ConfigureContext to get the output binary format.
///
/// \returns the AOT compiler output binary format.
WASMEDGE_CAPI_EXPORT extern enum WasmEdge_CompilerOutputFormat
WasmEdge_ConfigureCompilerGetOutputFormat(const WasmEdge_ConfigureContext *Cxt);

/// Set the dump IR option of AOT compiler.
///
/// \param Cxt the WasmEdge_ConfigureContext to set the boolean value.
/// \param IsDump the boolean value to determine to dump IR or not when
/// compilation in AOT compiler.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ConfigureCompilerSetDumpIR(WasmEdge_ConfigureContext *Cxt,
                                    const bool IsDump);

/// Get the dump IR option of AOT compiler.
///
/// \param Cxt the WasmEdge_ConfigureContext to get the boolean value.
///
/// \returns the boolean value to determine to dump IR or not when compilation
/// in AOT compiler.
WASMEDGE_CAPI_EXPORT extern bool
WasmEdge_ConfigureCompilerIsDumpIR(const WasmEdge_ConfigureContext *Cxt);

/// Set the generic binary option of AOT compiler.
///
/// \param Cxt the WasmEdge_ConfigureContext to set the boolean value.
/// \param IsGeneric the boolean value to determine to generate the generic
/// binary or not when compilation in AOT compiler.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ConfigureCompilerSetGenericBinary(WasmEdge_ConfigureContext *Cxt,
                                           const bool IsGeneric);

/// Get the generic binary option of AOT compiler.
///
/// \param Cxt the WasmEdge_ConfigureContext to get the boolean value.
///
/// \returns the boolean value to determine to generate the generic binary or
/// not when compilation in AOT compiler.
WASMEDGE_CAPI_EXPORT extern bool
WasmEdge_ConfigureCompilerIsGenericBinary(const WasmEdge_ConfigureContext *Cxt);

/// Set the instruction counting option.
///
/// \param Cxt the WasmEdge_ConfigureContext to set the boolean value.
/// \param IsCount the boolean value to determine to support instruction
/// counting when execution or not after compilation by the AOT compiler.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ConfigureStatisticsSetInstructionCounting(
    WasmEdge_ConfigureContext *Cxt, const bool IsCount);

/// Get the instruction counting option.
///
/// \param Cxt the WasmEdge_ConfigureContext to get the boolean value.
///
/// \returns the boolean value to determine to support instruction counting when
/// execution or not after compilation by the AOT compiler.
WASMEDGE_CAPI_EXPORT extern bool
WasmEdge_ConfigureStatisticsIsInstructionCounting(
    const WasmEdge_ConfigureContext *Cxt);

/// Set the cost measuring option.
///
/// \param Cxt the WasmEdge_ConfigureContext to set the boolean value.
/// \param IsMeasure the boolean value to determine to support cost measuring
/// when execution or not after compilation by the AOT compiler.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ConfigureStatisticsSetCostMeasuring(WasmEdge_ConfigureContext *Cxt,
                                             const bool IsMeasure);

/// Get the cost measuring option.
///
/// \param Cxt the WasmEdge_ConfigureContext to get the boolean value.
///
/// \returns the boolean value to determine to support cost measuring when
/// execution or not after compilation by the AOT compiler.
WASMEDGE_CAPI_EXPORT extern bool WasmEdge_ConfigureStatisticsIsCostMeasuring(
    const WasmEdge_ConfigureContext *Cxt);

/// Set the time measuring option.
///
/// \param Cxt the WasmEdge_ConfigureContext to set the boolean value.
/// \param IsMeasure the boolean value to determine to support time when
/// execution or not after compilation by the AOT compiler.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ConfigureStatisticsSetTimeMeasuring(WasmEdge_ConfigureContext *Cxt,
                                             const bool IsMeasure);

/// Get the time measuring option.
///
/// \param Cxt the WasmEdge_ConfigureContext to get the boolean value.
///
/// \returns the boolean value to determine to support time measuring when
/// execution or not after compilation by the AOT compiler.
WASMEDGE_CAPI_EXPORT extern bool WasmEdge_ConfigureStatisticsIsTimeMeasuring(
    const WasmEdge_ConfigureContext *Cxt);

/// Deletion of the WasmEdge_ConfigureContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_ConfigureContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ConfigureDelete(WasmEdge_ConfigureContext *Cxt);

/// <<<<<<<< WasmEdge configure functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge statistics functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_StatisticsContext.
///
/// The caller owns the object and should call `WasmEdge_StatisticsDelete` to
/// free it.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_StatisticsContext *
WasmEdge_StatisticsCreate(void);

/// Get the instruction count in execution.
///
/// \param Cxt the WasmEdge_StatisticsContext to get data.
///
/// \returns the instruction count in total execution.
WASMEDGE_CAPI_EXPORT extern uint64_t
WasmEdge_StatisticsGetInstrCount(const WasmEdge_StatisticsContext *Cxt);

/// Get the instruction count per second in execution.
///
/// \param Cxt the WasmEdge_StatisticsContext to get data.
///
/// \returns the instruction count per second.
WASMEDGE_CAPI_EXPORT extern double
WasmEdge_StatisticsGetInstrPerSecond(const WasmEdge_StatisticsContext *Cxt);

/// Get the total cost in execution.
///
/// \param Cxt the WasmEdge_StatisticsContext to get data.
///
/// \returns the total cost.
WASMEDGE_CAPI_EXPORT extern uint64_t
WasmEdge_StatisticsGetTotalCost(const WasmEdge_StatisticsContext *Cxt);

/// Set the costs of instructions.
///
/// \param Cxt the WasmEdge_StatisticsContext to set the cost table.
/// \param CostArr the cost table array.
/// \param Len the length of the cost table array.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_StatisticsSetCostTable(WasmEdge_StatisticsContext *Cxt,
                                uint64_t *CostArr, const uint32_t Len);

/// Set the cost limit in execution.
///
/// The WASM execution will be aborted if the instruction costs exceeded the
/// limit and the ErrCode::CostLimitExceeded will be returned.
///
/// \param Cxt the WasmEdge_StatisticsContext to set the cost table.
/// \param Limit the cost limit.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_StatisticsSetCostLimit(WasmEdge_StatisticsContext *Cxt,
                                const uint64_t Limit);

/// Deletion of the WasmEdge_StatisticsContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_StatisticsContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_StatisticsDelete(WasmEdge_StatisticsContext *Cxt);

/// <<<<<<<< WasmEdge statistics functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge AST module functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Get the length of imports list of the AST module.
///
/// \param Cxt the WasmEdge_ASTModuleContext.
///
/// \returns length of the imports list.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_ASTModuleListImportsLength(const WasmEdge_ASTModuleContext *Cxt);

/// List the imports of the AST module.
///
/// If the `Imports` buffer length is smaller than the result of the imports
/// list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_ASTModuleContext.
/// \param [out] Imports the import type contexts buffer. Can be NULL if import
/// types are not needed.
/// \param Len the buffer length.
///
/// \returns actual exported function list size.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_ASTModuleListImports(const WasmEdge_ASTModuleContext *Cxt,
                              const WasmEdge_ImportTypeContext **Imports,
                              const uint32_t Len);

/// Get the length of exports list of the AST module.
///
/// \param Cxt the WasmEdge_ASTModuleContext.
///
/// \returns length of the exports list.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_ASTModuleListExportsLength(const WasmEdge_ASTModuleContext *Cxt);

/// List the exports of the AST module.
///
/// If the `Exports` buffer length is smaller than the result of the exports
/// list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_ASTModuleContext.
/// \param [out] Exports the export type contexts buffer. Can be NULL if export
/// types are not needed.
/// \param Len the buffer length.
///
/// \returns actual exported function list size.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_ASTModuleListExports(const WasmEdge_ASTModuleContext *Cxt,
                              const WasmEdge_ExportTypeContext **Exports,
                              const uint32_t Len);

/// Deletion of the WasmEdge_ASTModuleContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_ASTModuleContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ASTModuleDelete(WasmEdge_ASTModuleContext *Cxt);

/// <<<<<<<< WasmEdge AST module functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge function type functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_FunctionTypeContext.
///
/// The caller owns the object and should call `WasmEdge_FunctionTypeDelete` to
/// free it.
///
/// \param ParamList the value types list of parameters. NULL if the length is
/// 0.
/// \param ParamLen the ParamList buffer length.
/// \param ReturnList the value types list of returns. NULL if the length is 0.
/// \param ReturnLen the ReturnList buffer length.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_FunctionTypeContext *
WasmEdge_FunctionTypeCreate(const enum WasmEdge_ValType *ParamList,
                            const uint32_t ParamLen,
                            const enum WasmEdge_ValType *ReturnList,
                            const uint32_t ReturnLen);

/// Get the parameter types list length from the WasmEdge_FunctionTypeContext.
///
/// \param Cxt the WasmEdge_FunctionTypeContext.
///
/// \returns the parameter types list length.
WASMEDGE_CAPI_EXPORT extern uint32_t WasmEdge_FunctionTypeGetParametersLength(
    const WasmEdge_FunctionTypeContext *Cxt);

/// Get the parameter types list from the WasmEdge_FunctionTypeContext.
///
/// If the `List` buffer length is smaller than the length of the parameter type
/// list, the overflowed values will be discarded.
///
/// \param Cxt the WasmEdge_FunctionTypeContext.
/// \param [out] List the WasmEdge_ValType buffer to fill the parameter value
/// types.
/// \param Len the value type buffer length.
///
/// \returns the actual parameter types list length.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_FunctionTypeGetParameters(const WasmEdge_FunctionTypeContext *Cxt,
                                   enum WasmEdge_ValType *List,
                                   const uint32_t Len);

/// Get the return types list length from the WasmEdge_FunctionTypeContext.
///
/// \param Cxt the WasmEdge_FunctionTypeContext.
///
/// \returns the return types list length.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_FunctionTypeGetReturnsLength(const WasmEdge_FunctionTypeContext *Cxt);

/// Get the return types list from the WasmEdge_FunctionTypeContext.
///
/// If the `List` buffer length is smaller than the length of the return type
/// list, the overflowed values will be discarded.
///
/// \param Cxt the WasmEdge_FunctionTypeContext.
/// \param [out] List the WasmEdge_ValType buffer to fill the return value
/// types.
/// \param Len the value type buffer length.
///
/// \returns the actual return types list length.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_FunctionTypeGetReturns(const WasmEdge_FunctionTypeContext *Cxt,
                                enum WasmEdge_ValType *List,
                                const uint32_t Len);

/// Deletion of the WasmEdge_FunctionTypeContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_FunctionTypeContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_FunctionTypeDelete(WasmEdge_FunctionTypeContext *Cxt);

/// <<<<<<<< WasmEdge function type functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge table type functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_TableTypeContext.
///
/// The caller owns the object and should call `WasmEdge_TableTypeDelete` to
/// free it.
///
/// \param RefType the reference type of the table type.
/// \param Limit the limit struct of the table type.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_TableTypeContext *
WasmEdge_TableTypeCreate(const enum WasmEdge_RefType RefType,
                         const WasmEdge_Limit Limit);

/// Get the reference type from a table type.
///
/// \param Cxt the WasmEdge_TableTypeContext.
///
/// \returns the reference type of the table type.
WASMEDGE_CAPI_EXPORT extern enum WasmEdge_RefType
WasmEdge_TableTypeGetRefType(const WasmEdge_TableTypeContext *Cxt);

/// Get the limit from a table type.
///
/// \param Cxt the WasmEdge_TableTypeContext.
///
/// \returns the limit struct of the table type.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Limit
WasmEdge_TableTypeGetLimit(const WasmEdge_TableTypeContext *Cxt);

/// Deletion of the WasmEdge_TableTypeContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_TableTypeContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_TableTypeDelete(WasmEdge_TableTypeContext *Cxt);

/// <<<<<<<< WasmEdge table type functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge memory type functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_MemoryTypeContext.
///
/// The caller owns the object and should call `WasmEdge_MemoryTypeDelete` to
/// free it.
///
/// \param Limit the limit struct of the memory type.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_MemoryTypeContext *
WasmEdge_MemoryTypeCreate(const WasmEdge_Limit Limit);

/// Get the limit from a memory type.
///
/// \param Cxt the WasmEdge_MemoryTypeContext.
///
/// \returns the limit struct of the memory type.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Limit
WasmEdge_MemoryTypeGetLimit(const WasmEdge_MemoryTypeContext *Cxt);

/// Deletion of the WasmEdge_MemoryTypeContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_MemoryTypeContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_MemoryTypeDelete(WasmEdge_MemoryTypeContext *Cxt);

/// <<<<<<<< WasmEdge memory type functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge global type functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_GlobalTypeContext.
///
/// The caller owns the object and should call `WasmEdge_GlobalTypeDelete` to
/// free it.
///
/// \param ValType the value type of the global type.
/// \param Mut the mutation of the global type.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_GlobalTypeContext *
WasmEdge_GlobalTypeCreate(const enum WasmEdge_ValType ValType,
                          const enum WasmEdge_Mutability Mut);

/// Get the value type from a global type.
///
/// \param Cxt the WasmEdge_GlobalTypeContext.
///
/// \returns the value type of the global type.
WASMEDGE_CAPI_EXPORT extern enum WasmEdge_ValType
WasmEdge_GlobalTypeGetValType(const WasmEdge_GlobalTypeContext *Cxt);

/// Get the mutability from a global type.
///
/// \param Cxt the WasmEdge_GlobalTypeContext.
///
/// \returns the mutability of the global type.
WASMEDGE_CAPI_EXPORT extern enum WasmEdge_Mutability
WasmEdge_GlobalTypeGetMutability(const WasmEdge_GlobalTypeContext *Cxt);

/// Deletion of the WasmEdge_GlobalTypeContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_GlobalTypeContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_GlobalTypeDelete(WasmEdge_GlobalTypeContext *Cxt);

/// <<<<<<<< WasmEdge global type functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge import type functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Get the external type from an import type.
///
/// \param Cxt the WasmEdge_ImportTypeContext.
///
/// \returns the external type of the import type.
WASMEDGE_CAPI_EXPORT extern enum WasmEdge_ExternalType
WasmEdge_ImportTypeGetExternalType(const WasmEdge_ImportTypeContext *Cxt);

/// Get the module name from an import type.
///
/// The returned string object is linked to the module name of the import type,
/// and the caller should __NOT__ call the `WasmEdge_StringDelete`.
///
/// \param Cxt the WasmEdge_ImportTypeContext.
///
/// \returns string object. Length will be 0 and Buf will be NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_String
WasmEdge_ImportTypeGetModuleName(const WasmEdge_ImportTypeContext *Cxt);

/// Get the external name from an import type.
///
/// The returned string object is linked to the external name of the import
/// type, and the caller should __NOT__ call the `WasmEdge_StringDelete`.
///
/// \param Cxt the WasmEdge_ImportTypeContext.
///
/// \returns string object. Length will be 0 and Buf will be NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_String
WasmEdge_ImportTypeGetExternalName(const WasmEdge_ImportTypeContext *Cxt);

/// Get the external value (which is function type) from an import type.
///
/// The import type context should be the one queried from the AST module
/// context, or this function will cause unexpected error.
/// The function type context links to the function type in the import type
/// context and the AST module context. The caller should __NOT__ call the
/// `WasmEdge_FunctionTypeDelete`.
///
/// \param ASTCxt the WasmEdge_ASTModuleContext.
/// \param Cxt the WasmEdge_ImportTypeContext which queried from the `ASTCxt`.
///
/// \returns the function type. NULL if failed or the external type of the
/// import type is not `WasmEdge_ExternalType_Function`.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_FunctionTypeContext *
WasmEdge_ImportTypeGetFunctionType(const WasmEdge_ASTModuleContext *ASTCxt,
                                   const WasmEdge_ImportTypeContext *Cxt);

/// Get the external value (which is table type) from an import type.
///
/// The import type context should be the one queried from the AST module
/// context, or this function will cause unexpected error.
/// The table type context links to the table type in the import type context
/// and the AST module context. The caller should __NOT__ call the
/// `WasmEdge_TableTypeDelete`.
///
/// \param ASTCxt the WasmEdge_ASTModuleContext.
/// \param Cxt the WasmEdge_ImportTypeContext which queried from the `ASTCxt`.
///
/// \returns the table type. NULL if failed or the external type of the import
/// type is not `WasmEdge_ExternalType_Table`.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_TableTypeContext *
WasmEdge_ImportTypeGetTableType(const WasmEdge_ASTModuleContext *ASTCxt,
                                const WasmEdge_ImportTypeContext *Cxt);

/// Get the external value (which is memory type) from an import type.
///
/// The import type context should be the one queried from the AST module
/// context, or this function will cause unexpected error.
/// The memory type context links to the memory type in the import type context
/// and the AST module context. The caller should __NOT__ call the
/// `WasmEdge_MemoryTypeDelete`.
///
/// \param ASTCxt the WasmEdge_ASTModuleContext.
/// \param Cxt the WasmEdge_ImportTypeContext which queried from the `ASTCxt`.
///
/// \returns the memory type. NULL if failed or the external type of the import
/// type is not `WasmEdge_ExternalType_Memory`.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_MemoryTypeContext *
WasmEdge_ImportTypeGetMemoryType(const WasmEdge_ASTModuleContext *ASTCxt,
                                 const WasmEdge_ImportTypeContext *Cxt);

/// Get the external value (which is global type) from an import type.
///
/// The import type context should be the one queried from the AST module
/// context, or this function will cause unexpected error.
/// The global type context links to the global type in the import type context
/// and the AST module context. The caller should __NOT__ call the
/// `WasmEdge_GlobalTypeDelete`.
///
/// \param ASTCxt the WasmEdge_ASTModuleContext.
/// \param Cxt the WasmEdge_ImportTypeContext which queried from the `ASTCxt`.
///
/// \returns the global type. NULL if failed or the external type of the import
/// type is not `WasmEdge_ExternalType_Global`.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_GlobalTypeContext *
WasmEdge_ImportTypeGetGlobalType(const WasmEdge_ASTModuleContext *ASTCxt,
                                 const WasmEdge_ImportTypeContext *Cxt);

/// <<<<<<<< WasmEdge import type functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge export type functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Get the external type from an export type.
///
/// \param Cxt the WasmEdge_ExportTypeContext.
///
/// \returns the external type of the export type.
WASMEDGE_CAPI_EXPORT extern enum WasmEdge_ExternalType
WasmEdge_ExportTypeGetExternalType(const WasmEdge_ExportTypeContext *Cxt);

/// Get the external name from an export type.
///
/// The returned string object is linked to the external name of the export
/// type, and the caller should __NOT__ call the `WasmEdge_StringDelete`.
///
/// \param Cxt the WasmEdge_ExportTypeContext.
///
/// \returns string object. Length will be 0 and Buf will be NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_String
WasmEdge_ExportTypeGetExternalName(const WasmEdge_ExportTypeContext *Cxt);

/// Get the external value (which is function type) from an export type.
///
/// The export type context should be the one queried from the AST module
/// context, or this function will cause unexpected error.
/// The function type context links to the function type in the export type
/// context and the AST module context. The caller should __NOT__ call the
/// `WasmEdge_FunctionTypeDelete`.
///
/// \param ASTCxt the WasmEdge_ASTModuleContext.
/// \param Cxt the WasmEdge_ExportTypeContext which queried from the `ASTCxt`.
///
/// \returns the function type. NULL if failed or the external type of the
/// export type is not `WasmEdge_ExternalType_Function`.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_FunctionTypeContext *
WasmEdge_ExportTypeGetFunctionType(const WasmEdge_ASTModuleContext *ASTCxt,
                                   const WasmEdge_ExportTypeContext *Cxt);

/// Get the external value (which is table type) from an export type.
///
/// The export type context should be the one queried from the AST module
/// context, or this function will cause unexpected error.
/// The table type context links to the table type in the export type context
/// and the AST module context. The caller should __NOT__ call the
/// `WasmEdge_TableTypeDelete`.
///
/// \param ASTCxt the WasmEdge_ASTModuleContext.
/// \param Cxt the WasmEdge_ExportTypeContext which queried from the `ASTCxt`.
///
/// \returns the table type. NULL if failed or the external type of the export
/// type is not `WasmEdge_ExternalType_Table`.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_TableTypeContext *
WasmEdge_ExportTypeGetTableType(const WasmEdge_ASTModuleContext *ASTCxt,
                                const WasmEdge_ExportTypeContext *Cxt);

/// Get the external value (which is memory type) from an export type.
///
/// The export type context should be the one queried from the AST module
/// context, or this function will cause unexpected error.
/// The memory type context links to the memory type in the export type context
/// and the AST module context. The caller should __NOT__ call the
/// `WasmEdge_MemoryTypeDelete`.
///
/// \param ASTCxt the WasmEdge_ASTModuleContext.
/// \param Cxt the WasmEdge_ExportTypeContext which queried from the `ASTCxt`.
///
/// \returns the memory type. NULL if failed or the external type of the export
/// type is not `WasmEdge_ExternalType_Memory`.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_MemoryTypeContext *
WasmEdge_ExportTypeGetMemoryType(const WasmEdge_ASTModuleContext *ASTCxt,
                                 const WasmEdge_ExportTypeContext *Cxt);

/// Get the external value (which is global type) from an export type.
///
/// The export type context should be the one queried from the AST module
/// context, or this function will cause unexpected error.
/// The global type context links to the global type in the export type context
/// and the AST module context. The caller should __NOT__ call the
/// `WasmEdge_GlobalTypeDelete`.
///
/// \param ASTCxt the WasmEdge_ASTModuleContext.
/// \param Cxt the WasmEdge_ExportTypeContext which queried from the `ASTCxt`.
///
/// \returns the global type. NULL if failed or the external type of the export
/// type is not `WasmEdge_ExternalType_Global`.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_GlobalTypeContext *
WasmEdge_ExportTypeGetGlobalType(const WasmEdge_ASTModuleContext *ASTCxt,
                                 const WasmEdge_ExportTypeContext *Cxt);

/// <<<<<<<< WasmEdge export type functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge AOT compiler functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_CompilerContext.
///
/// The caller owns the object and should call `WasmEdge_CompilerDelete` to free
/// it.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_CompilerContext *
WasmEdge_CompilerCreate(const WasmEdge_ConfigureContext *ConfCxt);

/// Creation of the WasmEdge_CompilerContext.
///
/// The caller owns the object and should call `WasmEdge_CompilerDelete` to free
/// it.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_CompilerCompile(WasmEdge_CompilerContext *Cxt, const char *InPath,
                         const char *OutPath);

/// Deletion of the WasmEdge_CompilerContext.
///
/// After calling this function, the context will be freed and should
/// __NOT__ be used.
///
/// \param Cxt the WasmEdge_CompilerContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_CompilerDelete(WasmEdge_CompilerContext *Cxt);

/// <<<<<<<< WasmEdge AOT compiler functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge loader functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_LoaderContext.
///
/// The caller owns the object and should call `WasmEdge_LoaderDelete` to free
/// it.
///
/// \param ConfCxt the WasmEdge_ConfigureContext as the configuration of Loader.
/// NULL for the default configuration.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_LoaderContext *
WasmEdge_LoaderCreate(const WasmEdge_ConfigureContext *ConfCxt);

/// Load and parse the WASM module from a WASM file into a
/// WasmEdge_ASTModuleContext.
///
/// Load and parse the WASM module from the file path, and return a
/// `WasmEdge_ASTModuleContext` as result. The caller owns the
/// `WasmEdge_ASTModuleContext` object and should call
/// `WasmEdge_ASTModuleDelete` to free it.
///
/// \param Cxt the WasmEdge_LoaderContext.
/// \param [out] Module the output WasmEdge_ASTModuleContext if succeeded.
/// \param Path the NULL-terminated C string of the WASM file path.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_LoaderParseFromFile(WasmEdge_LoaderContext *Cxt,
                             WasmEdge_ASTModuleContext **Module,
                             const char *Path);

/// Load and parse the WASM module from a buffer into WasmEdge_ASTModuleContext.
///
/// Load and parse the WASM module from a buffer, and return a
/// WasmEdge_ASTModuleContext as result. The caller owns the
/// WasmEdge_ASTModuleContext object and should call `WasmEdge_ASTModuleDelete`
/// to free it.
///
/// \param Cxt the WasmEdge_LoaderContext.
/// \param [out] Module the output WasmEdge_ASTModuleContext if succeeded.
/// \param Buf the buffer of WASM binary.
/// \param BufLen the length of the buffer.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_LoaderParseFromBuffer(WasmEdge_LoaderContext *Cxt,
                               WasmEdge_ASTModuleContext **Module,
                               const uint8_t *Buf, const uint32_t BufLen);

/// Deletion of the WasmEdge_LoaderContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_LoaderContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_LoaderDelete(WasmEdge_LoaderContext *Cxt);

/// <<<<<<<< WasmEdge loader functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge validator functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_ValidatorContext.
///
/// The caller owns the object and should call `WasmEdge_ValidatorDelete` to
/// free it.
///
/// \param ConfCxt the WasmEdge_ConfigureContext as the configuration of
/// Validator. NULL for the default configuration.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_ValidatorContext *
WasmEdge_ValidatorCreate(const WasmEdge_ConfigureContext *ConfCxt);

/// Validate the WasmEdge AST Module.
///
/// \param Cxt the WasmEdge_ValidatorContext.
/// \param ModuleCxt the WasmEdge_ASTModuleContext to validate.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_ValidatorValidate(WasmEdge_ValidatorContext *Cxt,
                           const WasmEdge_ASTModuleContext *ModuleCxt);

/// Deletion of the WasmEdge_ValidatorContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_ValidatorContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ValidatorDelete(WasmEdge_ValidatorContext *Cxt);

/// <<<<<<<< WasmEdge validator functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge executor functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_ExecutorContext.
///
/// The caller owns the object and should call `WasmEdge_ExecutorDelete` to free
/// it.
///
/// \param ConfCxt the WasmEdge_ConfigureContext as the configuration of
/// Executor. NULL for the default configuration.
/// \param StatCxt the WasmEdge_StatisticsContext as the statistics object set
/// into Executor. The statistics will refer to this context, and the life cycle
/// should be ensured until the executor context is deleted. NULL for not doing
/// the statistics.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_ExecutorContext *
WasmEdge_ExecutorCreate(const WasmEdge_ConfigureContext *ConfCxt,
                        WasmEdge_StatisticsContext *StatCxt);

/// Instantiate WasmEdge AST Module into a store.
///
/// Instantiate the WasmEdge AST Module as an active anonymous module in store.
/// You can call `WasmEdge_StoreGetActiveModule` to retrieve the active module
/// instance.
///
/// \param Cxt the WasmEdge_ExecutorContext to instantiate the module.
/// \param StoreCxt the WasmEdge_StoreContext to store the instantiated module.
/// \param ASTCxt the WasmEdge AST Module context generated by loader or
/// compiler.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_ExecutorInstantiate(WasmEdge_ExecutorContext *Cxt,
                             WasmEdge_StoreContext *StoreCxt,
                             const WasmEdge_ASTModuleContext *ASTCxt);

/// Register and instantiate WasmEdge import object into a store.
///
/// Instantiate the instances in WasmEdge import object context and register
/// into a store with their exported name and the host module name.
///
/// \param Cxt the WasmEdge_ExecutorContext to instantiate the module.
/// \param StoreCxt the WasmEdge_StoreContext to store the instantiated module.
/// \param ImportCxt the WasmEdge_ImportObjectContext to register.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_ExecutorRegisterImport(WasmEdge_ExecutorContext *Cxt,
                                WasmEdge_StoreContext *StoreCxt,
                                const WasmEdge_ImportObjectContext *ImportCxt);

/// Register and instantiate WasmEdge AST Module into a store.
///
/// Instantiate the instances in WasmEdge AST Module and register into a store
/// with their exported name and module name.
///
/// \param Cxt the WasmEdge_ExecutorContext to instantiate the module.
/// \param StoreCxt the WasmEdge_StoreContext to store the instantiated module.
/// \param ASTCxt the WasmEdge AST Module context generated by loader or
/// compiler.
/// \param ModuleName the module name WasmEdge_String for all exported
/// instances.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result WasmEdge_ExecutorRegisterModule(
    WasmEdge_ExecutorContext *Cxt, WasmEdge_StoreContext *StoreCxt,
    const WasmEdge_ASTModuleContext *ASTCxt, WasmEdge_String ModuleName);

/// Invoke a WASM function by name.
///
/// After instantiating a WASM module, the WASM module is registered as the
/// anonymous module in the store context. Then you can repeatedly call this
/// function to invoke exported WASM functions by their names until the store
/// context is reset or a new WASM module is registered or instantiated. For
/// calling the functions in registered WASM modules with names in store, please
/// use `WasmEdge_ExecutorInvokeRegistered` instead.
///
/// \param Cxt the WasmEdge_ExecutorContext.
/// \param StoreCxt the WasmEdge_StoreContext which the module instantiated in.
/// \param FuncName the function name WasmEdge_String.
/// \param Params the WasmEdge_Value buffer with the parameter values.
/// \param ParamLen the parameter buffer length.
/// \param [out] Returns the WasmEdge_Value buffer to fill the return values.
/// \param ReturnLen the return buffer length.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result WasmEdge_ExecutorInvoke(
    WasmEdge_ExecutorContext *Cxt, WasmEdge_StoreContext *StoreCxt,
    const WasmEdge_String FuncName, const WasmEdge_Value *Params,
    const uint32_t ParamLen, WasmEdge_Value *Returns, const uint32_t ReturnLen);

/// Invoke a WASM function by its module name and function name.
///
/// After registering a WASM module, the WASM module is registered with its name
/// in the store context. Then you can repeatedly call this function to invoke
/// exported WASM functions by their module names and function names until the
/// store context is reset.
///
/// \param Cxt the WasmEdge_ExecutorContext.
/// \param StoreCxt the WasmEdge_StoreContext which the module instantiated in.
/// \param ModuleName the module name WasmEdge_String.
/// \param FuncName the function name WasmEdge_String.
/// \param Params the WasmEdge_Value buffer with the parameter values.
/// \param ParamLen the parameter buffer length.
/// \param [out] Returns the WasmEdge_Value buffer to fill the return values.
/// \param ReturnLen the return buffer length.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result WasmEdge_ExecutorInvokeRegistered(
    WasmEdge_ExecutorContext *Cxt, WasmEdge_StoreContext *StoreCxt,
    const WasmEdge_String ModuleName, const WasmEdge_String FuncName,
    const WasmEdge_Value *Params, const uint32_t ParamLen,
    WasmEdge_Value *Returns, const uint32_t ReturnLen);

/// Deletion of the WasmEdge_ExecutorContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_ExecutorContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ExecutorDelete(WasmEdge_ExecutorContext *Cxt);

/// <<<<<<<< WasmEdge executor functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge store functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_StoreContext.
///
/// The caller owns the object and should call `WasmEdge_StoreDelete` to free
/// it.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_StoreContext *WasmEdge_StoreCreate(void);

/// Get the function instance context by the function name.
///
/// After instantiating a WASM module, the WASM module is registered into the
/// store context as an anonymous module. Then you can call this function to get
/// the exported function instance context of the anonymous module by the
/// function name. If you want to get the exported function of registered named
/// modules in the store context, please call
/// `WasmEdge_StoreFindFunctionRegistered` instead. The result function instance
/// context links to the function instance in the store context and owned by the
/// store context.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param Name the function name WasmEdge_String.
///
/// \returns pointer to the function instance context. NULL if not found.
WASMEDGE_CAPI_EXPORT extern WasmEdge_FunctionInstanceContext *
WasmEdge_StoreFindFunction(WasmEdge_StoreContext *Cxt,
                           const WasmEdge_String Name);

/// Get the function instance context by the function name and the module name.
///
/// After registering a WASM module, you can call this function to get the
/// exported function instance context by the module name and the function name.
/// The result function instance context links to the function instance in the
/// store context and owned by the store context.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param FuncName the function name WasmEdge_String.
///
/// \returns pointer to the function instance context. NULL if not found.
WASMEDGE_CAPI_EXPORT extern WasmEdge_FunctionInstanceContext *
WasmEdge_StoreFindFunctionRegistered(WasmEdge_StoreContext *Cxt,
                                     const WasmEdge_String ModuleName,
                                     const WasmEdge_String FuncName);

/// Get the table instance context by the table name.
///
/// After instantiating a WASM module, the WASM module is registered into the
/// store context as an anonymous module. Then you can call this function to get
/// the exported table instance context of the anonymous module by the table
/// name. If you want to get the exported table of registered named modules in
/// the store context, please call `WasmEdge_StoreFindTableRegistered` instead.
/// The result table instance context links to the table instance in the store
/// context and owned by the store context. The caller should __NOT__ call the
/// `WasmEdge_TableInstanceDelete`.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param Name the table name WasmEdge_String.
///
/// \returns pointer to the table instance context. NULL if not found.
WASMEDGE_CAPI_EXPORT extern WasmEdge_TableInstanceContext *
WasmEdge_StoreFindTable(WasmEdge_StoreContext *Cxt, const WasmEdge_String Name);

/// Get the table instance context by the table name and the module name.
///
/// After registering a WASM module, you can call this function to get the
/// exported table instance context by the module name and the table name.
/// The result table instance context links to the table instance in the store
/// context and owned by the store context. The caller should __NOT__ call the
/// `WasmEdge_TableInstanceDelete`.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param TableName the table name WasmEdge_String.
///
/// \returns pointer to the table instance context. NULL if not found.
WASMEDGE_CAPI_EXPORT extern WasmEdge_TableInstanceContext *
WasmEdge_StoreFindTableRegistered(WasmEdge_StoreContext *Cxt,
                                  const WasmEdge_String ModuleName,
                                  const WasmEdge_String TableName);

/// Get the memory instance context by the memory name.
///
/// After instantiating a WASM module, the WASM module is registered into the
/// store context as an anonymous module. Then you can call this function to get
/// the exported memory instance context of the anonymous module by the memory
/// name. If you want to get the exported memory of registered named modules in
/// the store context, please call `WasmEdge_StoreFindMemoryRegistered` instead.
/// The result memory instance context links to the memory instance in the store
/// context and owned by the store context. The caller should __NOT__ call the
/// `WasmEdge_MemoryInstanceDelete`.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param Name the memory name WasmEdge_String.
///
/// \returns pointer to the memory instance context. NULL if not found.
WASMEDGE_CAPI_EXPORT extern WasmEdge_MemoryInstanceContext *
WasmEdge_StoreFindMemory(WasmEdge_StoreContext *Cxt,
                         const WasmEdge_String Name);

/// Get the memory instance context by the memory name and the module name.
///
/// After registering a WASM module, you can call this function to get the
/// exported memory instance context by the module name and the memory name.
/// The result memory instance context links to the memory instance in the store
/// context and owned by the store context. The caller should __NOT__ call the
/// `WasmEdge_MemoryInstanceDelete`.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param MemoryName the memory name WasmEdge_String.
///
/// \returns pointer to the memory instance context. NULL if not found.
WASMEDGE_CAPI_EXPORT extern WasmEdge_MemoryInstanceContext *
WasmEdge_StoreFindMemoryRegistered(WasmEdge_StoreContext *Cxt,
                                   const WasmEdge_String ModuleName,
                                   const WasmEdge_String MemoryName);

/// Get the global instance context by the instance address.
///
/// After instantiating a WASM module, the WASM module is registered into the
/// store context as an anonymous module. Then you can call this function to get
/// the exported global instance context of the anonymous module by the global
/// name. If you want to get the exported global of registered named modules in
/// the store context, please call `WasmEdge_StoreFindGlobalRegistered` instead.
/// The result global instance context links to the global instance in the store
/// context and owned by the store context. The caller should __NOT__ call the
/// `WasmEdge_GlobalInstanceDelete`.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param Name the global name WasmEdge_String.
///
/// \returns pointer to the global instance context. NULL if not found.
WASMEDGE_CAPI_EXPORT extern WasmEdge_GlobalInstanceContext *
WasmEdge_StoreFindGlobal(WasmEdge_StoreContext *Cxt,
                         const WasmEdge_String Name);

/// Get the global instance context by the global name and the module name.
///
/// After registering a WASM module, you can call this function to get the
/// exported global instance context by the module name and the global name.
/// The result global instance context links to the global instance in the store
/// context and owned by the store context. The caller should __NOT__ call the
/// `WasmEdge_GlobalInstanceDelete`.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param GlobalName the global name WasmEdge_String.
///
/// \returns pointer to the global instance context. NULL if not found.
WASMEDGE_CAPI_EXPORT extern WasmEdge_GlobalInstanceContext *
WasmEdge_StoreFindGlobalRegistered(WasmEdge_StoreContext *Cxt,
                                   const WasmEdge_String ModuleName,
                                   const WasmEdge_String GlobalName);

/// Get the length of exported function list in store.
///
/// If you want to get the function list of the registered named modules in the
/// store context, please call `WasmEdge_StoreListFunctionRegisteredLength`
/// instead.
///
/// \param Cxt the WasmEdge_StoreContext.
///
/// \returns length of the exported function list.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListFunctionLength(const WasmEdge_StoreContext *Cxt);

/// List the exported function names.
///
/// After instantiating a WASM module, the WASM module is registered into the
/// store context as an anonymous module. Then you can call this function to get
/// the exported function list of the anonymous module. If you want to get the
/// function list of the registered named modules in the store context, please
/// call `WasmEdge_StoreListFunctionRegistered` instead.
/// The returned function names filled into the `Names` array are linked to the
/// exported names of functions in the store context, and the caller should
/// __NOT__ call the `WasmEdge_StringDelete`.
/// If the `Names` buffer length is smaller than the result of the exported
/// function list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param [out] Names the output WasmEdge_String buffer of the function names.
/// \param Len the buffer length.
///
/// \returns actual exported function list size.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListFunction(const WasmEdge_StoreContext *Cxt,
                           WasmEdge_String *Names, const uint32_t Len);

/// Get the exported function list length of the registered module in store.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
///
/// \returns the exported function list length of the module.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListFunctionRegisteredLength(const WasmEdge_StoreContext *Cxt,
                                           const WasmEdge_String ModuleName);

/// List the exported function names of the registered module.
///
/// After registering a WASM module, you can call this function to get the
/// exported function list of the registered module by the module name.
/// The returned function names filled into the `Names` array are linked to the
/// exported names of functions in the store context, and the caller should
/// __NOT__ call the `WasmEdge_StringDelete`.
/// If the `Names` buffer length is smaller than the result of the exported
/// function list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param [out] Names the output WasmEdge_String buffer of the function names.
/// \param Len the buffer length.
///
/// \returns actual exported function list size of the module.
WASMEDGE_CAPI_EXPORT extern uint32_t WasmEdge_StoreListFunctionRegistered(
    const WasmEdge_StoreContext *Cxt, const WasmEdge_String ModuleName,
    WasmEdge_String *Names, const uint32_t Len);

/// Get the length of exported table list in store.
///
/// If you want to get the table list of the registered named modules in the
/// store context, please call `WasmEdge_StoreListTableRegisteredLength`
/// instead.
///
/// \param Cxt the WasmEdge_StoreContext.
///
/// \returns length of the exported table list.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListTableLength(const WasmEdge_StoreContext *Cxt);

/// List the exported table names.
///
/// After instantiating a WASM module, the WASM module is registered into the
/// store context as an anonymous module. Then you can call this function to get
/// the exported table list of the anonymous module. If you want to get the
/// table list of the registered named modules in the store context, please
/// call `WasmEdge_StoreListTableRegistered` instead.
/// The returned table names filled into the `Names` array are linked to the
/// exported names of tables in the store context, and the caller should
/// __NOT__ call the `WasmEdge_StringDelete`.
/// If the `Names` buffer length is smaller than the result of the exported
/// table list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param [out] Names the output WasmEdge_String buffer of the table names.
/// \param Len the buffer length.
///
/// \returns actual exported table list size.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListTable(const WasmEdge_StoreContext *Cxt,
                        WasmEdge_String *Names, const uint32_t Len);

/// Get the exported table list length of the registered module in store.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
///
/// \returns the exported table list length of the module.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListTableRegisteredLength(const WasmEdge_StoreContext *Cxt,
                                        const WasmEdge_String ModuleName);

/// List the exported table names of the registered module.
///
/// After registering a WASM module, you can call this function to get the
/// exported table list of the registered module by the module name.
/// The returned table names filled into the `Names` array are linked to the
/// exported names of tables in the store context, and the caller should
/// __NOT__ call the `WasmEdge_StringDelete`.
/// If the `Names` buffer length is smaller than the result of the exported
/// table list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param [out] Names the output WasmEdge_String buffer of the table names.
/// \param Len the buffer length.
///
/// \returns actual exported table list size of the module.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListTableRegistered(const WasmEdge_StoreContext *Cxt,
                                  const WasmEdge_String ModuleName,
                                  WasmEdge_String *Names, const uint32_t Len);

/// Get the length of exported memory list in store.
///
/// If you want to get the memory list of the registered named modules in the
/// store context, please call `WasmEdge_StoreListMemoryRegisteredLength`
/// instead.
///
/// \param Cxt the WasmEdge_StoreContext.
///
/// \returns length of the exported memory list.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListMemoryLength(const WasmEdge_StoreContext *Cxt);

/// List the exported memory names.
///
/// After instantiating a WASM module, the WASM module is registered into the
/// store context as an anonymous module. Then you can call this function to get
/// the exported memory list of the anonymous module. If you want to get the
/// memory list of the registered named modules in the store context, please
/// call `WasmEdge_StoreListMemoryRegistered` instead.
/// The returned memory names filled into the `Names` array are linked to the
/// exported names of memories in the store context, and the caller should
/// __NOT__ call the `WasmEdge_StringDelete`.
/// If the `Names` buffer length is smaller than the result of the exported
/// memory list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param [out] Names the output WasmEdge_String buffer of the memory names.
/// \param Len the buffer length.
///
/// \returns actual exported memory list size.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListMemory(const WasmEdge_StoreContext *Cxt,
                         WasmEdge_String *Names, const uint32_t Len);

/// Get the exported memory list length of the registered module in store.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
///
/// \returns the exported memory list length of the module.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListMemoryRegisteredLength(const WasmEdge_StoreContext *Cxt,
                                         const WasmEdge_String ModuleName);

/// List the exported memory names of the registered module.
///
/// After registering a WASM module, you can call this function to get the
/// exported memory list of the registered module by the module name.
/// The returned memory names filled into the `Names` array are linked to the
/// exported names of memories in the store context, and the caller should
/// __NOT__ call the `WasmEdge_StringDelete`.
/// If the `Names` buffer length is smaller than the result of the exported
/// memory list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param [out] Names the output WasmEdge_String buffer of the memory names.
/// \param Len the buffer length.
///
/// \returns actual exported memory list size of the module.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListMemoryRegistered(const WasmEdge_StoreContext *Cxt,
                                   const WasmEdge_String ModuleName,
                                   WasmEdge_String *Names, const uint32_t Len);

/// Get the length of exported global list in store.
///
/// If you want to get the global list of the registered named modules in the
/// store context, please call `WasmEdge_StoreListGlobalRegisteredLength`
/// instead.
///
/// \param Cxt the WasmEdge_StoreContext.
///
/// \returns length of the exported global list.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListGlobalLength(const WasmEdge_StoreContext *Cxt);

/// List the exported global names.
///
/// After instantiating a WASM module, the WASM module is registered into the
/// store context as an anonymous module. Then you can call this function to get
/// the exported global list of the anonymous module. If you want to get the
/// global list of the registered named modules in the store context, please
/// call `WasmEdge_StoreListGlobalRegistered` instead.
/// The returned global names filled into the `Names` array are linked to the
/// exported names of globals in the store context, and the caller should
/// __NOT__ call the `WasmEdge_StringDelete`.
/// If the `Names` buffer length is smaller than the result of the exported
/// global list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param [out] Names the output WasmEdge_String buffer of the global names.
/// \param Len the buffer length.
///
/// \returns actual exported global list size.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListGlobal(const WasmEdge_StoreContext *Cxt,
                         WasmEdge_String *Names, const uint32_t Len);

/// Get the exported global list length of the registered module in store.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
///
/// \returns the exported global list length of the module.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListGlobalRegisteredLength(const WasmEdge_StoreContext *Cxt,
                                         const WasmEdge_String ModuleName);

/// List the exported global names of the registered module.
///
/// After registering a WASM module, you can call this function to get the
/// exported global list of the registered module by the module name.
/// The returned global names filled into the `Names` array are linked to the
/// exported names of globals in the store context, and the caller should
/// __NOT__ call the `WasmEdge_StringDelete`.
/// If the `Names` buffer length is smaller than the result of the exported
/// global list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param [out] Names the output WasmEdge_String buffer of the global names.
/// \param Len the buffer length.
///
/// \returns actual exported global list size of the module.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListGlobalRegistered(const WasmEdge_StoreContext *Cxt,
                                   const WasmEdge_String ModuleName,
                                   WasmEdge_String *Names, const uint32_t Len);

/// Get the length of registered module list in store.
///
/// \param Cxt the WasmEdge_StoreContext.
///
/// \returns length of registered named module list.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListModuleLength(const WasmEdge_StoreContext *Cxt);

/// List the registered module names.
///
/// This function will list all registered module names.
/// The returned module names filled into the `Names` array are linked to the
/// registered module names in the store context, and the caller should __NOT__
/// call the `WasmEdge_StringDelete`.
/// If the `Names` buffer length is smaller than the result of the registered
/// named module list size, the overflowed return values will be discarded.
///
/// \param Cxt the WasmEdge_StoreContext.
/// \param [out] Names the output names WasmEdge_String buffer of named modules.
/// \param Len the buffer length.
///
/// \returns actual registered named module list size.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_StoreListModule(const WasmEdge_StoreContext *Cxt,
                         WasmEdge_String *Names, const uint32_t Len);

/// Deletion of the WasmEdge_StoreContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_StoreContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_StoreDelete(WasmEdge_StoreContext *Cxt);

/// <<<<<<<< WasmEdge store functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge function instance functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

typedef WasmEdge_Result (*WasmEdge_HostFunc_t)(
    void *Data, WasmEdge_MemoryInstanceContext *MemCxt,
    const WasmEdge_Value *Params, WasmEdge_Value *Returns);
/// Creation of the WasmEdge_FunctionInstanceContext for host functions.
///
/// The caller owns the object and should call `WasmEdge_FunctionInstanceDelete`
/// to free it if the returned object is not added into a
/// `WasmEdge_ImportObjectContext`. The following is an example to create a host
/// function context.
/// ```c
/// WasmEdge_Result FuncAdd(void *Data, WasmEdge_MemoryInstanceContext *MemCxt,
///                         const WasmEdge_Value *In, WasmEdge_Value *Out) {
///   /// Function to return A + B.
///   int32_t A = WasmEdge_ValueGetI32(In[0]);
///   int32_t B = WasmEdge_ValueGetI32(In[1]);
///   Out[0] = WasmEdge_ValueGenI32(A + B);
///   /// Return execution status
///   return WasmEdge_Result_Success;
/// }
///
///
/// enum WasmEdge_ValType Params[2] = {WasmEdge_ValType_I32,
///                                    WasmEdge_ValType_I32};
/// enum WasmEdge_ValType Returns[1] = {WasmEdge_ValType_I32};
/// WasmEdge_FunctionTypeContext *FuncType =
///     WasmEdge_FunctionTypeCreate(Params, 2, Returns, 1);
/// WasmEdge_FunctionInstanceContext *HostFunc =
///     WasmEdge_FunctionInstanceCreate(FuncType, FuncAdd, NULL, 0);
/// WasmEdge_FunctionTypeDelete(FuncType);
/// ...
/// ```
///
/// \param Type the function type context to describe the host function
/// signature.
/// \param HostFunc the host function pointer. The host function signature must
/// be as following:
/// ```c
/// typedef WasmEdge_Result (*WasmEdge_HostFunc_t)(
///     void *Data,
///     WasmEdge_MemoryInstanceContext *MemCxt,
///     const WasmEdge_Value *Params,
///     WasmEdge_Value *Returns);
/// ```
/// The `Params` is the input parameters array with length guaranteed to be the
/// same as the parameter types in the `Type`. The `Returns` is the output
/// results array with length guaranteed to be the same as the result types in
/// the `Type`. The return value is `WasmEdge_Result` for the execution status.
/// \param Data the additional object, such as the pointer to a data structure,
/// to set to this host function context. The caller should guarantee the life
/// cycle of the object. NULL if the additional data object is not needed.
/// \param Cost the function cost in statistics. Pass 0 if the calculation is
/// not needed.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_FunctionInstanceContext *
WasmEdge_FunctionInstanceCreate(const WasmEdge_FunctionTypeContext *Type,
                                WasmEdge_HostFunc_t HostFunc, void *Data,
                                const uint64_t Cost);

typedef WasmEdge_Result (*WasmEdge_WrapFunc_t)(
    void *This, void *Data, WasmEdge_MemoryInstanceContext *MemCxt,
    const WasmEdge_Value *Params, const uint32_t ParamLen,
    WasmEdge_Value *Returns, const uint32_t ReturnLen);
/// Creation of the WasmEdge_FunctionInstanceContext for host functions.
///
/// This function is for the languages which cannot pass the function pointer of
/// the host function into this shared library directly. The caller owns the
/// object and should call `WasmEdge_FunctionInstanceDelete` to free it if the
/// returned object is not added into a `WasmEdge_ImportObjectContext`. The
/// following is an example to create a host function context for other
/// languages.
/// ```c
/// /// `RealFunc` is the pointer to the function in other languages.
///
/// WasmEdge_Result FuncAddWrap(void *This, void *Data,
///                             WasmEdge_MemoryInstanceContext *MemCxt,
///                             const WasmEdge_Value *In, const uint32_t InLen,
///                             WasmEdge_Value *Out, const uint32_t OutLen) {
///   /// Wrapper function of host function to return A + B.
///
///   /// `This` is the same as `RealFunc`.
///   int32_t A = WasmEdge_ValueGetI32(In[0]);
///   int32_t B = WasmEdge_ValueGetI32(In[1]);
///
///   /// Call the function of `This` in the host language ...
///   int32_t Result = ...;
///
///   Out[0] = Result;
///   /// Return the execution status
///   return WasmEdge_Result_Success;
/// }
///
/// enum WasmEdge_ValType Params[2] = {WasmEdge_ValType_I32,
///                                    WasmEdge_ValType_I32};
/// enum WasmEdge_ValType Returns[1] = {WasmEdge_ValType_I32};
/// WasmEdge_FunctionTypeContext *FuncType =
///     WasmEdge_FunctionTypeCreate(Params, 2, Returns, 1);
/// WasmEdge_FunctionInstanceContext *HostFunc =
///     WasmEdge_FunctionInstanceCreateBinding(
///         FuncType, FuncAddWrap, RealFunc, NULL, 0);
/// WasmEdge_FunctionTypeDelete(FuncType);
/// ...
/// ```
///
/// \param Type the function type context to describe the host function
/// signature.
/// \param WrapFunc the wrapper function pointer. The wrapper function signature
/// must be as following:
/// ```c
/// typedef WasmEdge_Result (*WasmEdge_WrapFunc_t)(
///     void *This,
///     void *Data,
///     WasmEdge_MemoryInstanceContext *MemCxt,
///     const WasmEdge_Value *Params,
///     const uint32_t ParamLen,
///     WasmEdge_Value *Returns,
///     const uint32_t ReturnLen);
/// ```
/// The `This` is the pointer the same as the `Binding` parameter of this
/// function. The `Params` is the input parameters array with length guaranteed
/// to be the same as the parameter types in the `Type`, and the `ParamLen` is
/// the length of the array. The `Returns` is the output results array with
/// length guaranteed to be the same as the result types in the `Type`, and the
/// `ReturnLen` is the length of the array. The return value is
/// `WasmEdge_Result` for the execution status.
/// \param Binding the `this` pointer of the host function target or the
/// function indexing maintained by the caller which can specify the host
/// function. When invoking the host function, this pointer will be the first
/// argument of the wrapper function.
/// \param Data the additional object, such as the pointer to a data structure,
/// to set to this host function context. The caller should guarantee the life
/// cycle of the object. NULL if the additional data object is not needed.
/// \param Cost the function cost in statistics. Pass 0 if the calculation is
/// not needed.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_FunctionInstanceContext *
WasmEdge_FunctionInstanceCreateBinding(const WasmEdge_FunctionTypeContext *Type,
                                       WasmEdge_WrapFunc_t WrapFunc,
                                       void *Binding, void *Data,
                                       const uint64_t Cost);

/// Get the function type context of the function instance.
///
/// The function type context links to the function type in the function
/// instance context and owned by the context. The caller should __NOT__ call
/// the `WasmEdge_FunctionTypeDelete`.
///
/// \param Cxt the WasmEdge_FunctionInstanceContext.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_FunctionTypeContext *
WasmEdge_FunctionInstanceGetFunctionType(
    const WasmEdge_FunctionInstanceContext *Cxt);

/// Deletion of the WasmEdge_FunctionInstanceContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_FunctionInstanceContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_FunctionInstanceDelete(WasmEdge_FunctionInstanceContext *Cxt);

/// <<<<<<<< WasmEdge function instance functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge table instance functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_TableInstanceContext.
///
/// The caller owns the object and should call `WasmEdge_TableInstanceDelete` to
/// free it.
///
/// \param TabType the table type context to initialize the table instance
/// context.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_TableInstanceContext *
WasmEdge_TableInstanceCreate(const WasmEdge_TableTypeContext *TabType);

/// Get the table type context from a table instance.
///
/// The table type context links to the table type in the table instance context
/// and owned by the context. The caller should __NOT__ call the
/// `WasmEdge_TableTypeDelete`.
///
/// \param Cxt the WasmEdge_TableInstanceContext.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_TableTypeContext *
WasmEdge_TableInstanceGetTableType(const WasmEdge_TableInstanceContext *Cxt);

/// Get the reference value in a table instance.
///
/// \param Cxt the WasmEdge_TableInstanceContext.
/// \param [out] Data the result reference value.
/// \param Offset the reference value offset (index) in the table instance.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_TableInstanceGetData(const WasmEdge_TableInstanceContext *Cxt,
                              WasmEdge_Value *Data, const uint32_t Offset);

/// Set the reference value into a table instance.
///
/// \param Cxt the WasmEdge_TableInstanceContext.
/// \param Data the reference value to set into the table instance.
/// \param Offset the reference value offset (index) in the table instance.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_TableInstanceSetData(WasmEdge_TableInstanceContext *Cxt,
                              WasmEdge_Value Data, const uint32_t Offset);

/// Get the size of a table instance.
///
/// \param Cxt the WasmEdge_TableInstanceContext.
///
/// \returns the size of the table instance.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_TableInstanceGetSize(const WasmEdge_TableInstanceContext *Cxt);

/// Grow a table instance with a size.
///
/// \param Cxt the WasmEdge_TableInstanceContext.
/// \param Size the count of reference values to grow in the table instance.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_TableInstanceGrow(WasmEdge_TableInstanceContext *Cxt,
                           const uint32_t Size);

/// Deletion of the WasmEdge_TableInstanceContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_TableInstanceContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_TableInstanceDelete(WasmEdge_TableInstanceContext *Cxt);

/// <<<<<<<< WasmEdge table instance functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge memory instance functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_MemoryInstanceContext.
///
/// The caller owns the object and should call `WasmEdge_MemoryInstanceDelete`
/// to free it.
///
/// \param MemType the memory type context to initialize the memory instance
/// context.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_MemoryInstanceContext *
WasmEdge_MemoryInstanceCreate(const WasmEdge_MemoryTypeContext *MemType);

/// Get the memory type context from a memory instance.
///
/// The memory type context links to the memory type in the memory instance
/// context and owned by the context. The caller should __NOT__ call the
/// `WasmEdge_MemoryTypeDelete`.
///
/// \param Cxt the WasmEdge_MemoryInstanceContext.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_MemoryTypeContext *
WasmEdge_MemoryInstanceGetMemoryType(const WasmEdge_MemoryInstanceContext *Cxt);

/// Copy the data to the output buffer from a memory instance.
///
/// \param Cxt the WasmEdge_MemoryInstanceContext.
/// \param [out] Data the result data buffer of copying destination.
/// \param Offset the data start offset in the memory instance.
/// \param Length the requested data length. If the `Offset + Length` is larger
/// than the data size in the memory instance, this function will failed.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_MemoryInstanceGetData(const WasmEdge_MemoryInstanceContext *Cxt,
                               uint8_t *Data, const uint32_t Offset,
                               const uint32_t Length);

/// Copy the data into a memory instance from the input buffer.
///
/// \param Cxt the WasmEdge_MemoryInstanceContext.
/// \param Data the data buffer to copy.
/// \param Offset the data start offset in the memory instance.
/// \param Length the data buffer length. If the `Offset + Length` is larger
/// than the data size in the memory instance, this function will failed.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_MemoryInstanceSetData(WasmEdge_MemoryInstanceContext *Cxt,
                               const uint8_t *Data, const uint32_t Offset,
                               const uint32_t Length);

/// Get the data pointer in a memory instance.
///
/// \param Cxt the WasmEdge_MemoryInstanceContext.
/// \param Offset the data start offset in the memory instance.
/// \param Length the requested data length. If the `Offset + Length` is larger
/// than the data size in the memory instance, this function will return NULL.
///
/// \returns the pointer to data with the start offset. NULL if failed.
WASMEDGE_CAPI_EXPORT extern uint8_t *
WasmEdge_MemoryInstanceGetPointer(WasmEdge_MemoryInstanceContext *Cxt,
                                  const uint32_t Offset, const uint32_t Length);

/// Get the const data pointer in a const memory instance.
///
/// \param Cxt the WasmEdge_MemoryInstanceContext.
/// \param Offset the data start offset in the memory instance.
/// \param Length the requested data length. If the `Offset + Length` is larger
/// than the data size in the memory instance, this function will return NULL.
///
/// \returns the pointer to data with the start offset. NULL if failed.
WASMEDGE_CAPI_EXPORT extern const uint8_t *
WasmEdge_MemoryInstanceGetPointerConst(
    const WasmEdge_MemoryInstanceContext *Cxt, const uint32_t Offset,
    const uint32_t Length);

/// Get the current page size (64 KiB of each page) of a memory instance.
///
/// \param Cxt the WasmEdge_MemoryInstanceContext.
///
/// \returns the page size of the memory instance.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_MemoryInstanceGetPageSize(const WasmEdge_MemoryInstanceContext *Cxt);

/// Grow a memory instance with a page size.
///
/// \param Cxt the WasmEdge_MemoryInstanceContext.
/// \param Page the page count to grow in the memory instance.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_MemoryInstanceGrowPage(WasmEdge_MemoryInstanceContext *Cxt,
                                const uint32_t Page);

/// Deletion of the WasmEdge_MemoryInstanceContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_MemoryInstanceContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_MemoryInstanceDelete(WasmEdge_MemoryInstanceContext *Cxt);

/// <<<<<<<< WasmEdge memory instance functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// >>>>>>>> WasmEdge global instance functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_GlobalInstanceContext.
///
/// The caller owns the object and should call `WasmEdge_GlobalInstanceDelete`
/// to free it.
///
/// \param GlobType the global type context to initialize the global instance
/// context.
/// \param Value the initial value with its value type of the global instance.
/// This function will fail if the value type of `GlobType` and `Value` are not
/// the same.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_GlobalInstanceContext *
WasmEdge_GlobalInstanceCreate(const WasmEdge_GlobalTypeContext *GlobType,
                              const WasmEdge_Value Value);

/// Get the global type context from a global instance.
///
/// The global type context links to the global type in the global instance
/// context and owned by the context. The caller should __NOT__ call the
/// `WasmEdge_GlobalTypeDelete`.
///
/// \param Cxt the WasmEdge_GlobalInstanceContext.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_GlobalTypeContext *
WasmEdge_GlobalInstanceGetGlobalType(const WasmEdge_GlobalInstanceContext *Cxt);

/// Get the value from a global instance.
///
/// \param Cxt the WasmEdge_GlobalInstanceContext.
///
/// \returns the current value of the global instance.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Value
WasmEdge_GlobalInstanceGetValue(const WasmEdge_GlobalInstanceContext *Cxt);

/// Set the value from a global instance.
///
/// This function will do nothing if the global context is set as the `Const`
/// mutation or the value type not matched.
///
/// \param Cxt the WasmEdge_GlobalInstanceContext.
/// \param Value the value to set into the global context.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_GlobalInstanceSetValue(WasmEdge_GlobalInstanceContext *Cxt,
                                const WasmEdge_Value Value);

/// Deletion of the WasmEdge_GlobalInstanceContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_GlobalInstanceContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_GlobalInstanceDelete(WasmEdge_GlobalInstanceContext *Cxt);

/// <<<<<<<< WasmEdge global instance functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// <<<<<<<< WasmEdge import object functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

/// Creation of the WasmEdge_ImportObjectContext.
///
/// The caller owns the object and should call `WasmEdge_ImportObjectDelete` to
/// free it.
///
/// \param ModuleName the module name WasmEdge_String of this host module to
/// import.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_ImportObjectContext *
WasmEdge_ImportObjectCreate(const WasmEdge_String ModuleName);

/// Creation of the WasmEdge_ImportObjectContext for the WASI specification.
///
/// This function will create a WASI host module that contains the WASI host
/// functions and initialize it. The caller owns the object and should call
/// `WasmEdge_ImportObjectDelete` to free it.
///
/// \param Args the command line arguments. The first argument suggests being
/// the program name. NULL if the length is 0.
/// \param ArgLen the length of the command line arguments.
/// \param Envs the environment variables in the format `ENV=VALUE`. NULL if the
/// length is 0.
/// \param EnvLen the length of the environment variables.
/// \param Preopens the directory paths to preopen. String format in
/// `PATH1:PATH2` means the path mapping, or the same path will be mapped. NULL
/// if the length is 0.
/// \param PreopenLen the length of the directory paths to preopen.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_ImportObjectContext *
WasmEdge_ImportObjectCreateWASI(const char *const *Args, const uint32_t ArgLen,
                                const char *const *Envs, const uint32_t EnvLen,
                                const char *const *Preopens,
                                const uint32_t PreopenLen);

/// Initialize the WasmEdge_ImportObjectContext for the WASI specification.
///
/// This function will initialize the WASI host module with the parameters.
///
/// \param Cxt the WasmEdge_ImportObjectContext of WASI import object.
/// \param Args the command line arguments. The first argument suggests being
/// the program name. NULL if the length is 0.
/// \param ArgLen the length of the command line arguments.
/// \param Envs the environment variables in the format `ENV=VALUE`. NULL if the
/// length is 0.
/// \param EnvLen the length of the environment variables.
/// \param Preopens the directory paths to preopen. String format in
/// `PATH1:PATH2` means the path mapping, or the same path will be mapped. NULL
/// if the length is 0.
/// \param PreopenLen the length of the directory paths to preopen.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_ImportObjectInitWASI(
    WasmEdge_ImportObjectContext *Cxt, const char *const *Args,
    const uint32_t ArgLen, const char *const *Envs, const uint32_t EnvLen,
    const char *const *Preopens, const uint32_t PreopenLen);

/// Get the WASI exit code.
///
/// This function will return the exit code after running the "_start" function
/// of a `wasm32-wasi` program.
///
/// \param Cxt the WasmEdge_ImportObjectContext of WASI import object.
///
/// \returns the exit code after executing the "_start" function. Return
/// `EXIT_FAILURE` if the `Cxt` is NULL or not a WASI host module.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_ImportObjectWASIGetExitCode(WasmEdge_ImportObjectContext *Cxt);

/// Creation of the WasmEdge_ImportObjectContext for the wasmedge_process
/// specification.
///
/// This function will create a wasmedge_process host module that contains the
/// wasmedge_process host functions and initialize it. The caller owns the
/// object and should call `WasmEdge_ImportObjectDelete` to free it.
///
/// \param AllowedCmds the allowed commands white list. NULL if the length is 0.
/// \param CmdsLen the length of the allowed commands white list.
/// \param AllowAll the boolean value to allow all commands. `false` is
/// suggested. If this value is `true`, the allowed commands white list will not
/// be recorded and all commands can be executed by wasmedge_process.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_ImportObjectContext *
WasmEdge_ImportObjectCreateWasmEdgeProcess(const char *const *AllowedCmds,
                                           const uint32_t CmdsLen,
                                           const bool AllowAll);

/// Initialize the WasmEdge_ImportObjectContext for the wasmedge_process
/// specification.
///
/// This function will initialize the wasmedge_process host module with the
/// parameters.
///
/// \param Cxt the WasmEdge_ImportObjectContext of wasmedge_process import
/// object.
/// \param AllowedCmds the allowed commands white list. NULL if the
/// length is 0.
/// \param CmdsLen the length of the allowed commands white list.
/// \param AllowAll the boolean value to allow all commands. `false` is
/// suggested. If this value is `true`, the allowed commands white list will not
/// be recorded and all commands can be executed by wasmedge_process.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_ImportObjectInitWasmEdgeProcess(
    WasmEdge_ImportObjectContext *Cxt, const char *const *AllowedCmds,
    const uint32_t CmdsLen, const bool AllowAll);

/// Add a function instance context into a WasmEdge_ImportObjectContext.
///
/// Move the function instance context into the import object. The caller should
/// __NOT__ access or delete the function instance context after calling this
/// function.
///
/// \param Cxt the WasmEdge_ImportObjectContext to add the host function.
/// \param Name the host function name WasmEdge_String.
/// \param FuncCxt the WasmEdge_FunctionInstanceContext to add.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ImportObjectAddFunction(WasmEdge_ImportObjectContext *Cxt,
                                 const WasmEdge_String Name,
                                 WasmEdge_FunctionInstanceContext *FuncCxt);

/// Add a table instance context into a WasmEdge_ImportObjectContext.
///
/// Move the table instance context into the import object. The caller should
/// __NOT__ access or delete the table instance context after calling this
/// function.
///
/// \param Cxt the WasmEdge_ImportObjectContext to add the table instance.
/// \param Name the export table name WasmEdge_String.
/// \param TableCxt the WasmEdge_TableInstanceContext to add.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ImportObjectAddTable(WasmEdge_ImportObjectContext *Cxt,
                              const WasmEdge_String Name,
                              WasmEdge_TableInstanceContext *TableCxt);

/// Add a memory instance context into a WasmEdge_ImportObjectContext.
///
/// Move the memory instance context into the import object. The caller should
/// __NOT__ access or delete the memory instance context after calling this
/// function.
///
/// \param Cxt the WasmEdge_ImportObjectContext to add the memory instance.
/// \param Name the export memory name WasmEdge_String.
/// \param MemoryCxt the WasmEdge_MemoryInstanceContext to add.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ImportObjectAddMemory(WasmEdge_ImportObjectContext *Cxt,
                               const WasmEdge_String Name,
                               WasmEdge_MemoryInstanceContext *MemoryCxt);

/// Add a global instance context into a WasmEdge_ImportObjectContext.
///
/// Move the global instance context into the import object. The caller should
/// __NOT__ access or delete the global instance context after calling this
/// function.
///
/// \param Cxt the WasmEdge_ImportObjectContext to add the global instance.
/// \param Name the export global name WasmEdge_String.
/// \param GlobalCxt the WasmEdge_GlobalInstanceContext to add.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ImportObjectAddGlobal(WasmEdge_ImportObjectContext *Cxt,
                               const WasmEdge_String Name,
                               WasmEdge_GlobalInstanceContext *GlobalCxt);

/// Deletion of the WasmEdge_ImportObjectContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_ImportObjectContext to delete.
WASMEDGE_CAPI_EXPORT extern void
WasmEdge_ImportObjectDelete(WasmEdge_ImportObjectContext *Cxt);

/// >>>>>>>> WasmEdge import object functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// >>>>>>>> WasmEdge VM functions >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

/// Creation of the WasmEdge_VMContext.
///
/// The caller owns the object and should call `WasmEdge_VMDelete` to free it.
///
/// \param ConfCxt the WasmEdge_ConfigureContext as the configuration of VM.
/// NULL for the default configuration.
/// \param StoreCxt the WasmEdge_StoreContext as the external WASM store of VM.
/// The instantiation and execution will refer to this store context, and the
/// life cycle should be ensured until the VM context is deleted. NULL for the
/// default store owned by `WasmEdge_VMContext`.
///
/// \returns pointer to context, NULL if failed.
WASMEDGE_CAPI_EXPORT extern WasmEdge_VMContext *
WasmEdge_VMCreate(const WasmEdge_ConfigureContext *ConfCxt,
                  WasmEdge_StoreContext *StoreCxt);

/// Register and instantiate WASM into the store in VM from a WASM file.
///
/// Load a WASM file from the path, and register all exported instances and
/// instantiate them into the store in VM with their exported name and module
/// name.
///
/// \param Cxt the WasmEdge_VMContext which contains the store.
/// \param ModuleName the WasmEdge_String of module name for all exported
/// instances.
/// \param Path the NULL-terminated C string of the WASM file path.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_VMRegisterModuleFromFile(WasmEdge_VMContext *Cxt,
                                  const WasmEdge_String ModuleName,
                                  const char *Path);

/// Register and instantiate WASM into the store in VM from a buffer.
///
/// Load a WASM module from a buffer, and register all exported instances and
/// instantiate them into the store in VM with their exported name and module
/// name.
///
/// \param Cxt the WasmEdge_VMContext which contains the store.
/// \param ModuleName the WasmEdge_String of module name for all exported
/// instances.
/// \param Buf the buffer of WASM binary.
/// \param BufLen the length of the buffer.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_VMRegisterModuleFromBuffer(WasmEdge_VMContext *Cxt,
                                    const WasmEdge_String ModuleName,
                                    const uint8_t *Buf, const uint32_t BufLen);

/// Register and instantiate WasmEdge import object into the store in VM.
///
/// Instantiate the instances in WasmEdge import object context and register
/// them into the store in VM with their exported name and the host module name.
///
/// \param Cxt the WasmEdge_VMContext which contains the store.
/// \param ImportCxt the WasmEdge_ImportObjectContext to register.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result WasmEdge_VMRegisterModuleFromImport(
    WasmEdge_VMContext *Cxt, const WasmEdge_ImportObjectContext *ImportCxt);

/// Register and instantiate WASM into the store in VM from a WasmEdge AST
/// Module.
///
/// Load from the WasmEdge AST Module, and register all exported instances and
/// instantiate them into the store in VM with their exported name and module
/// name.
///
/// \param Cxt the WasmEdge_VMContext which contains the store.
/// \param ModuleName the WasmEdge_String of module name for all exported
/// instances.
/// \param ASTCxt the WasmEdge AST Module context generated by loader or
/// compiler.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_VMRegisterModuleFromASTModule(WasmEdge_VMContext *Cxt,
                                       const WasmEdge_String ModuleName,
                                       const WasmEdge_ASTModuleContext *ASTCxt);

/// Instantiate the WASM module from a WASM file and invoke a function by name.
///
/// This is the function to invoke a WASM function rapidly.
/// Load and instantiate the WASM module from the file path, and then invoke a
/// function by name and parameters. If the `Returns` buffer length is smaller
/// than the arity of the function, the overflowed return values will be
/// discarded.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param Path the NULL-terminated C string of the WASM file path.
/// \param FuncName the function name WasmEdge_String.
/// \param Params the WasmEdge_Value buffer with the parameter values.
/// \param ParamLen the parameter buffer length.
/// \param [out] Returns the WasmEdge_Value buffer to fill the return values.
/// \param ReturnLen the return buffer length.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result WasmEdge_VMRunWasmFromFile(
    WasmEdge_VMContext *Cxt, const char *Path, const WasmEdge_String FuncName,
    const WasmEdge_Value *Params, const uint32_t ParamLen,
    WasmEdge_Value *Returns, const uint32_t ReturnLen);

/// Instantiate the WASM module from a buffer and invoke a function by name.
///
/// This is the function to invoke a WASM function rapidly.
/// Load and instantiate the WASM module from a buffer, and then invoke a
/// function by name and parameters. If the `Returns` buffer length is smaller
/// than the arity of the function, the overflowed return values will be
/// discarded.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param Buf the buffer of WASM binary.
/// \param BufLen the length of the buffer.
/// \param FuncName the function name WasmEdge_String.
/// \param Params the WasmEdge_Value buffer with the parameter values.
/// \param ParamLen the parameter buffer length.
/// \param [out] Returns the WasmEdge_Value buffer to fill the return values.
/// \param ReturnLen the return buffer length.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result WasmEdge_VMRunWasmFromBuffer(
    WasmEdge_VMContext *Cxt, const uint8_t *Buf, const uint32_t BufLen,
    const WasmEdge_String FuncName, const WasmEdge_Value *Params,
    const uint32_t ParamLen, WasmEdge_Value *Returns, const uint32_t ReturnLen);

/// Instantiate the WASM module from a WasmEdge AST Module and invoke a function
/// by name.
///
/// This is the function to invoke a WASM function rapidly.
/// Load and instantiate the WASM module from the WasmEdge AST Module, and then
/// invoke the function by name and parameters. If the `Returns` buffer length
/// is smaller than the arity of the function, the overflowed return values will
/// be discarded.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param ASTCxt the WasmEdge AST Module context generated by loader or
/// compiler.
/// \param FuncName the function name WasmEdge_String.
/// \param Params the WasmEdge_Value buffer with the parameter values.
/// \param ParamLen the parameter buffer length.
/// \param [out] Returns the WasmEdge_Value buffer to fill the return values.
/// \param ReturnLen the return buffer length.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result WasmEdge_VMRunWasmFromASTModule(
    WasmEdge_VMContext *Cxt, const WasmEdge_ASTModuleContext *ASTCxt,
    const WasmEdge_String FuncName, const WasmEdge_Value *Params,
    const uint32_t ParamLen, WasmEdge_Value *Returns, const uint32_t ReturnLen);

/// Load the WASM module from a WASM file.
///
/// This is the first step to invoke a WASM function step by step.
/// Load and parse the WASM module from the file path. You can then call
/// `WasmEdge_VMValidate` for the next step.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param Path the NULL-terminated C string of the WASM file path.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_VMLoadWasmFromFile(WasmEdge_VMContext *Cxt, const char *Path);

/// Load the WASM module from a buffer.
///
/// This is the first step to invoke a WASM function step by step.
/// Load and parse the WASM module from a buffer. You can then call
/// `WasmEdge_VMValidate` for the next step.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param Buf the buffer of WASM binary.
/// \param BufLen the length of the buffer.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_VMLoadWasmFromBuffer(WasmEdge_VMContext *Cxt, const uint8_t *Buf,
                              const uint32_t BufLen);

/// Load the WASM module from loaded WasmEdge AST Module.
///
/// This is the first step to invoke a WASM function step by step.
/// Copy the loaded WasmEdge AST Module context into VM. The VM context has no
/// dependency on the input AST Module context. You can then call
/// `WasmEdge_VMValidate` for the next step.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param ASTCxt the WasmEdge AST Module context generated by loader or
/// compiler.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_VMLoadWasmFromASTModule(WasmEdge_VMContext *Cxt,
                                 const WasmEdge_ASTModuleContext *ASTCxt);

/// Validate the WASM module loaded into the VM context.
///
/// This is the second step to invoke a WASM function step by step.
/// After loading a WASM module into VM context, You can call this function to
/// validate it. And you can then call `WasmEdge_VMInstantiate` for the next
/// step. Note that only validated WASM modules can be instantiated in the VM
/// context.
///
/// \param Cxt the WasmEdge_VMContext.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_VMValidate(WasmEdge_VMContext *Cxt);

/// Instantiate the validated WASM module in the VM context.
///
/// This is the third step to invoke a WASM function step by step.
/// After validating a WASM module in the VM context, You can call this function
/// to instantiate it. And you can then call `WasmEdge_VMExecute` for invoking
/// the exported function in this WASM module.
///
/// \param Cxt the WasmEdge_VMContext.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_VMInstantiate(WasmEdge_VMContext *Cxt);

/// Invoke a WASM function by name.
///
/// This is the final step to invoke a WASM function step by step.
/// After instantiating a WASM module in the VM context, the WASM module is
/// registered into the store in the VM context as an anonymous module. Then you
/// can repeatedly call this function to invoke the exported WASM functions by
/// their names until the VM context is reset or a new WASM module is registered
/// or loaded. For calling the functions in registered WASM modules with module
/// names, please use `WasmEdge_VMExecuteRegistered` instead. If the `Returns`
/// buffer length is smaller than the arity of the function, the overflowed
/// return values will be discarded.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param FuncName the function name WasmEdge_String.
/// \param Params the WasmEdge_Value buffer with the parameter values.
/// \param ParamLen the parameter buffer length.
/// \param [out] Returns the WasmEdge_Value buffer to fill the return values.
/// \param ReturnLen the return buffer length.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result
WasmEdge_VMExecute(WasmEdge_VMContext *Cxt, const WasmEdge_String FuncName,
                   const WasmEdge_Value *Params, const uint32_t ParamLen,
                   WasmEdge_Value *Returns, const uint32_t ReturnLen);

/// Invoke a WASM function by its module name and function name.
///
/// After registering a WASM module in the VM context, you can repeatedly call
/// this function to invoke exported WASM functions by their module names and
/// function names until the VM context is reset. If the `Returns` buffer length
/// is smaller than the arity of the function, the overflowed return values will
/// be discarded.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param FuncName the function name WasmEdge_String.
/// \param Params the WasmEdge_Value buffer with the parameter values.
/// \param ParamLen the parameter buffer length.
/// \param [out] Returns the WasmEdge_Value buffer to fill the return values.
/// \param ReturnLen the return buffer length.
///
/// \returns WasmEdge_Result. Call `WasmEdge_ResultGetMessage` for the error
/// message.
WASMEDGE_CAPI_EXPORT extern WasmEdge_Result WasmEdge_VMExecuteRegistered(
    WasmEdge_VMContext *Cxt, const WasmEdge_String ModuleName,
    const WasmEdge_String FuncName, const WasmEdge_Value *Params,
    const uint32_t ParamLen, WasmEdge_Value *Returns, const uint32_t ReturnLen);

/// Get the function type by function name.
///
/// After instantiating a WASM module in the VM context, the WASM module is
/// registered into the store in the VM context as an anonymous module. Then you
/// can call this function to get the function type by the exported function
/// name until the VM context is reset or a new WASM module is registered or
/// loaded. For getting the function type of functions in registered WASM
/// modules with module names, please use `WasmEdge_VMGetFunctionTypeRegistered`
/// instead.
/// The returned function type context are linked to the context owned by the VM
/// context, and the caller should __NOT__ call the
/// `WasmEdge_FunctionTypeDelete` to delete it.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param FuncName the function name WasmEdge_String.
///
/// \returns the function type. NULL if the function not found.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_FunctionTypeContext *
WasmEdge_VMGetFunctionType(WasmEdge_VMContext *Cxt,
                           const WasmEdge_String FuncName);

/// Get the function type by function name.
///
/// After registering a WASM module in the VM context, you can call this
/// function to get the function type by the functions' exported module names
/// and function names until the VM context is reset.
/// The returned function type context are linked to the context owned by the VM
/// context, and the caller should __NOT__ call the
/// `WasmEdge_FunctionTypeDelete` to delete it.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param ModuleName the module name WasmEdge_String.
/// \param FuncName the function name WasmEdge_String.
///
/// \returns the function type. NULL if the function not found.
WASMEDGE_CAPI_EXPORT extern const WasmEdge_FunctionTypeContext *
WasmEdge_VMGetFunctionTypeRegistered(WasmEdge_VMContext *Cxt,
                                     const WasmEdge_String ModuleName,
                                     const WasmEdge_String FuncName);

/// Reset of WasmEdge_VMContext.
///
/// After calling this function, the statistics, loaded module, and the
/// instances in the store except registered instances will be cleared.
///
/// \param Cxt the WasmEdge_VMContext to reset.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_VMCleanup(WasmEdge_VMContext *Cxt);

/// Get the length of exported function list.
///
/// \param Cxt the WasmEdge_VMContext.
///
/// \returns length of exported function list.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_VMGetFunctionListLength(WasmEdge_VMContext *Cxt);

/// Get the exported function list.
///
/// The returned function names filled into the `Names` array link to the
/// exported names of functions owned by the vm context, and the caller should
/// __NOT__ call the `WasmEdge_StringDelete` to delete them.
/// The function type contexts filled into the `FuncTypes` array of the
/// corresponding function names link to the context owned by the VM context.
/// The caller should __NOT__ call the `WasmEdge_FunctionTypeDelete` to delete
/// them.
/// If the `Names` and `FuncTypes` buffer lengths are smaller than the result of
/// the exported function list size, the overflowed return values will be
/// discarded.
///
/// \param Cxt the WasmEdge_VMContext.
/// \param [out] Names the output names WasmEdge_String buffer of exported
/// functions. Can be NULL if names are not needed.
/// \param [out] FuncTypes the function type contexts buffer. Can be NULL if
/// function types are not needed.
/// \param Len the buffer length.
///
/// \returns actual exported function list size.
WASMEDGE_CAPI_EXPORT extern uint32_t
WasmEdge_VMGetFunctionList(WasmEdge_VMContext *Cxt, WasmEdge_String *Names,
                           const WasmEdge_FunctionTypeContext **FuncTypes,
                           const uint32_t Len);

/// Get the import object corresponding to the WasmEdge_HostRegistration
/// settings.
///
/// When creating the VM context with configuration, the host module will be
/// registered according to the `WasmEdge_HostRegistration` settings added into
/// the `WasmEdge_ConfigureContext`. You can call this function to get the
/// `WasmEdge_ImportObjectContext` corresponding to the settings. The import
/// object context links to the context owned by the VM context. The caller
/// should __NOT__ call the `WasmEdge_ImportObjectDelete`.
///
/// ```c
/// WasmEdge_ConfigureContext *Conf = WasmEdge_ConfigureCreate();
/// WasmEdge_ConfigureAddHostRegistration(Conf, WasmEdge_HostRegistration_Wasi);
/// WasmEdge_ConfigureAddHostRegistration(
///     Conf, WasmEdge_HostRegistration_WasmEdge_Process);
/// WasmEdge_VMContext *VM = WasmEdge_VMCreate(Conf, NULL);
/// WasmEdge_ImportObjectContext *WasiMod =
///     WasmEdge_VMGetImportModuleContext(VM, WasmEdge_HostRegistration_Wasi);
/// WasmEdge_ImportObjectContext *ProcessMod =
///     WasmEdge_VMGetImportModuleContext(
///         VM, WasmEdge_HostRegistration_WasmEdge_Process);
/// ```
///
/// \param Cxt the WasmEdge_VMContext.
/// \param Reg the host registration value to get the import module.
///
/// \returns pointer to the import module context. NULL if not found.
WASMEDGE_CAPI_EXPORT extern WasmEdge_ImportObjectContext *
WasmEdge_VMGetImportModuleContext(WasmEdge_VMContext *Cxt,
                                  const enum WasmEdge_HostRegistration Reg);

/// Get the store context used in the WasmEdge_VMContext.
///
/// The store context links to the store in the VM context and owned by the VM
/// context. The caller should __NOT__ call the `WasmEdge_StoreDelete`.
///
/// \param Cxt the WasmEdge_VMContext.
///
/// \returns pointer to the store context.
WASMEDGE_CAPI_EXPORT extern WasmEdge_StoreContext *
WasmEdge_VMGetStoreContext(WasmEdge_VMContext *Cxt);

/// Get the statistics context used in the WasmEdge_VMContext.
///
/// The statistics context links to the statistics in the VM context and owned
/// by the VM context. The caller should __NOT__ call the
/// `WasmEdge_StatisticsDelete`.
///
/// \param Cxt the WasmEdge_VMContext.
///
/// \returns pointer to the statistics context.
WASMEDGE_CAPI_EXPORT extern WasmEdge_StatisticsContext *
WasmEdge_VMGetStatisticsContext(WasmEdge_VMContext *Cxt);

/// Deletion of the WasmEdge_VMContext.
///
/// After calling this function, the context will be freed and should __NOT__ be
/// used.
///
/// \param Cxt the WasmEdge_VMContext to delete.
WASMEDGE_CAPI_EXPORT extern void WasmEdge_VMDelete(WasmEdge_VMContext *Cxt);

/// <<<<<<<< WasmEdge VM functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

#ifdef __cplusplus
} /// extern "C"
#endif

#endif /// WASMEDGE_C_API_H
