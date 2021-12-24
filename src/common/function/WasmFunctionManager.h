//
// Created by arcosx on 12/14/21.
//

#ifndef NEBULA_GRAPH_WASMFUNCTIONMANAGER_H
#define NEBULA_GRAPH_WASMFUNCTIONMANAGER_H
#include <wasmedge/wasmedge.h>

#include <cassert>
#include <cctype>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include <wasmtime.hh>

#include "common/datatypes/Value.h"
#include "common/function/FunctionManager.h"

namespace nebula {

enum class WasmFunctionParamType {
  INT32 = 1,
  INT64 = 2,
  FLOAT = 3,
  STRING = 4,
  LIST = 5,
};

struct WasmtimeRunInstance {
  std::vector<WasmFunctionParamType> inParam;
  WasmFunctionParamType outParam;

  wasmtime::Func func;
  wasmtime::Instance instance;
  WasmtimeRunInstance(const wasmtime::Func &func, const wasmtime::Instance &instance)
      : func(func), instance(instance) {}
};

struct WasmEdgeRunInstance {
  std::vector<WasmFunctionParamType> inParam;
  WasmFunctionParamType outParam;

  WasmEdge_String funcHandlerName;
  std::vector<uint8_t> wasmBuffer;
  WasmEdgeRunInstance(const WasmEdge_String &funcHandlerName,
                      const std::vector<uint8_t> &wasmBuffer)
      : funcHandlerName(funcHandlerName), wasmBuffer(wasmBuffer) {}
};

// include two runtime:
// wasmtime: for wat (wat://)
// WasmEdge: for wasm binary(wasm://) remote network or local path(path://)
class WasmFunctionManager {
 private:
  // the type map...
  std::unordered_map<std::string, std::string> typeMap;
  // wasmtime
  wasmtime::Engine *engine;
  wasmtime::Store *store;
  std::unordered_map<std::string, WasmtimeRunInstance> modules;

  // wasmedge
  WasmEdge_ConfigureContext *ConfCxt;
  WasmEdge_VMContext *VMCxt;
  std::unordered_map<std::string, WasmEdgeRunInstance> wasmModules;

  WasmFunctionManager() {
    engine = new wasmtime::Engine;
    store = new wasmtime::Store(*engine);
    /* 创建配置上下文以及 WASI 支持。 */
    /* 除非你需要使用 WASI，否则这步不是必须的。 */
    ConfCxt = WasmEdge_ConfigureCreate();
    WasmEdge_ConfigureAddHostRegistration(ConfCxt, WasmEdge_HostRegistration_Wasi);
    /* 创建VM的时候可以提供空的配置。*/
    VMCxt = WasmEdge_VMCreate(ConfCxt, NULL);
  }

  ~WasmFunctionManager() {
    delete (store);
    delete (engine);
    /* 资源析构。 */
    WasmEdge_VMDelete(VMCxt);
    WasmEdge_ConfigureDelete(ConfCxt);
  }
  WasmFunctionManager(const WasmFunctionManager &);
  WasmFunctionManager &operator=(const WasmFunctionManager &);

  // base64 tool
  constexpr static const char b64_table[65] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  constexpr static const char reverse_table[128] = {
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62,
      64, 64, 64, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64, 64, 0,
      1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
      23, 24, 25, 64, 64, 64, 64, 64, 64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
      39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64};

 public:
  static const std::string TYPE_WAT_MOUDLE;
  static const std::string TYPE_WASM_MOUDLE;
  static const std::string TYPE_WASM_PATH;

  static WasmFunctionManager &getInstance() {
    static WasmFunctionManager instance;
    return instance;
  }

  WasmtimeRunInstance createInstanceAndFunction(const std::string &watString,
                                                const std::string functionHandler);
  std::vector<Value> run(const WasmtimeRunInstance &wasmTimeRunInstance, std::vector<Value> args);

  bool DeleteFunction(std::string functionName);
  std::vector<Value> runWithNebulaDataHandle(std::string functionName, std::vector<Value> args);
  std::vector<Value> run(const WasmEdgeRunInstance &wasmEdgeRunInstance, std::vector<Value> args);
  bool existFunction(std::string functionName);
  WasmFunctionParamType getFunctionReturnType(std::string functionName);
  std::vector<uint8_t> getHTTPString(const std::string &basicString);
  std::string getFileString(const std::string &basicString);
  bool RegisterFunction(std::vector<WasmFunctionParamType> inParam,
                        WasmFunctionParamType outParam,
                        std::string moduleType,
                        std::string functionName,
                        std::string functionHandler,
                        const std::string &base64OrOtherString);

  // base64 tool
  static std::string myBase64Decode(const ::std::string &ascdata) {
    using ::std::string;
    string retval;
    const string::const_iterator last = ascdata.end();
    int bits_collected = 0;
    unsigned int accumulator = 0;

    for (string::const_iterator i = ascdata.begin(); i != last; ++i) {
      const int c = *i;
      if (::std::isspace(c) || c == '=') {
        // Skip whitespace and padding. Be liberal in what you accept.
        continue;
      }
      if ((c > 127) || (c < 0) || (reverse_table[c] > 63)) {
        throw ::std::invalid_argument(
            "This contains characters not legal in a base64 encoded string.");
      }
      accumulator = (accumulator << 6) | reverse_table[c];
      bits_collected += 6;
      if (bits_collected >= 8) {
        bits_collected -= 8;
        retval += static_cast<char>((accumulator >> bits_collected) & 0xffu);
      }
    }
    return retval;
  }
};
}  // namespace nebula

#endif  // NEBULA_GRAPH_WASMFUNCTIONMANAGER_H
