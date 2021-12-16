//
// Created by arcosx on 12/14/21.
//

#ifndef NEBULA_GRAPH_WASMFUNCTIONMANAGER_H
#define NEBULA_GRAPH_WASMFUNCTIONMANAGER_H
#include "common/datatypes/Value.h"
#include "common/function/FunctionManager.h"
#include <wasmtime.hh>


namespace nebula {

struct WasmRuntime {
  wasmtime::Func func;
  wasmtime::Instance instance;
  WasmRuntime(const wasmtime::Func &func, const wasmtime::Instance &instance);
};

class WasmFunctionManager {
 private:
  wasmtime::Engine *engine;
  wasmtime::Store *store;
  /* data */
 public:
  WasmFunctionManager();
  ~WasmFunctionManager();
  void runWatFile(const std::string &fileName) const;

  void runWat(const std::string &watString) const;

  WasmRuntime createInstanceAndFunction(const std::string &watString,
                                        const std::string functionHandler);
  std::vector<int> run(const WasmRuntime &wasmRuntime, std::vector<int> args);
};
}  // namespace nebula

#endif  // NEBULA_GRAPH_WASMFUNCTIONMANAGER_H
