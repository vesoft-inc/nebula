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
  WasmRuntime(const wasmtime::Func &func, const wasmtime::Instance &instance)
      : func(func), instance(instance) {}
};

class WasmFunctionManager {
 private:
  wasmtime::Engine *engine;
  wasmtime::Store *store;
  /* data */
  std::unordered_map<std::string, WasmRuntime> modules;
  WasmFunctionManager() {
    engine = new wasmtime::Engine;
    store = new wasmtime::Store(*engine);
  }

  ~WasmFunctionManager() {
    delete (store);
    delete (engine);
  }
  WasmFunctionManager(const WasmFunctionManager&);
  WasmFunctionManager& operator=(const WasmFunctionManager&);
 public:
//  static WasmFunctionManager& getInstance() {
//    static WasmFunctionManager instance;
//    return instance;
//  }

//  WasmFunctionManager(const WasmFunctionManager&)=delete;
//  WasmFunctionManager& operator=(const WasmFunctionManager&)=delete;

  static WasmFunctionManager& getInstance()
  {
    static WasmFunctionManager instance;
    return instance;
  }

  void runWatFile(const std::string &fileName) const;

  void runWat(const std::string &watString) const;

  WasmRuntime createInstanceAndFunction(const std::string &watString,
                                        const std::string functionHandler);
  std::vector<int> run(const WasmRuntime &wasmRuntime, std::vector<int> args);
  bool RegisterFunction(std::string functionName,
                        std::string functionHandler,
                        const std::string &watString);
  List runWithHandle(std::string functionName, List args);
};
}  // namespace nebula


#endif  // NEBULA_GRAPH_WASMFUNCTIONMANAGER_H
