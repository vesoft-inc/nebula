//
// Created by arcosx on 12/14/21.
//

#ifndef NEBULA_GRAPH_WASMFUNCTIONMANAGER_H
#define NEBULA_GRAPH_WASMFUNCTIONMANAGER_H

#include <wasmtime.hh>


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
};

#endif  // NEBULA_GRAPH_WASMFUNCTIONMANAGER_H
