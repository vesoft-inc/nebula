//
// Created by arcosx on 12/14/21.
//



#include <fstream>
#include <iostream>
#include <sstream>
#include "WasmFunctionManager.h"

WasmFunctionManager::WasmFunctionManager() {
  engine = new wasmtime::Engine;
  store = new wasmtime::Store(*engine);
}

WasmFunctionManager::~WasmFunctionManager() {
  delete (store);
  delete (engine);
}

std::string readFile(const std::string name) {
  std::ifstream watFile;
  watFile.open(name);
  std::stringstream strStream;
  strStream << watFile.rdbuf();
  return strStream.str();
}

void WasmFunctionManager::runWatFile(const std::string &fileName) const {
  // Read our input file, which in this case is a wat text file.
  auto wat = readFile(fileName);
  runWat(wat);
}

void WasmFunctionManager::runWat(const std::string &watString) const {
  auto module = wasmtime::Module::compile(*engine, watString).unwrap();
  auto instance = wasmtime::Instance::create(store, module, {}).unwrap();

  // Invoke `gcd` export
  auto func = std::get<wasmtime::Func>(*instance.get(store, "main"));
  auto results = func.call(store, {6, 27}).unwrap();

  std::cout << "gcd(6, 27) = " << results[0].i32() << "\n";
}

WasmRuntime WasmFunctionManager::createInstanceAndFunction(
    const std::string &watString, const std::string functionHandler) {
  auto module = wasmtime::Module::compile(*engine, watString).unwrap();
  auto instance = wasmtime::Instance::create(store, module, {}).unwrap();

  auto function_obj = instance.get(store, functionHandler);

  wasmtime::Func *func = std::get_if<wasmtime::Func>(&*function_obj);
  return WasmRuntime(*func,instance);
}


WasmRuntime::WasmRuntime(const wasmtime::Func &func, const wasmtime::Instance &instance)
    : func(func), instance(instance) {}
