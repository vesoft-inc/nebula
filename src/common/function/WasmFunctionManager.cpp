//
// Created by arcosx on 12/14/21.
//
#include "common/datatypes/List.h"
#include "WasmFunctionManager.h"
#include <iostream>

namespace nebula {

std::string readFile(const std::string name) {
  std::ifstream watFile;
  watFile.open(name);
  std::stringstream strStream;
  strStream << watFile.rdbuf();
  return strStream.str();
}

//void WasmFunctionManager::runWatFile(const std::string &fileName) const {
//  // Read our input file, which in this case is a wat text file.
//  auto wat = readFile(fileName);
//  runWat(wat);
//}
//
//void WasmFunctionManager::runWat(const std::string &watString) const {
//  auto module = wasmtime::Module::compile(*engine, watString).unwrap();
//  auto instance = wasmtime::Instance::create(store, module, {}).unwrap();
//
//  // Invoke `gcd` export
//  auto func = std::get<wasmtime::Func>(*instance.get(store, "main"));
//  auto results = func.call(store, {6, 27}).unwrap();
//
//  std::cout << "gcd(6, 27) = " << results[0].i32() << "\n";
//}

WasmtimeRunInstance WasmFunctionManager::createInstanceAndFunction(const std::string &watString,
                                                           const std::string functionHandler) {
  auto module = wasmtime::Module::compile(*engine, watString).unwrap();
  auto instance = wasmtime::Instance::create(store, module, {}).unwrap();

  auto function_obj = instance.get(store, functionHandler);

  wasmtime::Func *func = std::get_if<wasmtime::Func>(&*function_obj);
  return WasmtimeRunInstance(*func, instance);
}

nebula::List WasmFunctionManager::runWithNebulaDataHandle(std::string functionName, nebula::List args) {

  if(modules.find(functionName) == modules.end()){
    // return not found!
    std::vector<nebula::Value> Error;
    Error.emplace_back("Not Found Function");
    return nebula::List(Error);
  }
  auto module = modules.at(functionName);
  // prepostcess
  auto values = args.values;
  std::vector<int> functionArgs;

  for (const auto& val : values) {
    functionArgs.push_back(val.getInt());
  }
  // run
  auto functionResult = WasmFunctionManager::run(module, functionArgs);

  // postprocess
  std::vector<nebula::Value> tmpProcess;
  for (const auto& val : functionResult) {
    tmpProcess.emplace_back(val);
  }
  return nebula::List(tmpProcess);
}

std::vector<int> WasmFunctionManager::run(std::string functionName, std::vector<int> args) {
  if(modules.find(functionName) == modules.end()){
    return std::vector<int>();
  }
  auto module = modules.at(functionName);
  // run
  return WasmFunctionManager::run(module, args);

}
std::vector<int> WasmFunctionManager::run(const WasmtimeRunInstance &wasmRuntime, std::vector<int> args) {
  // the args which wasm run

  std::vector<wasmtime::Val> argv;
  for (size_t i = 0; i < args.size(); ++i) {
    argv.emplace_back(static_cast<int32_t>(args[i]));
  }

  // the return
  std::vector<int> result;
    auto results = wasmRuntime.func.call(store, argv).unwrap();

  for (size_t i = 0; i < results.size(); ++i) {
    result.push_back(static_cast<int>(results[i].i32()));
  }

  return result;
}

bool WasmFunctionManager::RegisterFunction(std::string functionName,std::string functionHandler,const std::string &watBase64String) {
  // default functionHandler = "main"
  auto watString = myBase64Decode(watBase64String);
  auto wasmRuntime =  createInstanceAndFunction(watString,functionHandler);
  modules.emplace(functionName,wasmRuntime);
  return true;
}

bool  WasmFunctionManager::DeleteFunction(std::string functionName) {
  modules.erase(functionName);
  return true;
}

}  // namespace nebula
