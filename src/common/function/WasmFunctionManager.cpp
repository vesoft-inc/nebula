//
// Created by arcosx on 12/14/21.
//
#include "WasmFunctionManager.h"

#include <iostream>

#include "HTTPRequest.hpp"
#include "common/datatypes/List.h"

namespace nebula {

const std::string WasmFunctionManager::TYPE_WAT_MOUDLE = "WAT";    // wat str base64
const std::string WasmFunctionManager::TYPE_WASM_MOUDLE = "WASM";  // wasm str base64
const std::string WasmFunctionManager::TYPE_WASM_PATH =
    "WASM_PATH";  // wasm path (http or file path)

std::string readFile(const std::string name) {
  std::ifstream watFile;
  watFile.open(name);
  std::stringstream strStream;
  strStream << watFile.rdbuf();
  return strStream.str();
}

WasmtimeRunInstance WasmFunctionManager::createInstanceAndFunction(
    const std::string &watString, const std::string functionHandler) {
  auto module = wasmtime::Module::compile(*engine, watString).unwrap();
  auto instance = wasmtime::Instance::create(store, module, {}).unwrap();

  auto function_obj = instance.get(store, functionHandler);

  wasmtime::Func *func = std::get_if<wasmtime::Func>(&*function_obj);
  return WasmtimeRunInstance(*func, instance);
}

nebula::List WasmFunctionManager::runWithNebulaDataHandle(std::string functionName,
                                                          nebula::List args) {
  if (modules.find(functionName) == modules.end()) {
    // return not found!
    std::vector<nebula::Value> Error;
    Error.emplace_back("Not Found Function");
    return nebula::List(Error);
  }
  auto module = modules.at(functionName);
  // prepostcess
  auto values = args.values;
  std::vector<int> functionArgs;

  for (const auto &val : values) {
    functionArgs.push_back(val.getInt());
  }
  // run
  auto functionResult = WasmFunctionManager::run(module, functionArgs);

  // postprocess
  std::vector<nebula::Value> tmpProcess;
  for (const auto &val : functionResult) {
    tmpProcess.emplace_back(val);
  }
  return nebula::List(tmpProcess);
}

std::vector<int> WasmFunctionManager::run(std::string functionName, std::vector<int> args) {
  if (typeMap.find(functionName) == typeMap.end()) {
    return std::vector<int>();
  }

  std::unordered_map<std::string, std::string>::const_iterator gotType = typeMap.find(functionName);
  if (gotType->second == TYPE_WAT_MOUDLE) {
    auto module = modules.at(functionName);
    return run(module, args);
  }

  auto module = wasmModules.at(functionName);
  return run(module, args);
}
std::vector<int> WasmFunctionManager::run(const WasmtimeRunInstance &wasmTimeRunInstance,
                                          std::vector<int> args) {
  // the args which wasm run

  std::vector<wasmtime::Val> argv;
  for (size_t i = 0; i < args.size(); ++i) {
    argv.emplace_back(static_cast<int32_t>(args[i]));
  }

  // the return
  std::vector<int> result;
  auto results = wasmTimeRunInstance.func.call(store, argv).unwrap();

  for (size_t i = 0; i < results.size(); ++i) {
    result.push_back(static_cast<int>(results[i].i32()));
  }

  return result;
}

std::vector<int> WasmFunctionManager::run(const WasmEdgeRunInstance &wasmEdgeRunInstance,
                                          std::vector<int> args) {
  WasmEdge_Value Returns[1];
  WasmEdge_Value Params[2] = {WasmEdge_ValueGenI32(23), WasmEdge_ValueGenI32(456)};

  uint8_t buf[wasmEdgeRunInstance.wasmBuffer.size()];
  std::copy(wasmEdgeRunInstance.wasmBuffer.begin(), wasmEdgeRunInstance.wasmBuffer.end(), buf);

  WasmEdge_Result Res = WasmEdge_VMRunWasmFromBuffer(
      VMCxt, buf, sizeof(buf), wasmEdgeRunInstance.funcHandlerName, Params, 2, Returns, 1);
  if (WasmEdge_ResultOK(Res)) {
    printf("Get result: %d\n", WasmEdge_ValueGetI32(Returns[0]));
    //      printf("Get result: %d\n", WasmEdge_ValueGetI32(Returns[1]));
  } else {
    printf("Error message: %s\n", WasmEdge_ResultGetMessage(Res));
  }
  std::vector<int> result;
  result.push_back(static_cast<int>(WasmEdge_ValueGetI32(Returns[0])));
  return result;
}

// default wat functionHandler = "main"
bool WasmFunctionManager::RegisterFunction(std::string moduleType,
                                           std::string functionName,
                                           std::string functionHandler,
                                           const std::string &base64OrOtherString) {
  if (moduleType == TYPE_WAT_MOUDLE) {
    auto watString = myBase64Decode(base64OrOtherString);
    auto wasmRuntime = createInstanceAndFunction(watString, functionHandler);
    modules.emplace(functionName, wasmRuntime);
    typeMap.emplace(functionName, TYPE_WAT_MOUDLE);
    return true;
  }

  std::string wasmString;
  WasmEdge_String functionHandlerName = WasmEdge_StringCreateByCString(functionHandler.data());
  if (moduleType == TYPE_WASM_PATH) {
    // still file to this map
    if (base64OrOtherString.rfind("http://", 0) == 0) {
      std::vector<uint8_t> bufVector = getHTTPString(base64OrOtherString);
      if(bufVector.empty()){
        return false;
      }

      wasmModules.emplace(functionName, WasmEdgeRunInstance(functionHandlerName, bufVector));
      typeMap.emplace(functionName, TYPE_WASM_MOUDLE);
      return true;
    } else if (base64OrOtherString.rfind("file://", 0) == 0) {
      wasmString = getFileString(base64OrOtherString);
      if (wasmString == "") {
        return false;
      }
      std::vector<uint8_t> bufVector(wasmString.begin(), wasmString.end());
      wasmModules.emplace(functionName, WasmEdgeRunInstance(functionHandlerName, bufVector));
      typeMap.emplace(functionName, TYPE_WASM_MOUDLE);
      return true;
    } else {
      return false;
    }
  }

  if (moduleType == TYPE_WASM_MOUDLE) {
    wasmString = myBase64Decode(base64OrOtherString);
    std::vector<uint8_t> bufVector(wasmString.begin(), wasmString.end());
    wasmModules.emplace(functionName, WasmEdgeRunInstance(functionHandlerName, bufVector));
    typeMap.emplace(functionName, TYPE_WASM_MOUDLE);
    return true;
  }

  return false;
}

bool WasmFunctionManager::DeleteFunction(std::string functionName) {
  modules.erase(functionName);
  return true;
}

// use https://github.com/elnormous/HTTPRequest
std::vector<uint8_t> WasmFunctionManager::getHTTPString(const std::string &basicString) {
  try {
    http::Request request{basicString};
    const auto response = request.send("GET");
    return response.body;
  } catch (const std::exception &e) {
    std::cerr << "Request failed, error: " << e.what() << '\n';
  }
  return std::vector<uint8_t>();
}

std::string WasmFunctionManager::getFileString(const std::string &basicString) {
  std::string prefix = "file://";
  std::string filePath = basicString.substr(prefix.length());
  std::ifstream localFile(filePath);
  if (!localFile) {
    return "";
  }
  std::string str((std::istreambuf_iterator<char>(localFile)), std::istreambuf_iterator<char>());
  return str;
}

}  // namespace nebula
