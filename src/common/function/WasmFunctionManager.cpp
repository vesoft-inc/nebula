//
// Created by arcosx on 12/14/21.
//
#include "WasmFunctionManager.h"

#include <iostream>

#include "HTTPRequest.hpp"
#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Geography.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Vertex.h"

namespace nebula {

const std::string WasmFunctionManager::TYPE_WAT_MOUDLE = "WAT";    // wat str base64
const std::string WasmFunctionManager::TYPE_WASM_MOUDLE = "WASM";  // wasm str base64bindata
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

std::vector<Value> WasmFunctionManager::runWithNebulaDataHandle(std::string functionName,
                                                                std::vector<Value> args) {
  // find type it must exist if we checked.
  std::unordered_map<std::string, std::string>::const_iterator gotType = typeMap.find(functionName);

  if (gotType->second == TYPE_WAT_MOUDLE) {
    auto module = modules.at(functionName);
    return run(module, args);
  }

  auto module = wasmModules.at(functionName);
  return run(module, args);
}

std::vector<Value> WasmFunctionManager::run(const WasmtimeRunInstance &wasmTimeRunInstance,
                                            std::vector<Value> args) {
  // the args which wasm run
  std::vector<wasmtime::Val> functionArgs;

  if (args.size() != wasmTimeRunInstance.inParam.size()) {
    LOG(ERROR) << "Param not equal function register";
    return {};
  }
  for (size_t i = 0; i < wasmTimeRunInstance.inParam.size(); ++i) {
    switch (wasmTimeRunInstance.inParam[i]) {
      case WasmFunctionParamType::INT32: {
        functionArgs.emplace_back(args[i].getInt());
        break;
      }
      case WasmFunctionParamType::INT64: {
        functionArgs.emplace_back(args[i].getInt());
        break;
      }
      case WasmFunctionParamType::FLOAT: {
        functionArgs.emplace_back(args[i].getFloat());
        break;
      }
      default:
        LOG(ERROR) << "Param not Support ";
        return {};
    }
  }

  // the return
  std::vector<Value> result;
  auto results = wasmTimeRunInstance.func.call(store, functionArgs).unwrap();

  switch (wasmTimeRunInstance.outParam) {
    case WasmFunctionParamType::INT32: {
      result.emplace_back(static_cast<int>(results[0].i32()));
      return result;
    }
    case WasmFunctionParamType::INT64: {
      result.emplace_back(static_cast<int>(results[0].i64()));
      return result;
    }
    case WasmFunctionParamType::FLOAT: {
      result.emplace_back(static_cast<int>(results[0].f32()));
      return result;
    }
    case WasmFunctionParamType::LIST: {
      for (size_t i = 0; i < results.size(); ++i) {
        switch (results[i].kind()) {
          case wasmtime::ValKind::I32:
            result.emplace_back(static_cast<int>(results[i].i32()));
            break;
          case wasmtime::ValKind::I64:
            result.emplace_back(static_cast<int>(results[i].i64()));
            break;
          case wasmtime::ValKind::F32:
            result.emplace_back(static_cast<float>(results[i].f32()));
            break;
          case wasmtime::ValKind::F64:
            result.emplace_back(static_cast<float>(results[i].f64()));
            break;
          default:
            LOG(ERROR) << "Return type not Support ";
            return {};
        }
      }
      return result;
    }
    default:
      return {};
  }
}

std::vector<Value> WasmFunctionManager::run(const WasmEdgeRunInstance &wasmEdgeRunInstance,
                                            std::vector<Value> args) {
  if (args.size() != wasmEdgeRunInstance.inParam.size()) {
    LOG(ERROR) << "Param not equal function register";
    return {};
  }

  uint8_t buf[wasmEdgeRunInstance.wasmBuffer.size()];
  std::copy(wasmEdgeRunInstance.wasmBuffer.begin(), wasmEdgeRunInstance.wasmBuffer.end(), buf);

  // 1. register wasmedge file
  WasmEdge_Result Res;
  Res = WasmEdge_VMLoadWasmFromBuffer(VMCxt, buf, sizeof(buf));
  if (!WasmEdge_ResultOK(Res)) {
    printf("Loading phase failed: %s\n", WasmEdge_ResultGetMessage(Res));
    return {};
  }
  Res = WasmEdge_VMValidate(VMCxt);
  if (!WasmEdge_ResultOK(Res)) {
    printf("Validation phase failed: %s\n", WasmEdge_ResultGetMessage(Res));
    return {};
  }
  Res = WasmEdge_VMInstantiate(VMCxt);
  if (!WasmEdge_ResultOK(Res)) {
    printf("Instantiation phase failed: %s\n", WasmEdge_ResultGetMessage(Res));
    return {};
  }

  // 2. set the memory
  auto Store = WasmEdge_VMGetStoreContext(VMCxt);
  uint32_t MemLen = WasmEdge_StoreListMemoryLength(Store);
  WasmEdge_String MemNames[MemLen];
  WasmEdge_StoreListMemory(Store, MemNames, MemLen);
  auto MemInst = WasmEdge_StoreFindMemory(Store, MemNames[0]);
  uint32_t ResultMemAddr = 8;

  // the final args pass to function
  std::vector<WasmEdge_Value> Args;

  for (size_t i = 0; i < wasmEdgeRunInstance.inParam.size(); ++i) {
    switch (wasmEdgeRunInstance.inParam[i]) {
      case WasmFunctionParamType::INT32: {
        Args.emplace_back(WasmEdge_ValueGenI32(args[i].getInt()));
        break;
      }
      case WasmFunctionParamType::INT64: {
        Args.emplace_back(WasmEdge_ValueGenI64(args[i].getInt()));
        break;
      }
      case WasmFunctionParamType::FLOAT: {
        Args.emplace_back(WasmEdge_ValueGenF32(args[i].getFloat()));
        break;
      }
      case WasmFunctionParamType::STRING: {
        std::string RequestStr = args[i].getStr();
        // 2.1 set the magic number as first address

        Args.emplace_back(WasmEdge_ValueGenI32(ResultMemAddr));

        uint32_t MallocSize = 0, MallocAddr = 0;
        std::string StrArg = RequestStr;
        MallocSize = StrArg.length();
        WasmEdge_Value Params = WasmEdge_ValueGenI32(MallocSize);
        WasmEdge_Value Rets;
        // must be `__wbindgen_malloc`
        WasmEdge_String memFuncName = WasmEdge_StringCreateByCString("__wbindgen_malloc");

        // 2.2 set the memory to vm
        WasmEdge_Result createResult = WasmEdge_VMExecute(VMCxt, memFuncName, &Params, 1, &Rets, 1);
        WasmEdge_StringDelete(memFuncName);

        MallocAddr = (uint32_t)WasmEdge_ValueGetI32(Rets);

        if (!WasmEdge_ResultOK(createResult)) {
          printf("set data failed: %s\n", WasmEdge_ResultGetMessage(createResult));
        } else {
          printf("set data success: %s\n", WasmEdge_ResultGetMessage(createResult));
        }

        Args.emplace_back(WasmEdge_ValueGenI32(MallocAddr));
        Args.emplace_back(WasmEdge_ValueGenI32(MallocSize));

        // 2.3 copy data to memory.
        std::vector<uint8_t> bufVector(StrArg.begin(), StrArg.end());
        WasmEdge_MemoryInstanceSetData(MemInst, bufVector.data(), MallocAddr, bufVector.size());
        break;
      }
      default:
        LOG(ERROR) << "Param not Support ";
        return {};
    }
  }

  std::vector<Value> result;

  // single return
  if (wasmEdgeRunInstance.outParam == WasmFunctionParamType::INT32 ||
      wasmEdgeRunInstance.outParam == WasmFunctionParamType::INT64 ||
      wasmEdgeRunInstance.outParam == WasmFunctionParamType::FLOAT) {
    // 3. the return
    WasmEdge_Value Ret;
    // 4. run the function
    WasmEdge_String FuncHanderName = wasmEdgeRunInstance.funcHandlerName;

    Res = WasmEdge_VMExecute(VMCxt, FuncHanderName, Args.data(), Args.size(), &Ret, 1);
    WasmEdge_StringDelete(FuncHanderName);
    if (!WasmEdge_ResultOK(Res)) {
      LOG(ERROR) << "get return failed:" << WasmEdge_ResultGetMessage(Res);
    } else {
      LOG(INFO) << "get return success:" << WasmEdge_ResultGetMessage(Res);
    }

    if (wasmEdgeRunInstance.outParam == WasmFunctionParamType::INT32) {
      result.push_back(static_cast<int>(WasmEdge_ValueGetI32(Ret)));
      return result;
    }
    if (wasmEdgeRunInstance.outParam == WasmFunctionParamType::INT64) {
      result.push_back(static_cast<int>(WasmEdge_ValueGetI64(Ret)));
      return result;
    }
    if (wasmEdgeRunInstance.outParam == WasmFunctionParamType::FLOAT) {
      result.push_back(static_cast<int>(WasmEdge_ValueGetF32(Ret)));
      return result;
    }
  } else if (wasmEdgeRunInstance.outParam == WasmFunctionParamType::LIST) {
    // hackathon: default 3 param
    // TODO:// dynamic number return
    int paramNum = 3;
    WasmEdge_Value Ret[paramNum];
    // 4. run the function
    WasmEdge_String FuncHanderName = wasmEdgeRunInstance.funcHandlerName;
    Res = WasmEdge_VMExecute(VMCxt, FuncHanderName, Args.data(), Args.size(), Ret, paramNum);
    WasmEdge_StringDelete(FuncHanderName);
    if (!WasmEdge_ResultOK(Res)) {
      printf("get return failed: %s\n", WasmEdge_ResultGetMessage(Res));
    } else {
      printf("get return success: %s\n", WasmEdge_ResultGetMessage(Res));
    }

    for (int i = 0; i < paramNum; ++i) {
      switch (Ret[i].Type) {
        case WasmEdge_ValType_I32:
          result.emplace_back(static_cast<float>(WasmEdge_ValueGetI32(Ret[i])));
          break;
        case WasmEdge_ValType_I64:
          result.emplace_back(static_cast<float>(WasmEdge_ValueGetI64(Ret[i])));
          break;
        case WasmEdge_ValType_F32:
          result.emplace_back(static_cast<float>(WasmEdge_ValueGetF32(Ret[i])));
          break;
        case WasmEdge_ValType_F64:
          result.emplace_back(static_cast<float>(WasmEdge_ValueGetF64(Ret[i])));
          break;
        default:
          LOG(ERROR) << "Return Param not Support ";
          return {};
      }
    }

  } else {  // the string type
            // 4. run the function
    WasmEdge_String FuncHanderName = wasmEdgeRunInstance.funcHandlerName;

    WasmEdge_Value Ret;

    Res = WasmEdge_VMExecute(VMCxt, FuncHanderName, Args.data(), Args.size(), &Ret, 1);
    WasmEdge_StringDelete(FuncHanderName);
    if (!WasmEdge_ResultOK(Res)) {
      printf("get return failed: %s\n", WasmEdge_ResultGetMessage(Res));
    } else {
      printf("get return success: %s\n", WasmEdge_ResultGetMessage(Res));
    }

    // 5
    // 5.1 get the data address
    uint8_t ResultMem[8];
    Res = WasmEdge_MemoryInstanceGetData(MemInst, ResultMem, ResultMemAddr, 8);
    uint32_t ResultDataAddr = 0;
    uint32_t ResultDataLen = 0;
    if (WasmEdge_ResultOK(Res)) {
      ResultDataAddr =
          ResultMem[0] | (ResultMem[1] << 8) | (ResultMem[2] << 16) | (ResultMem[3] << 24);
      ResultDataLen =
          ResultMem[4] | (ResultMem[5] << 8) | (ResultMem[6] << 16) | (ResultMem[7] << 24);
    } else {
      printf("get data return failed: %s\n", WasmEdge_ResultGetMessage(Res));
      return {};
    }
    // 5.2 get the data
    std::vector<uint8_t> ResultData(ResultDataLen);
    auto getFinalRes =
        WasmEdge_MemoryInstanceGetData(MemInst, ResultData.data(), ResultDataAddr, ResultDataLen);
    if (WasmEdge_ResultOK(getFinalRes)) {
      printf("get final data success\n");
    } else {
      printf("get final data return failed: %s\n", WasmEdge_ResultGetMessage(getFinalRes));
      return {};
    }
    std::string ResultString(ResultData.begin(), ResultData.end());

    result.push_back(ResultString);
    return result;
  }
  return {};
}

// default wat functionHandler = "main"
bool WasmFunctionManager::RegisterFunction(std::vector<WasmFunctionParamType> inParam,
                                           WasmFunctionParamType outParam,
                                           std::string moduleType,
                                           std::string functionName,
                                           std::string functionHandler,
                                           const std::string &base64OrOtherString) {
  if (typeMap.find(functionName) != typeMap.end()) {
    LOG(ERROR) << "UDF function \"" << functionName << "\" exists!";
    return false;
  }

  // for WAT
  if (moduleType == TYPE_WAT_MOUDLE) {
    auto watString = myBase64Decode(base64OrOtherString);
    auto wasmRuntime = createInstanceAndFunction(watString, functionHandler);
    wasmRuntime.inParam = inParam;
    wasmRuntime.outParam = outParam;
    modules.emplace(functionName, wasmRuntime);
    typeMap.emplace(functionName, TYPE_WAT_MOUDLE);
    return true;
  }

  // for WASM-PATH
  std::string wasmString;
  WasmEdge_String functionHandlerName = WasmEdge_StringCreateByCString(functionHandler.data());
  if (moduleType == TYPE_WASM_PATH) {
    // still file to this map
    if (base64OrOtherString.rfind("http://", 0) == 0) {
      std::vector<uint8_t> bufVector = getHTTPString(base64OrOtherString);
      if (bufVector.empty()) {
        return false;
      }
      auto wasmEdgeRunInstance = WasmEdgeRunInstance(functionHandlerName, bufVector);
      wasmEdgeRunInstance.inParam = inParam;
      wasmEdgeRunInstance.outParam = outParam;
      wasmModules.emplace(functionName, wasmEdgeRunInstance);
      typeMap.emplace(functionName, TYPE_WASM_MOUDLE);
      return true;
    } else if (base64OrOtherString.rfind("file://", 0) == 0) {
      wasmString = getFileString(base64OrOtherString);
      if (wasmString == "") {
        return false;
      }
      std::vector<uint8_t> bufVector(wasmString.begin(), wasmString.end());
      auto wasmEdgeRunInstance = WasmEdgeRunInstance(functionHandlerName, bufVector);
      wasmEdgeRunInstance.inParam = inParam;
      wasmEdgeRunInstance.outParam = outParam;
      wasmModules.emplace(functionName, wasmEdgeRunInstance);
      typeMap.emplace(functionName, TYPE_WASM_MOUDLE);
      return true;
    } else {
      return false;
    }
  }

  if (moduleType == TYPE_WASM_MOUDLE) {
    wasmString = myBase64Decode(base64OrOtherString);
    std::vector<uint8_t> bufVector(wasmString.begin(), wasmString.end());
    auto wasmEdgeRunInstance = WasmEdgeRunInstance(functionHandlerName, bufVector);
    wasmEdgeRunInstance.inParam = inParam;
    wasmEdgeRunInstance.outParam = outParam;
    wasmModules.emplace(functionName, wasmEdgeRunInstance);
    typeMap.emplace(functionName, TYPE_WASM_MOUDLE);
    return true;
  }

  return false;
}

bool WasmFunctionManager::DeleteFunction(std::string functionName) {
  if (typeMap.find(functionName) == typeMap.end()) {
    return true;
  }

  std::unordered_map<std::string, std::string>::const_iterator gotType = typeMap.find(functionName);
  if (gotType->second == TYPE_WAT_MOUDLE) {
    modules.erase(functionName);
  }
  if (gotType->second == TYPE_WASM_MOUDLE) {
    wasmModules.erase(functionName);
  }
  typeMap.erase(functionName);
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
bool WasmFunctionManager::existFunction(std::string functionName) {
  if (typeMap.find(functionName) == typeMap.end()) {
    return false;
  }
  return true;
}
WasmFunctionParamType WasmFunctionManager::getFunctionReturnType(std::string functionName) {
  std::unordered_map<std::string, std::string>::const_iterator gotType = typeMap.find(functionName);
  if (gotType->second == TYPE_WAT_MOUDLE) {
    auto module = modules.at(functionName);
    return module.outParam;
  }
  auto module = wasmModules.at(functionName);
  return module.outParam;
}

}  // namespace nebula
