//
// Created by arcosx on 12/14/21.
//
#include <gtest/gtest.h>

#include "common/function/WasmFunctionManager.h"

namespace nebula {
namespace graph {
TEST(WasmFunctionTest, gen) {
  WasmFunctionManager& wasmFunctionManager = WasmFunctionManager::getInstance();

  auto autoWasmBase64Str =
      "AGFzbQEAAAABBwFgAn9/AX8DAgEABwoBBmFkZFR3bwAACgkBBwAgACABagsADgRuYW1lAgcBAAIAAAEA";
  std::vector<WasmFunctionParamType> inparms;
  inparms.push_back(WasmFunctionParamType::INT32);
  inparms.push_back(WasmFunctionParamType::INT32);
  WasmFunctionParamType outParam = (WasmFunctionParamType::INT32);
  wasmFunctionManager.RegisterFunction(inparms,
                                       outParam,
                                       WasmFunctionManager::TYPE_WASM_MOUDLE,
                                       "addTwo",
                                       "addTwo",
                                       autoWasmBase64Str);
  std::vector<Value> args;
  args.emplace_back(1);
  args.emplace_back(3);
  auto wasmHTTPResult = wasmFunctionManager.runWithNebulaDataHandle("addTwo", args);
  std::cout << wasmHTTPResult[0] << "\n";
  EXPECT_EQ(wasmHTTPResult[0], 4);
  inparms.clear();
  args.clear();

  // need wasm-cpp project to test
  auto wasmHTTP = "http://0.0.0.0:8080/rust_bindgen_funcs_lib_bg.wasm";
  inparms.push_back(WasmFunctionParamType::STRING);
  outParam = (WasmFunctionParamType::STRING);
  wasmFunctionManager.RegisterFunction(
      inparms, outParam, WasmFunctionManager::TYPE_WASM_PATH, "string_test", "say", wasmHTTP);
  args.emplace_back("triplez");
  wasmHTTPResult = wasmFunctionManager.runWithNebulaDataHandle("string_test", args);
  std::cout << wasmHTTPResult[0].getStr() << "\n";
  EXPECT_EQ(wasmHTTPResult[0], "hello triplez");
}  // namespace graph
}  // namespace graph
}  // namespace nebula