//
// Created by arcosx on 12/14/21.
//
#include <gtest/gtest.h>

#include "common/function/WasmFunctionManager.h"

namespace nebula {
namespace graph {
TEST(WasmFunctionTest, gen) {
  WasmFunctionManager& wasmFunctionManager = WasmFunctionManager::getInstance();

  auto autoWatBase64Str =
      "KG1vZHVsZQogIChmdW5jICRnY2QgKHBhcmFtIGkzMiBpMzIpIChyZXN1bHQgaTMyKQogICAgKGxvY2FsIGkzMikKICAg"
      "IGJsb2NrICA7OyBsYWJlbCA9IEAxCiAgICAgIGJsb2NrICA7OyBsYWJlbCA9IEAyCiAgICAgICAgbG9jYWwuZ2V0IDAK"
      "ICAgICAgICBicl9pZiAwICg7QDI7KQogICAgICAgIGxvY2FsLmdldCAxCiAgICAgICAgbG9jYWwuc2V0IDIKICAgICAg"
      "ICBiciAxICg7QDE7KQogICAgICBlbmQKICAgICAgbG9vcCAgOzsgbGFiZWwgPSBAMgogICAgICAgIGxvY2FsLmdldCAx"
      "CiAgICAgICAgbG9jYWwuZ2V0IDAKICAgICAgICBsb2NhbC50ZWUgMgogICAgICAgIGkzMi5yZW1fdQogICAgICAgIGxv"
      "Y2FsLnNldCAwCiAgICAgICAgbG9jYWwuZ2V0IDIKICAgICAgICBsb2NhbC5zZXQgMQogICAgICAgIGxvY2FsLmdldCAw"
      "CiAgICAgICAgYnJfaWYgMCAoO0AyOykKICAgICAgZW5kCiAgICBlbmQKICAgIGxvY2FsLmdldCAyCiAgKQogIChleHBv"
      "cnQgIm1haW4iIChmdW5jICRnY2QpKSA7OyBleHBvcnQgd2l0aCBtYWluCik=";
  std::vector<WasmFunctionParamType> inparms;
  inparms.push_back(WasmFunctionParamType::INT32);
  inparms.push_back(WasmFunctionParamType::INT32);
  WasmFunctionParamType outParam = (WasmFunctionParamType::INT32);
  wasmFunctionManager.RegisterFunction(
      inparms, outParam, WasmFunctionManager::TYPE_WAT_MOUDLE, "gcd", "main", autoWatBase64Str);
  std::vector<Value> args;
  args.emplace_back(1);
  args.emplace_back(3);
  auto watStringResult = wasmFunctionManager.runWithNebulaDataHandle("gcd", args);
  std::cout << watStringResult[0] << "\n";
  inparms.clear();
  args.clear();

  auto autoWasmBase64Str =
      "AGFzbQEAAAABBwFgAn9/AX8DAgEABwoBBmFkZFR3bwAACgkBBwAgACABagsADgRuYW1lAgcBAAIAAAEA";
  inparms.push_back(WasmFunctionParamType::INT32);
  inparms.push_back(WasmFunctionParamType::INT32);
  outParam = (WasmFunctionParamType::INT32);
  wasmFunctionManager.RegisterFunction(inparms,
                                       outParam,
                                       WasmFunctionManager::TYPE_WASM_MOUDLE,
                                       "addTwo",
                                       "addTwo",
                                       autoWasmBase64Str);
  args.emplace_back(1);
  args.emplace_back(3);
  auto wasmStrResult = wasmFunctionManager.runWithNebulaDataHandle("addTwo", args);
  std::cout << wasmStrResult[0] << "\n";
  EXPECT_EQ(wasmStrResult[0], 4);
  inparms.clear();
  args.clear();

  // need wasm-cpp project to test
  auto wasmHTTP = "http://0.0.0.0:8080/string/say/pkg/rust_bindgen_funcs_lib_bg.wasm";
  inparms.push_back(WasmFunctionParamType::STRING);
  outParam = (WasmFunctionParamType::STRING);
  wasmFunctionManager.RegisterFunction(
      inparms, outParam, WasmFunctionManager::TYPE_WASM_PATH, "string_test", "say", wasmHTTP);
  args.emplace_back("triplez");
  auto wasmHTTPResult = wasmFunctionManager.runWithNebulaDataHandle("string_test", args);
  std::cout << wasmHTTPResult[0].getStr() << "\n";
  EXPECT_EQ(wasmHTTPResult[0], "hello triplez");
  // test double call
  args.clear();
  args.emplace_back("arcosx");
  auto wasmHTTPAgainResult = wasmFunctionManager.runWithNebulaDataHandle("string_test", args);
  std::cout << wasmHTTPAgainResult[0].getStr() << "\n";
  EXPECT_EQ(wasmHTTPAgainResult[0], "hello arcosx");

}  // namespace graph
}  // namespace graph
}  // namespace nebula