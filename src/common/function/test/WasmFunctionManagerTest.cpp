//
// Created by arcosx on 12/14/21.
//
#include <gtest/gtest.h>
#include "common/function/WasmFunctionManager.h"

namespace nebula {
namespace graph {
TEST(WasmFunctionTest, gen) {

//  auto watStr = "(module\n"
//      "  (func $gcd (param i32 i32) (result i32)\n"
//      "    (local i32)\n"
//      "    block  ;; label = @1\n"
//      "      block  ;; label = @2\n"
//      "        local.get 0\n"
//      "        br_if 0 (;@2;)\n"
//      "        local.get 1\n"
//      "        local.set 2\n"
//      "        br 1 (;@1;)\n"
//      "      end\n"
//      "      loop  ;; label = @2\n"
//      "        local.get 1\n"
//      "        local.get 0\n"
//      "        local.tee 2\n"
//      "        i32.rem_u\n"
//      "        local.set 0\n"
//      "        local.get 2\n"
//      "        local.set 1\n"
//      "        local.get 0\n"
//      "        br_if 0 (;@2;)\n"
//      "      end\n"
//      "    end\n"
//      "    local.get 2\n"
//      "  )\n"
//      "  (export \"main\" (func $gcd))\n"
//      ")";
  auto WatBase64Str = "KG1vZHVsZQogIChmdW5jICRnY2QgKHBhcmFtIGkzMiBpMzIpIChyZXN1bHQgaTMyKQogICAgKGxvY2FsIGkzMikKICAgIGJsb2NrICA7OyBsYWJlbCA9IEAxCiAgICAgIGJsb2NrICA7OyBsYWJlbCA9IEAyCiAgICAgICAgbG9jYWwuZ2V0IDAKICAgICAgICBicl9pZiAwICg7QDI7KQogICAgICAgIGxvY2FsLmdldCAxCiAgICAgICAgbG9jYWwuc2V0IDIKICAgICAgICBiciAxICg7QDE7KQogICAgICBlbmQKICAgICAgbG9vcCAgOzsgbGFiZWwgPSBAMgogICAgICAgIGxvY2FsLmdldCAxCiAgICAgICAgbG9jYWwuZ2V0IDAKICAgICAgICBsb2NhbC50ZWUgMgogICAgICAgIGkzMi5yZW1fdQogICAgICAgIGxvY2FsLnNldCAwCiAgICAgICAgbG9jYWwuZ2V0IDIKICAgICAgICBsb2NhbC5zZXQgMQogICAgICAgIGxvY2FsLmdldCAwCiAgICAgICAgYnJfaWYgMCAoO0AyOykKICAgICAgZW5kCiAgICBlbmQKICAgIGxvY2FsLmdldCAyCiAgKQogIChleHBvcnQgIm1haW4iIChmdW5jICRnY2QpKSA7OyBleHBvcnQgd2l0aCBtYWluCikK";
  WasmFunctionManager&  wasmFunctionManager = WasmFunctionManager::getInstance();
  wasmFunctionManager.RegisterFunction("gcd","main",WatBase64Str);
  auto result =  wasmFunctionManager.run("gcd",{6, 27});
  std::cout << result[0] << "\n";
}  // namespace graph
}}