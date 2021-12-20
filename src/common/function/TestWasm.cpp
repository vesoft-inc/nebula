//
// Created by arcosx on 12/14/21.
//
#include <wasmedge/wasmedge.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <wasmtime.hh>
#include <fstream>

using namespace wasmtime;

std::string readFile(const char* name) {
  std::ifstream watFile;
  watFile.open(name);
  std::stringstream strStream;
  strStream << watFile.rdbuf();
  return strStream.str();
}

int main() {
//   Load our WebAssembly (parsed WAT in our case), and then load it into a
//   `Module` which is attached to a `Store`. After we've got that we
//   can instantiate it.
  Engine engine;
  Store store(engine);
  auto WatStr = "(module\n"
      "  (func $gcd (param i32 i32) (result i32)\n"
      "    (local i32)\n"
      "    block  ;; label = @1\n"
      "      block  ;; label = @2\n"
      "        local.get 0\n"
      "        br_if 0 (;@2;)\n"
      "        local.get 1\n"
      "        local.set 2\n"
      "        br 1 (;@1;)\n"
      "      end\n"
      "      loop  ;; label = @2\n"
      "        local.get 1\n"
      "        local.get 0\n"
      "        local.tee 2\n"
      "        i32.rem_u\n"
      "        local.set 0\n"
      "        local.get 2\n"
      "        local.set 1\n"
      "        local.get 0\n"
      "        br_if 0 (;@2;)\n"
      "      end\n"
      "    end\n"
      "    local.get 2\n"
      "  )\n"
      "  (export \"gcd\" (func $gcd))\n"
      ")";
  auto module = Module::compile(engine,WatStr).unwrap();
  auto instance = Instance::create(store, module, {}).unwrap();

  // Invoke `gcd` export
  auto gcd = std::get<Func>(*instance.get(store, "gcd"));
  auto results = gcd.call(store, {6, 27}).unwrap();

  std::cout << "gcd(6, 27) = " << results[0].i32() << "\n";

  WasmEdge_ConfigureContext *ConfCxt = WasmEdge_ConfigureCreate();
  WasmEdge_ConfigureAddHostRegistration(ConfCxt, WasmEdge_HostRegistration_Wasi);
  /* 创建VM的时候可以提供空的配置。*/
  WasmEdge_VMContext *VMCxt = WasmEdge_VMCreate(ConfCxt, NULL);

  /* 参数以及返回的数组。 */
  WasmEdge_Value Params[2] = { WasmEdge_ValueGenI32(23),WasmEdge_ValueGenI32(456) };
  WasmEdge_Value Returns[1];
  /* 要调用的函数名。 */

  std::ifstream myfile ("/home/arcosx/Downloads/test.wasm");
  std::string str((std::istreambuf_iterator<char>(myfile)),
                  std::istreambuf_iterator<char>());
  std::vector<uint8_t> myVector(str.begin(), str.end());
  uint8_t fuck[myVector.size()];
  std::copy(myVector.begin(), myVector.end(), fuck);
//  uint8_t *p = &myVector[0];
  WasmEdge_String FuncName = WasmEdge_StringCreateByCString("addTwo");
  /* 运行文件里的 WASM 函数。 */
//  WasmEdge_Result Res = WasmEdge_VMRunWasmFromFile(VMCxt, "/home/arcosx/Downloads/test.wasm", FuncName, Params, 2, Returns, 1);
  WasmEdge_Result Res = WasmEdge_VMRunWasmFromBuffer(VMCxt, fuck,sizeof(fuck), FuncName, Params, 2, Returns, 1);


  if (WasmEdge_ResultOK(Res)) {
    printf("Get result: %d\n", WasmEdge_ValueGetI32(Returns[0]));
    //      printf("Get result: %d\n", WasmEdge_ValueGetI32(Returns[1]));
  } else {
    printf("Error message: %s\n", WasmEdge_ResultGetMessage(Res));
  }

  /* 资源析构。 */
  WasmEdge_VMDelete(VMCxt);
  WasmEdge_ConfigureDelete(ConfCxt);
  WasmEdge_StringDelete(FuncName);
  return 0;
}
