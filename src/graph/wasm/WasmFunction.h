//
// Created by arcosx on 12/14/21.
//

#ifndef NEBULA_GRAPH_WASMFUNCTION_H
#define NEBULA_GRAPH_WASMFUNCTION_H


#include <wasmtime.hh>


class WasmFunction
{
 private:
  wasmtime::Engine *engine;
  wasmtime::Store *store;
  /* data */
 public:
  WasmFunction();
  ~WasmFunction();
  void runWatFile(const std::string &fileName) const;
  void runWat(const std::string &watString) const;

};

#endif  // NEBULA_GRAPH_WASMFUNCTION_H
