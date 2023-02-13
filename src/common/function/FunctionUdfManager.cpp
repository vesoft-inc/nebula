/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "FunctionUdfManager.h"

#include <dirent.h>
#include <dlfcn.h>

#include <cstring>
#include <iostream>

#include "graph/service/GraphFlags.h"

DEFINE_string(udf_path, "lib/udf", "path to hold the udf");

namespace nebula {

static const char *dlsym_error;
static std::unordered_map<std::string, Value::Type> udfFunReturnType_;
static std::unordered_map<std::string, std::vector<std::vector<nebula::Value::Type>>>
    udfFunInputType_;
std::unordered_map<std::string, FunctionManager::FunctionAttributes> udfFunctions_;

std::atomic<bool> expired_{};
std::atomic<bool> try_to_expire_{};
std::mutex mutex_;
std::condition_variable expired_cond_;

FunctionUdfManager &FunctionUdfManager::instance() {
  static FunctionUdfManager instance;
  return instance;
}

std::vector<std::string> getFilesList(const std::string &path, const char *ftype) {
  std::vector<std::string> filenames;
  DIR *pDir;
  struct dirent *ptr;
  if (!(pDir = opendir(path.c_str()))) {
    LOG(ERROR) << "UDF Folder doesn't Exist!" << dlsym_error;
    return filenames;
  }
  while ((ptr = readdir(pDir)) != 0) {
    if (strcmp(ptr->d_name, ".") != 0 && strcmp(ptr->d_name, "..") != 0 &&
        strcmp((ptr->d_name) + strlen(ptr->d_name) - strlen(ftype), ftype) == 0) {
      filenames.emplace_back(ptr->d_name);
      LOG(INFO) << "Load UDF SO Name: " << ptr->d_name;
    }
  }
  closedir(pDir);
  return filenames;
}

FunctionUdfManager::create_f *FunctionUdfManager::getGraphFunctionClass(void *func_handle) {
  auto *create_func = reinterpret_cast<create_f *>(dlsym(func_handle, "create"));
  dlsym_error = dlerror();
  if (dlsym_error) {
    LOG(ERROR) << "Cannot load symbol create: " << dlsym_error;
  }
  return create_func;
}

FunctionUdfManager::destroy_f *FunctionUdfManager::deleteGraphFunctionClass(void *func_handle) {
  auto *destroy_func = reinterpret_cast<destroy_f *>(dlsym(func_handle, "destroy"));
  dlsym_error = dlerror();
  if (dlsym_error) {
    LOG(ERROR) << "Cannot load symbol destroy: " << dlsym_error;
  }
  return destroy_func;
}

FunctionUdfManager::FunctionUdfManager() {
  initAndLoadSoFunction();
  expired_ = true;
  try_to_expire_ = false;

  std::thread([this]() {
    while (!try_to_expire_) {
      std::this_thread::sleep_for(std::chrono::seconds(300));
      initAndLoadSoFunction();
    }
    {
      std::lock_guard<std::mutex> locker(mutex_);
      expired_ = true;
      expired_cond_.notify_one();
    }
  }).detach();
}

void FunctionUdfManager::initAndLoadSoFunction() {
  auto udfPath = FLAGS_udf_path;
  LOG(INFO) << "Load UDF so library: " << udfPath;
  std::vector<std::string> files = getFilesList(udfPath, ".so");

  for (auto &file : files) {
    const std::string &path = udfPath;
    std::string so_path_string = path + file;
    const char *soPath = so_path_string.c_str();
    try {
      void *func_handle = dlopen(soPath, RTLD_LAZY);
      if (!func_handle) {
        LOG(ERROR) << "Cannot load udf library: " << dlerror();
      }
      dlerror();

      create_f *create_func = getGraphFunctionClass(func_handle);
      destroy_f *destroy_func = deleteGraphFunctionClass(func_handle);
      if (create_func == nullptr || destroy_func == nullptr) {
        LOG(ERROR) << "GraphFunction Create Or Destroy Error: " << soPath;
        break;
      }

      GraphFunction *gf = create_func();
      char *funName = gf->name();
      udfFunInputType_.emplace(funName, gf->inputType());
      udfFunReturnType_.emplace(funName, gf->returnType());
      addSoUdfFunction(funName, soPath, gf->minArity(), gf->maxArity(), gf->isPure());

      destroy_func(gf);
      dlclose(func_handle);
    } catch (...) {
      LOG(ERROR) << "load So library Error: " << soPath;
    }
  }
}

StatusOr<Value::Type> FunctionUdfManager::getUdfReturnType(
    std::string func, const std::vector<Value::Type> &argsType) {
  if (udfFunReturnType_.find(func) != udfFunReturnType_.end()) {
    if (udfFunInputType_.find(func) != udfFunInputType_.end()) {
      auto iter = udfFunInputType_.find(func);
      for (const auto &args : iter->second) {
        if (argsType == args || args[0] == Value::Type::NULLVALUE ||
            args[0] == Value::Type::__EMPTY__) {
          return udfFunReturnType_[func];
        }
      }
    }
    return Status::Error("Parameter's type error");
  }
  return Status::Error("Function `%s' not defined", func.c_str());
}

StatusOr<const FunctionManager::FunctionAttributes> nebula::FunctionUdfManager::loadUdfFunction(
    std::string func, size_t arity) {
  auto iter = udfFunctions_.find(func);
  if (iter == udfFunctions_.end()) {
    return Status::Error("Function `%s' not defined", func.c_str());
  }
  auto minArity = iter->second.minArity_;
  auto maxArity = iter->second.maxArity_;
  if (arity < minArity || arity > maxArity) {
    if (minArity == maxArity) {
      return Status::Error(
          "Arity not match for function `%s': "
          "provided %lu but %lu expected.",
          func.c_str(),
          arity,
          minArity);
    } else {
      return Status::Error(
          "Arity not match for function `%s': "
          "provided %lu but %lu-%lu expected.",
          func.c_str(),
          arity,
          minArity,
          maxArity);
    }
  }
  return iter->second;
}

void FunctionUdfManager::addSoUdfFunction(
    char *funName, const char *soPath, size_t minArity, size_t maxArity, bool isPure) {
  auto &attr = udfFunctions_[funName];
  attr.minArity_ = minArity;
  attr.maxArity_ = maxArity;
  attr.isAlwaysPure_ = isPure;
  std::string path = soPath;
  attr.body_ = [path](const auto &args) -> Value {
    try {
      char *soPath2 = const_cast<char *>(path.c_str());
      void *func_handle = dlopen(soPath2, RTLD_LAZY);
      if (!func_handle) {
        LOG(ERROR) << "Cannot load udf library: " << dlerror();
      }
      dlerror();

      create_f *create_func = getGraphFunctionClass(func_handle);
      destroy_f *destroy_func = deleteGraphFunctionClass(func_handle);

      GraphFunction *gf = create_func();
      Value res = gf->body(args);
      destroy_func(gf);
      dlclose(func_handle);
      return res;
    } catch (...) {
      return Value::kNullBadData;
    }
  };
}

}  // namespace nebula
