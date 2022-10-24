/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "FunctionManager.h"

#include <folly/json.h>

#include <boost/algorithm/string/replace.hpp>

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Geography.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Vertex.h"
#include "common/expression/Expression.h"
#include "common/geo/GeoFunction.h"
#include "common/geo/io/wkb/WKBReader.h"
#include "common/geo/io/wkb/WKBWriter.h"
#include "common/geo/io/wkt/WKTReader.h"
#include "common/geo/io/wkt/WKTWriter.h"
#include "common/thrift/ThriftTypes.h"
#include "common/time/TimeUtils.h"
#include "common/time/WallClock.h"

namespace nebula {

// static
FunctionManager &FunctionManager::instance() {
  static FunctionManager instance;
  return instance;
}

std::unordered_map<std::string, Value::Type> FunctionManager::variadicFunReturnType_ = {
    {"concat", Value::Type::STRING},
    {"concat_ws", Value::Type::STRING},
    {"cos_similarity", Value::Type::FLOAT},
    {"coalesce", Value::Type::__EMPTY__},
};

std::unordered_map<std::string, std::vector<TypeSignature>> FunctionManager::typeSignature_ = {
    {"abs",
     {TypeSignature({Value::Type::INT}, Value::Type::INT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"bit_and", {TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::INT)}},
    {"bit_or", {TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::INT)}},
    {"bit_xor", {TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::INT)}},
    {"floor",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"ceil",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"round",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT, Value::Type::INT}, Value::Type::FLOAT)}},
    {"sqrt",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"cbrt",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"hypot",
     {TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::INT, Value::Type::FLOAT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT, Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT, Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"pow",
     {TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::INT),
      TypeSignature({Value::Type::INT, Value::Type::FLOAT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT, Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT, Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"e", {TypeSignature({}, Value::Type::FLOAT)}},
    {"exp",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"exp2",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"log",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"log2",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"log10",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"pi", {TypeSignature({}, Value::Type::FLOAT)}},
    {"radians",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"sin",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"cos",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"asin",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"acos",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"tan",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"atan",
     {TypeSignature({Value::Type::INT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT)}},
    {"sign",
     {TypeSignature({Value::Type::INT}, Value::Type::INT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::INT)}},
    {"rand", {TypeSignature({}, Value::Type::FLOAT)}},
    {"rand32",
     {TypeSignature({}, Value::Type::INT),
      TypeSignature({Value::Type::INT}, Value::Type::INT),
      TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::INT)}},
    {"rand64",
     {TypeSignature({}, Value::Type::INT),
      TypeSignature({Value::Type::INT}, Value::Type::INT),
      TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::INT)}},
    {"now", {TypeSignature({}, Value::Type::INT)}},
    {"strcasecmp",
     {TypeSignature({Value::Type::STRING, Value::Type::STRING}, Value::Type::STRING)}},
    {"lower", {TypeSignature({Value::Type::STRING}, Value::Type::STRING)}},
    {"tolower", {TypeSignature({Value::Type::STRING}, Value::Type::STRING)}},
    {"upper", {TypeSignature({Value::Type::STRING}, Value::Type::STRING)}},
    {"toupper", {TypeSignature({Value::Type::STRING}, Value::Type::STRING)}},
    {"length",
     {
         TypeSignature({Value::Type::STRING}, Value::Type::INT),
         TypeSignature({Value::Type::PATH}, Value::Type::INT),
     }},
    {"trim", {TypeSignature({Value::Type::STRING}, Value::Type::STRING)}},
    {"ltrim", {TypeSignature({Value::Type::STRING}, Value::Type::STRING)}},
    {"rtrim", {TypeSignature({Value::Type::STRING}, Value::Type::STRING)}},
    {"left", {TypeSignature({Value::Type::STRING, Value::Type::INT}, Value::Type::STRING)}},
    {"right", {TypeSignature({Value::Type::STRING, Value::Type::INT}, Value::Type::STRING)}},
    {"replace",
     {TypeSignature({Value::Type::STRING, Value::Type::STRING, Value::Type::STRING},
                    Value::Type::STRING)}},
    {"reverse",
     {TypeSignature({Value::Type::STRING}, Value::Type::STRING),
      TypeSignature({Value::Type::LIST}, Value::Type::LIST)}},
    {"split", {TypeSignature({Value::Type::STRING, Value::Type::STRING}, Value::Type::LIST)}},
    {"lpad",
     {TypeSignature({Value::Type::STRING, Value::Type::INT, Value::Type::STRING},
                    Value::Type::STRING)}},
    {"rpad",
     {TypeSignature({Value::Type::STRING, Value::Type::INT, Value::Type::STRING},
                    Value::Type::STRING)}},
    {"substr",
     {TypeSignature({Value::Type::STRING, Value::Type::INT, Value::Type::INT}, Value::Type::STRING),
      TypeSignature({Value::Type::STRING, Value::Type::INT}, Value::Type::STRING)}},
    {"substring",
     {TypeSignature({Value::Type::STRING, Value::Type::INT, Value::Type::INT}, Value::Type::STRING),
      TypeSignature({Value::Type::STRING, Value::Type::INT}, Value::Type::STRING)}},
    {"tostring",
     {TypeSignature({Value::Type::INT}, Value::Type::STRING),
      TypeSignature({Value::Type::FLOAT}, Value::Type::STRING),
      TypeSignature({Value::Type::STRING}, Value::Type::STRING),
      TypeSignature({Value::Type::BOOL}, Value::Type::STRING),
      TypeSignature({Value::Type::DATE}, Value::Type::STRING),
      TypeSignature({Value::Type::TIME}, Value::Type::STRING),
      TypeSignature({Value::Type::DATETIME}, Value::Type::STRING)}},
    {"toboolean",
     {TypeSignature({Value::Type::STRING}, Value::Type::BOOL),
      TypeSignature({Value::Type::STRING}, Value::Type::NULLVALUE),
      TypeSignature({Value::Type::BOOL}, Value::Type::BOOL)}},
    {"tofloat",
     {TypeSignature({Value::Type::STRING}, Value::Type::FLOAT),
      TypeSignature({Value::Type::STRING}, Value::Type::NULLVALUE),
      TypeSignature({Value::Type::FLOAT}, Value::Type::FLOAT),
      TypeSignature({Value::Type::INT}, Value::Type::FLOAT)}},
    {"tointeger",
     {TypeSignature({Value::Type::STRING}, Value::Type::INT),
      TypeSignature({Value::Type::STRING}, Value::Type::NULLVALUE),
      TypeSignature({Value::Type::FLOAT}, Value::Type::INT),
      TypeSignature({Value::Type::INT}, Value::Type::INT)}},
    {"toset",
     {TypeSignature({Value::Type::LIST}, Value::Type::SET),
      TypeSignature({Value::Type::SET}, Value::Type::SET)}},
    {"hash",
     {TypeSignature({Value::Type::INT}, Value::Type::INT),
      TypeSignature({Value::Type::FLOAT}, Value::Type::INT),
      TypeSignature({Value::Type::STRING}, Value::Type::INT),
      TypeSignature({Value::Type::NULLVALUE}, Value::Type::INT),
      TypeSignature({Value::Type::__EMPTY__}, Value::Type::INT),
      TypeSignature({Value::Type::BOOL}, Value::Type::INT),
      TypeSignature({Value::Type::DATE}, Value::Type::INT),
      TypeSignature({Value::Type::DATETIME}, Value::Type::INT),
      TypeSignature({Value::Type::PATH}, Value::Type::INT),
      TypeSignature({Value::Type::VERTEX}, Value::Type::INT),
      TypeSignature({Value::Type::EDGE}, Value::Type::INT),
      TypeSignature({Value::Type::LIST}, Value::Type::INT)}},
    {"size",
     {
         TypeSignature({Value::Type::STRING}, Value::Type::INT),
         TypeSignature({Value::Type::NULLVALUE}, Value::Type::NULLVALUE),
         TypeSignature({Value::Type::__EMPTY__}, Value::Type::__EMPTY__),
         TypeSignature({Value::Type::LIST}, Value::Type::INT),
         TypeSignature({Value::Type::MAP}, Value::Type::INT),
         TypeSignature({Value::Type::SET}, Value::Type::INT),
         TypeSignature({Value::Type::DATASET}, Value::Type::INT),
     }},
    {"time",
     {TypeSignature({}, Value::Type::TIME),
      TypeSignature({Value::Type::STRING}, Value::Type::TIME),
      TypeSignature({Value::Type::MAP}, Value::Type::TIME)}},
    {"date",
     {TypeSignature({}, Value::Type::DATE),
      TypeSignature({Value::Type::STRING}, Value::Type::DATE),
      TypeSignature({Value::Type::MAP}, Value::Type::DATE)}},
    {"datetime",
     {TypeSignature({}, Value::Type::DATETIME),
      TypeSignature({Value::Type::STRING}, Value::Type::DATETIME),
      TypeSignature({Value::Type::MAP}, Value::Type::DATETIME),
      TypeSignature({Value::Type::INT}, Value::Type::DATETIME)}},
    {"timestamp",
     {TypeSignature({}, Value::Type::INT),
      TypeSignature({Value::Type::STRING}, Value::Type::INT),
      TypeSignature({Value::Type::INT}, Value::Type::INT),
      TypeSignature({Value::Type::DATETIME}, Value::Type::INT)}},
    {"tags",
     {
         TypeSignature({Value::Type::VERTEX}, Value::Type::LIST),
     }},
    {"labels",
     {
         TypeSignature({Value::Type::VERTEX}, Value::Type::LIST),
     }},
    {"properties",
     {
         TypeSignature({Value::Type::VERTEX}, Value::Type::MAP),
         TypeSignature({Value::Type::EDGE}, Value::Type::MAP),
         TypeSignature({Value::Type::MAP}, Value::Type::MAP),
     }},
    {"type",
     {
         TypeSignature({Value::Type::EDGE}, Value::Type::STRING),
     }},
    {"typeid",
     {
         TypeSignature({Value::Type::EDGE}, Value::Type::INT),
     }},
    {"rank",
     {
         TypeSignature({Value::Type::EDGE}, Value::Type::INT),
     }},
    {"startnode",
     {
         TypeSignature({Value::Type::EDGE}, Value::Type::VERTEX),
         TypeSignature({Value::Type::PATH}, Value::Type::VERTEX),
     }},
    {"endnode",
     {
         TypeSignature({Value::Type::EDGE}, Value::Type::VERTEX),
         TypeSignature({Value::Type::PATH}, Value::Type::VERTEX),
     }},
    {"keys",
     {
         TypeSignature({Value::Type::VERTEX}, Value::Type::LIST),
         TypeSignature({Value::Type::EDGE}, Value::Type::LIST),
         TypeSignature({Value::Type::MAP}, Value::Type::LIST),
     }},
    {"nodes",
     {
         TypeSignature({Value::Type::PATH}, Value::Type::LIST),
     }},
    {"tail",
     {
         TypeSignature({Value::Type::LIST}, Value::Type::LIST),
     }},
    {"relationships",
     {
         TypeSignature({Value::Type::PATH}, Value::Type::LIST),
     }},
    {"head",
     {
         TypeSignature({Value::Type::LIST}, Value::Type::__EMPTY__),
     }},
    {"last",
     {
         TypeSignature({Value::Type::LIST}, Value::Type::__EMPTY__),
     }},
    {"range",
     {TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::LIST),
      TypeSignature({Value::Type::INT, Value::Type::INT, Value::Type::INT}, Value::Type::LIST)}},
    {"hassameedgeinpath",
     {
         TypeSignature({Value::Type::PATH}, Value::Type::BOOL),
     }},
    {"hassamevertexinpath",
     {
         TypeSignature({Value::Type::PATH}, Value::Type::BOOL),
     }},
    {"reversepath",
     {
         TypeSignature({Value::Type::PATH}, Value::Type::PATH),
     }},
    {"datasetrowcol",
     {
         TypeSignature({Value::Type::DATASET, Value::Type::INT, Value::Type::INT},
                       Value::Type::__EMPTY__),
         TypeSignature({Value::Type::DATASET, Value::Type::INT, Value::Type::STRING},
                       Value::Type::__EMPTY__),
     }},
    // These geo functions of the ST prefix follow the Simple Feature Access and SQL/MM
    // specification. See https://www.ogc.org/standards/sfa, https://www.ogc.org/standards/sfs, and
    // https://www.researchgate.net/publication/221323544_SQLMM_Spatial_-_The_Standard_to_Manage_Spatial_Data_in_a_Relational_Database_System
    // geo constructors
    {"st_point",
     {
         TypeSignature({Value::Type::FLOAT, Value::Type::FLOAT}, Value::Type::GEOGRAPHY),
         TypeSignature({Value::Type::INT, Value::Type::INT}, Value::Type::GEOGRAPHY),
         TypeSignature({Value::Type::FLOAT, Value::Type::INT}, Value::Type::GEOGRAPHY),
         TypeSignature({Value::Type::INT, Value::Type::FLOAT}, Value::Type::GEOGRAPHY),
     }},
    // geo parsers
    {"st_geogfromtext",
     {
         TypeSignature({Value::Type::STRING}, Value::Type::GEOGRAPHY),
     }},
    // This function requires binary data to be support first.
    // {"st_geogfromwkb",
    //  {
    //      TypeSignature({Value::Type::STRING}, Value::Type::GEOGRAPHY),
    //  }},
    // geo formatters
    {"st_astext",
     {
         TypeSignature({Value::Type::GEOGRAPHY}, Value::Type::STRING),
     }},
    // This function requires binary data to be support first.
    // {"st_asbinary",
    //  {
    //      TypeSignature({Value::Type::GEOGRAPHY}, Value::Type::STRING),
    //  }},
    // geo transformations
    {"st_centroid",
     {
         TypeSignature({Value::Type::GEOGRAPHY}, Value::Type::GEOGRAPHY),
     }},
    // geo accessors
    {"st_isvalid",
     {
         TypeSignature({Value::Type::GEOGRAPHY}, Value::Type::BOOL),
     }},
    // TODO(jie) The geo predicates should follow the DE-9IM model. See
    // https://en.wikipedia.org/wiki/DE-9IM
    // geo predicates
    {"st_intersects",
     {
         TypeSignature({Value::Type::GEOGRAPHY, Value::Type::GEOGRAPHY}, Value::Type::BOOL),
     }},
    {"st_covers",
     {
         TypeSignature({Value::Type::GEOGRAPHY, Value::Type::GEOGRAPHY}, Value::Type::BOOL),
     }},
    {"st_coveredby",
     {
         TypeSignature({Value::Type::GEOGRAPHY, Value::Type::GEOGRAPHY}, Value::Type::BOOL),
     }},
    {"st_dwithin",
     {
         TypeSignature({Value::Type::GEOGRAPHY, Value::Type::GEOGRAPHY, Value::Type::FLOAT},
                       Value::Type::BOOL),
         TypeSignature({Value::Type::GEOGRAPHY, Value::Type::GEOGRAPHY, Value::Type::INT},
                       Value::Type::BOOL),
         TypeSignature({Value::Type::GEOGRAPHY,
                        Value::Type::GEOGRAPHY,
                        Value::Type::FLOAT,
                        Value::Type::BOOL},
                       Value::Type::BOOL),
         TypeSignature(
             {Value::Type::GEOGRAPHY, Value::Type::GEOGRAPHY, Value::Type::INT, Value::Type::BOOL},
             Value::Type::BOOL),
     }},
    // geo measures
    {"st_distance",
     {
         TypeSignature({Value::Type::GEOGRAPHY, Value::Type::GEOGRAPHY}, Value::Type::FLOAT),
     }},
    // geo s2 functions
    {"s2_cellidfrompoint",
     {
         TypeSignature({Value::Type::GEOGRAPHY}, Value::Type::INT),
         TypeSignature({Value::Type::GEOGRAPHY, Value::Type::INT}, Value::Type::INT),
     }},
    {"s2_coveringcellids",
     {
         TypeSignature({Value::Type::GEOGRAPHY}, Value::Type::LIST),
         TypeSignature({Value::Type::GEOGRAPHY, Value::Type::INT}, Value::Type::LIST),
         TypeSignature({Value::Type::GEOGRAPHY, Value::Type::INT, Value::Type::INT},
                       Value::Type::LIST),
         TypeSignature(
             {Value::Type::GEOGRAPHY, Value::Type::INT, Value::Type::INT, Value::Type::INT},
             Value::Type::LIST),
         TypeSignature({Value::Type::GEOGRAPHY,
                        Value::Type::INT,
                        Value::Type::INT,
                        Value::Type::INT,
                        Value::Type::INT},
                       Value::Type::LIST),
         TypeSignature({Value::Type::GEOGRAPHY,
                        Value::Type::INT,
                        Value::Type::INT,
                        Value::Type::INT,
                        Value::Type::FLOAT},
                       Value::Type::LIST),
     }},
    {"is_edge", {TypeSignature({Value::Type::EDGE}, Value::Type::BOOL)}},
    {"duration",
     {TypeSignature({Value::Type::STRING}, Value::Type::DURATION),
      TypeSignature({Value::Type::MAP}, Value::Type::DURATION)}},
    {"extract", {TypeSignature({Value::Type::STRING, Value::Type::STRING}, Value::Type::LIST)}},
    {"_nodeid", {TypeSignature({Value::Type::PATH, Value::Type::INT}, Value::Type::INT)}},
    {"json_extract",
     {TypeSignature({Value::Type::STRING}, Value::Type::MAP),
      TypeSignature({Value::Type::STRING}, Value::Type::NULLVALUE)}},
};

// static
StatusOr<Value::Type> FunctionManager::getReturnType(const std::string &funcName,
                                                     const std::vector<Value::Type> &argsType) {
  auto func = funcName;
  std::transform(func.begin(), func.end(), func.begin(), ::tolower);
  if (variadicFunReturnType_.find(func) != variadicFunReturnType_.end()) {
    return variadicFunReturnType_[func];
  }
  auto iter = typeSignature_.find(func);
  if (iter == typeSignature_.end()) {
    return Status::Error("Function `%s' not defined", funcName.c_str());
  }

  for (const auto &args : iter->second) {
    if (argsType == args.argsType_) {
      return args.returnType_;
    }
  }

  for (auto &argType : argsType) {
    // Most functions do not accept NULL or EMPTY
    // but if the parameters are given by NULL or EMPTY ,
    // then we will tell that it returns NULL or EMPTY
    if (argType == Value::Type::__EMPTY__ || argType == Value::Type::NULLVALUE) {
      return argType;
    }
  }

  return Status::Error("Parameter's type error");
}

FunctionManager::FunctionManager() {
  {
    // absolute value
    auto &attr = functions_["abs"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::abs(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::abs(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["bit_and"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value { return args[0].get() & args[1].get(); };
  }
  {
    auto &attr = functions_["bit_or"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value { return args[0].get() | args[1].get(); };
  }
  {
    auto &attr = functions_["bit_xor"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value { return args[0].get() ^ args[1].get(); };
  }
  {
    // to nearest integer value not greater than x
    auto &attr = functions_["floor"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::floor(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::floor(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // returns the smallest floating point number that is not less than x
    auto &attr = functions_["ceil"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::ceil(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::ceil(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // to nearest integral (as a floating-point value)
    auto &attr = functions_["round"];
    attr.minArity_ = 1;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT:
        case Value::Type::FLOAT: {
          if (args.size() == 2) {
            if (args[1].get().type() == Value::Type::INT) {
              auto decimal = args[1].get().getInt();
              return std::round(args[0].get().getFloat() * pow(10, decimal)) / pow(10, decimal);
            } else {
              return Value::kNullBadType;
            }
          }
          return std::round(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // square root
    auto &attr = functions_["sqrt"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          auto val = args[0].get().getInt();
          if (val < 0) {
            return Value::kNullValue;
          }
          return std::sqrt(val);
        }
        case Value::Type::FLOAT: {
          auto val = args[0].get().getFloat();
          if (val < 0) {
            return Value::kNullValue;
          }
          return std::sqrt(val);
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // cubic root
    auto &attr = functions_["cbrt"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::cbrt(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::cbrt(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // sqrt(x^2 + y^2)
    auto &attr = functions_["hypot"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (args[0].get().isNumeric() && args[1].get().isNumeric()) {
        auto x = args[0].get().isInt() ? args[0].get().getInt() : args[0].get().getFloat();
        auto y = args[1].get().isInt() ? args[1].get().getInt() : args[1].get().getFloat();
        return std::hypot(x, y);
      }
      return Value::kNullBadType;
    };
  }
  {
    // base^exp
    auto &attr = functions_["pow"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (args[0].get().isNumeric() && args[1].get().isNumeric()) {
        auto base = args[0].get().isInt() ? args[0].get().getInt() : args[0].get().getFloat();
        auto exp = args[1].get().isInt() ? args[1].get().getInt() : args[1].get().getFloat();
        auto val = std::pow(base, exp);
        if (args[0].get().isInt() && args[1].get().isInt()) {
          return static_cast<int64_t>(val);
        }
        return val;
      }
      return Value::kNullBadType;
    };
  }
  {
    // return the base of natural logarithm e
    auto &attr = functions_["e"];
    attr.minArity_ = 0;
    attr.maxArity_ = 0;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      UNUSED(args);
      return M_E;
    };
  }
  {
    // e^x
    auto &attr = functions_["exp"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::exp(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::exp(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // 2^x
    auto &attr = functions_["exp2"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::exp2(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::exp2(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // e-based logarithm
    auto &attr = functions_["log"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          auto val = args[0].get().getInt();
          if (val == 0) {
            return Value::kNullValue;
          }
          return std::log(val);
        }
        case Value::Type::FLOAT: {
          auto val = args[0].get().getFloat();
          if (val >= -kEpsilon && val <= kEpsilon) {
            return Value::kNullValue;
          }
          return std::log(val);
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // 2-based logarithm
    auto &attr = functions_["log2"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          auto val = args[0].get().getInt();
          if (val == 0) {
            return Value::kNullValue;
          }
          return std::log2(val);
        }
        case Value::Type::FLOAT: {
          auto val = args[0].get().getFloat();
          if (val >= -kEpsilon && val <= kEpsilon) {
            return Value::kNullValue;
          }
          return std::log2(val);
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // 10-based logarithm
    auto &attr = functions_["log10"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          auto val = args[0].get().getInt();
          if (val == 0) {
            return Value::kNullValue;
          }
          return std::log10(val);
        }
        case Value::Type::FLOAT: {
          auto val = args[0].get().getFloat();
          if (val >= -kEpsilon && val <= kEpsilon) {
            return Value::kNullValue;
          }
          return std::log10(val);
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    // return the mathematical constant PI
    auto &attr = functions_["pi"];
    attr.minArity_ = 0;
    attr.maxArity_ = 0;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      UNUSED(args);
      return M_PI;
    };
  }

  {
    auto &attr = functions_["radians"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT:
        case Value::Type::FLOAT: {
          return (args[0].get() * M_PI) / 180;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }

  {
    auto &attr = functions_["sin"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::sin(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::sin(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["asin"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::asin(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::asin(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["cos"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::cos(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::cos(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["acos"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::acos(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::acos(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["tan"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::tan(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::tan(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["atan"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          return std::atan(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          return std::atan(args[0].get().getFloat());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["sign"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::INT: {
          auto val = args[0].get().getInt();
          return val > 0 ? 1 : val < 0 ? -1 : 0;
        }
        case Value::Type::FLOAT: {
          auto val = args[0].get().getFloat();
          return val > 0 ? 1 : val < 0 ? -1 : 0;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["rand"];
    attr.minArity_ = 0;
    attr.maxArity_ = 0;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      UNUSED(args);
      return folly::Random::randDouble01();
    };
  }
  {
    // rand32(), rand32(max), rand32(min, max)
    auto &attr = functions_["rand32"];
    attr.minArity_ = 0;
    attr.maxArity_ = 2;
    setCompleteNonPure(attr);
    attr.body_ = [](const auto &args) -> Value {
      if (args.empty()) {
        auto value = folly::Random::rand32();
        return static_cast<int64_t>(static_cast<int32_t>(value));
      } else if (args.size() == 1UL) {
        if (args[0].get().isInt()) {
          auto max = args[0].get().getInt();
          if (max < 0 || max > std::numeric_limits<uint32_t>::max()) {
            return Value::kNullBadData;
          }
          auto value = folly::Random::rand32(max);
          return static_cast<int64_t>(static_cast<int32_t>(value));
        }
        return Value::kNullBadType;
      }
      DCHECK_EQ(2UL, args.size());
      if (args[0].get().isInt() && args[1].get().isInt()) {
        auto min = args[0].get().getInt();
        auto max = args[1].get().getInt();
        if (max < 0 || min < 0 || max > std::numeric_limits<uint32_t>::max() ||
            min > std::numeric_limits<uint32_t>::max()) {
          return Value::kNullBadData;
        }
        if (min >= max) {
          return Value::kNullBadData;
        }
        return static_cast<int64_t>(folly::Random::rand32(min, max));
      }
      return Value::kNullBadType;
    };
  }
  {
    // rand64(), rand64(max), rand64(min, max)
    auto &attr = functions_["rand64"];
    attr.minArity_ = 0;
    attr.maxArity_ = 2;
    setCompleteNonPure(attr);
    attr.body_ = [](const auto &args) -> Value {
      if (args.empty()) {
        return static_cast<int64_t>(folly::Random::rand64());
      } else if (args.size() == 1UL) {
        if (args[0].get().isInt()) {
          auto max = args[0].get().getInt();
          if (max < 0) {
            return Value::kNullBadData;
          }
          return static_cast<int64_t>(folly::Random::rand64(max));
        }
        return Value::kNullBadType;
      }
      DCHECK_EQ(2UL, args.size());
      if (args[0].get().isInt() && args[1].get().isInt()) {
        auto min = args[0].get().getInt();
        auto max = args[1].get().getInt();
        if (max < 0 || min < 0 || min >= max) {
          return Value::kNullBadData;
        }
        return static_cast<int64_t>(folly::Random::rand64(min, max));
      }
      return Value::kNullBadType;
    };
  }
  {
    // unix timestamp
    auto &attr = functions_["now"];
    attr.minArity_ = 0;
    attr.maxArity_ = 0;
    setCompleteNonPure(attr);
    attr.body_ = [](const auto &args) -> Value {
      UNUSED(args);
      return ::time(NULL);
    };
  }
  {
    auto &attr = functions_["strcasecmp"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (args[0].get().isStr() && args[1].get().isStr()) {
        return static_cast<int64_t>(
            ::strcasecmp(args[0].get().getStr().c_str(), args[1].get().getStr().c_str()));
      }
      return Value::kNullBadType;
    };
  }
  {
    auto &attr = functions_["lower"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          std::string value(args[0].get().getStr());
          folly::toLowerAscii(value);
          return value;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
    functions_["tolower"] = attr;
  }
  {
    auto &attr = functions_["upper"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          std::string value(args[0].get().getStr());
          std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
            return std::toupper(c);
          });
          return value;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
    functions_["toupper"] = attr;
  }
  {
    auto &attr = functions_["length"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          auto value = args[0].get().getStr();
          return static_cast<int64_t>(value.length());
        }
        case Value::Type::PATH: {
          auto path = args[0].get().getPath();
          return static_cast<int64_t>(path.steps.size());
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["trim"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          std::string value(args[0].get().getStr());
          return folly::trimWhitespace(value).toString();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["ltrim"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          std::string value(args[0].get().getStr());
          return folly::ltrimWhitespace(value).toString();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["rtrim"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          std::string value(args[0].get().getStr());
          return folly::rtrimWhitespace(value).toString();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["left"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          if (args[1].get().isNull() || !args[1].get().isInt()) {
            // opencypher raise an error
            return Value::kNullBadType;
          }
          auto len = args[1].get().getInt();
          if (len < 0) {
            // opencypher raise an error
            return Value::kNullBadType;
          }
          return args[0].get().getStr().substr(0, len);
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["right"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          if (args[1].get().isNull() || !args[1].get().isInt()) {
            // opencypher raise an error
            return Value::kNullBadType;
          }
          auto &value = args[0].get().getStr();
          auto len = args[1].get().getInt();
          if (len < 0) {
            // opencypher raise an error
            return Value::kNullBadType;
          }
          if (len > static_cast<int64_t>(value.size())) {
            len = value.size();
          }
          return value.substr(value.size() - len);
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["replace"];
    attr.minArity_ = 3;
    attr.maxArity_ = 3;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (args[0].get().isNull() || args[1].get().isNull() || args[2].get().isNull()) {
        return Value::kNullValue;
      }
      if (args[0].get().isStr() && args[1].get().isStr() && args[2].get().isStr()) {
        std::string origStr(args[0].get().getStr());
        std::string search(args[1].get().getStr());
        std::string newStr(args[2].get().getStr());
        return boost::replace_all_copy(origStr, search, newStr);
      }
      return Value::kNullBadType;
    };
  }
  {
    auto &attr = functions_["reverse"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          std::string origStr(args[0].get().getStr());
          std::reverse(origStr.begin(), origStr.end());
          return origStr;
        }
        case Value::Type::LIST: {
          auto &list = args[0].get().getList();
          List result(list.values);
          std::reverse(result.values.begin(), result.values.end());
          return result;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["split"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::STRING: {
          if (args[1].get().isNull()) {
            return Value::kNullValue;
          }
          if (!args[1].get().isStr()) {
            return Value::kNullBadType;
          }
          std::string origStr(args[0].get().getStr());
          std::string delim(args[1].get().getStr());
          List res;
          std::vector<folly::StringPiece> substrings;
          folly::split<folly::StringPiece>(delim, origStr, substrings);
          for (auto str : substrings) {
            res.emplace_back(str.toString());
          }
          return res;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["tostring"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE:
          return Value::kNullValue;
        case Value::Type::INT: {
          return folly::to<std::string>(args[0].get().getInt());
        }
        case Value::Type::FLOAT: {
          auto str = folly::to<std::string>(args[0].get().getFloat());
          std::size_t found = str.find('.');
          if (found == std::string::npos) {
            str += ".0";
          }
          return str;
        }
        case Value::Type::BOOL: {
          return args[0].get().getBool() ? "true" : "false";
        }
        case Value::Type::STRING: {
          return args[0].get().getStr();
        }
        case Value::Type::DATE: {
          return args[0].get().getDate().toString();
        }
        case Value::Type::TIME: {
          return args[0].get().getTime().toString();
        }
        case Value::Type::DATETIME: {
          return args[0].get().getDateTime().toString();
        }
        default:
          LOG(ERROR) << "toString has not been implemented for " << args[0].get().type();
          return Value::kNullBadType;
      }
    };
  }
  {
    auto &attr = functions_["toboolean"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value { return Value(args[0].get()).toBool(); };
  }
  {
    auto &attr = functions_["tofloat"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value { return Value(args[0].get()).toFloat(); };
  }
  {
    auto &attr = functions_["tointeger"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value { return Value(args[0].get()).toInt(); };
  }
  {
    auto &attr = functions_["toset"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value { return Value(args[0].get()).toSet(); };
  }
  {
    auto &attr = functions_["lpad"];
    attr.minArity_ = 3;
    attr.maxArity_ = 3;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (args[0].get().isStr() && args[1].get().isInt() && args[2].get().isStr()) {
        auto value = args[0].get().getStr();
        auto size = args[1].get().getInt();
        if (size < 0) {
          return std::string("");
        } else if (size < static_cast<int64_t>(value.size())) {
          return value.substr(0, static_cast<int32_t>(size));
        } else {
          auto extra = args[2].get().getStr();
          size -= value.size();
          std::stringstream stream;
          while (size > static_cast<int64_t>(extra.size())) {
            stream << extra;
            size -= extra.size();
          }
          stream << extra.substr(0, size);
          stream << value;
          return stream.str();
        }
      }
      return Value::kNullBadType;
    };
  }
  {
    auto &attr = functions_["rpad"];
    attr.minArity_ = 3;
    attr.maxArity_ = 3;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (args[0].get().isStr() && args[1].get().isInt() && args[2].get().isStr()) {
        auto value = args[0].get().getStr();
        if (args[1].get().getInt() < 0) {
          return "";
        }
        size_t size = args[1].get().getInt();
        if (size < value.size()) {
          return value.substr(0, static_cast<int32_t>(size));
        } else {
          auto extra = args[2].get().getStr();
          std::stringstream stream;
          stream << value;
          size -= value.size();
          while (size > extra.size()) {
            stream << extra;
            size -= extra.size();
          }
          stream << extra.substr(0, size);
          return stream.str();
        }
      }
      return Value::kNullBadType;
    };
  }
  {
    auto &attr = functions_["substr"];
    attr.minArity_ = 2;
    attr.maxArity_ = 3;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      auto argSize = args.size();
      if (args[0].get().isNull()) {
        return Value::kNullValue;
      }
      if (!args[0].get().isStr() || !args[1].get().isInt() ||
          (argSize == 3 && !args[2].get().isInt())) {
        return Value::kNullBadType;
      }
      auto value = args[0].get().getStr();
      auto start = args[1].get().getInt();
      auto length = 0;
      if (argSize == 3) {
        length = args[2].get().getInt();
      } else {
        length = static_cast<size_t>(start) >= value.size()
                     ? 0
                     : value.size() - static_cast<size_t>(start);
      }
      if (start < 0 || length < 0) {
        return Value::kNullBadData;
      }
      if (static_cast<size_t>(start) >= value.size() || length == 0) {
        return std::string("");
      }
      return value.substr(start, length);
    };
    functions_["substring"] = attr;
  }
  {
    // 64bit signed hash value
    auto &attr = functions_["hash"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE:
        case Value::Type::__EMPTY__:
        case Value::Type::INT:
        case Value::Type::FLOAT:
        case Value::Type::BOOL:
        case Value::Type::STRING:
        case Value::Type::DATE:
        case Value::Type::DATETIME:
        case Value::Type::VERTEX:
        case Value::Type::EDGE:
        case Value::Type::PATH:
        case Value::Type::LIST: {
          return static_cast<int64_t>(std::hash<nebula::Value>()(args[0].get()));
        }
        default:
          LOG(ERROR) << "Hash has not been implemented for " << args[0].get().type();
          return Value::kNullBadType;
      }
    };
  }
  {
    auto &attr = functions_["udf_is_in"];
    attr.minArity_ = 2;
    attr.maxArity_ = INT64_MAX;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      return std::find(args.begin() + 1, args.end(), args[0].get()) != args.end();
    };
  }
  {
    auto &attr = functions_["near"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      // auto result = geo::GeoFilter::near(args);
      // if (!result.ok()) {
      //     return std::string("");
      // } else {
      //     return std::move(result).value();
      // }
      // TODO
      UNUSED(args);
      return std::string("");
    };
  }
  {
    auto &attr = functions_["cos_similarity"];
    attr.minArity_ = 2;
    attr.maxArity_ = INT64_MAX;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (args.size() % 2 != 0) {
        LOG(ERROR) << "The number of arguments must be even.";
        // value range of cos is [-1, 1]
        // it means error when we return -2
        return static_cast<double>(-2);
      }
      // sum(xi * yi) / (sqrt(sum(pow(xi))) + sqrt(sum(pow(yi))))
      auto mid = args.size() / 2;
      double s1 = 0, s2 = 0, s3 = 0;
      for (decltype(args.size()) i = 0; i < mid; ++i) {
        if (args[i].get().isNumeric() && args[i + mid].get().isNumeric()) {
          auto xi = args[i].get().isInt() ? args[i].get().getInt() : args[i].get().getFloat();
          auto yi = args[i + mid].get().isInt() ? args[i + mid].get().getInt()
                                                : args[i + mid].get().getFloat();
          s1 += (xi * yi);
          s2 += (xi * xi);
          s3 += (yi * yi);
        } else {
          return Value::kNullBadType;
        }
      }
      if (std::abs(s2) <= kEpsilon || std::abs(s3) <= kEpsilon) {
        return static_cast<double>(-2);
      } else {
        return s1 / (std::sqrt(s2) * std::sqrt(s3));
      }
    };
  }
  {
    auto &attr = functions_["size"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE:
          return Value::kNullValue;
        case Value::Type::__EMPTY__:
          return Value::kEmpty;
        case Value::Type::STRING:
          return static_cast<int64_t>(args[0].get().getStr().size());
        case Value::Type::LIST:
          return static_cast<int64_t>(args[0].get().getList().size());
        case Value::Type::MAP:
          return static_cast<int64_t>(args[0].get().getMap().size());
        case Value::Type::SET:
          return static_cast<int64_t>(args[0].get().getSet().size());
        case Value::Type::DATASET:
          return static_cast<int64_t>(args[0].get().getDataSet().size());
        case Value::Type::INT:
        case Value::Type::FLOAT:
        case Value::Type::BOOL:
        case Value::Type::DATE:
        case Value::Type::DATETIME:
        case Value::Type::VERTEX:
        case Value::Type::EDGE:
        case Value::Type::PATH:
        default:
          LOG(ERROR) << "size() has not been implemented for " << args[0].get().type();
          return Value::kNullBadType;
      }
    };
  }
  {
    auto &attr = functions_["date"];
    // 0 for current time
    // 1 for string or map
    attr.minArity_ = 0;
    attr.maxArity_ = 1;
    attr.isPure_[0] = false;
    attr.isPure_[1] = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args.size()) {
        case 0: {
          auto result = time::TimeUtils::utcDate();
          if (!result.ok()) {
            return Value::kNullBadData;
          }
          return Value(std::move(result).value());
        }
        case 1: {
          if (args[0].get().isStr()) {
            auto result = time::TimeUtils::parseDate(args[0].get().getStr());
            if (!result.ok()) {
              return Value::kNullBadData;
            }
            return result.value();
          } else if (args[0].get().isMap()) {
            auto result = time::TimeUtils::dateFromMap(args[0].get().getMap());
            if (!result.ok()) {
              return Value::kNullBadData;
            }
            return result.value();
          } else {
            return Value::kNullBadType;
          }
        }
        default:
          LOG(FATAL) << "Unexpected arguments count " << args.size();
      }
    };
  }
  {
    auto &attr = functions_["time"];
    // 0 for current time
    // 1 for string or map
    attr.minArity_ = 0;
    attr.maxArity_ = 1;
    attr.isPure_[0] = false;
    attr.isPure_[1] = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args.size()) {
        case 0: {
          return Value(time::TimeUtils::utcTime());
        }
        case 1: {
          if (args[0].get().isStr()) {
            auto result = time::TimeUtils::parseTime(args[0].get().getStr());
            if (!result.ok()) {
              DLOG(ERROR) << "DEBUG POINT: " << result.status();
              return Value::kNullBadData;
            }
            if (result.value().withTimeZone) {
              return result.value().t;
            } else {
              return time::TimeUtils::timeToUTC(result.value().t);
            }
          } else if (args[0].get().isMap()) {
            auto result = time::TimeUtils::timeFromMap(args[0].get().getMap());
            if (!result.ok()) {
              return Value::kNullBadData;
            }
            return time::TimeUtils::timeToUTC(result.value());
          } else {
            return Value::kNullBadType;
          }
        }
        default:
          LOG(FATAL) << "Unexpected arguments count " << args.size();
      }
    };
  }
  {
    auto &attr = functions_["datetime"];
    // 0 for current time
    // 1 for string or map
    attr.minArity_ = 0;
    attr.maxArity_ = 1;
    attr.isPure_[0] = false;
    attr.isPure_[1] = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args.size()) {
        case 0: {
          return Value(time::TimeUtils::utcDateTime());
        }
        case 1: {
          if (args[0].get().isStr()) {
            auto result = time::TimeUtils::parseDateTime(args[0].get().getStr());
            if (!result.ok()) {
              return Value::kNullBadData;
            }
            if (result.value().withTimeZone) {
              return result.value().dt;
            } else {
              return time::TimeUtils::dateTimeToUTC(result.value().dt);
            }
          } else if (args[0].get().isMap()) {
            auto result = time::TimeUtils::dateTimeFromMap(args[0].get().getMap());
            if (!result.ok()) {
              return Value::kNullBadData;
            }
            return time::TimeUtils::dateTimeToUTC(result.value());
          } else if (args[0].get().isInt()) {
            return time::TimeConversion::unixSecondsToDateTime(args[0].get().getInt());
          } else {
            return Value::kNullBadData;
          }
        }
        default:
          LOG(FATAL) << "Unexpected arguments count " << args.size();
      }
    };
  }
  {
    auto &attr = functions_["timestamp"];
    attr.minArity_ = 0;
    attr.maxArity_ = 1;
    attr.isPure_[0] = false;
    attr.isPure_[1] = true;
    attr.body_ = [](const auto &args) -> Value {
      if (args.empty()) {
        return ::time(NULL);
      }
      auto status = time::TimeUtils::toTimestamp(args[0].get());
      if (!status.ok()) {
        return Value::kNullBadData;
      }
      return status.value();
    };
  }
  {
    auto &attr = functions_["range"];
    attr.minArity_ = 2;
    attr.maxArity_ = 3;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isInt() || !args[1].get().isInt()) {
        return Value::kNullBadType;
      }

      int64_t start = args[0].get().getInt();
      int64_t end = args[1].get().getInt();
      int64_t step = 1;
      if (args.size() == 3) {
        if (!args[2].get().isInt()) {
          return Value::kNullBadType;
        }
        step = args[2].get().getInt();
      }
      if (step == 0) {
        return Value::kNullBadData;
      }

      List res;
      for (auto i = start; step > 0 ? i <= end : i >= end; i = i + step) {
        res.emplace_back(i);
      }
      return Value(res);
    };
  }
  {
    auto &attr = functions_["id"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::VERTEX: {
          return args[0].get().getVertex().vid;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["tags"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::VERTEX: {
          List tags;
          for (auto &tag : args[0].get().getVertex().tags) {
            tags.emplace_back(tag.name);
          }
          return tags;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
    functions_["labels"] = attr;
  }
  {
    auto &attr = functions_["properties"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::VERTEX: {
          Map props;
          for (auto &tag : args[0].get().getVertex().tags) {
            props.kvs.insert(tag.props.cbegin(), tag.props.cend());
          }
          return Value(std::move(props));
        }
        case Value::Type::EDGE: {
          Map props;
          props.kvs = args[0].get().getEdge().props;
          return Value(std::move(props));
        }
        case Value::Type::MAP: {
          return args[0].get();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["type"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::EDGE: {
          return args[0].get().getEdge().name;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["typeid"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::EDGE: {
          return args[0].get().getEdge().type;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["src"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::EDGE: {
          const auto &edge = args[0].get().getEdge();
          return edge.type > 0 ? edge.src : edge.dst;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["dst"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::EDGE: {
          const auto &edge = args[0].get().getEdge();
          return edge.type > 0 ? edge.dst : edge.src;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["none_direct_dst"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::EDGE: {
          const auto &edge = args[0].get().getEdge();
          return edge.dst;
        }
        case Value::Type::VERTEX: {
          const auto &v = args[0].get().getVertex();
          return v.vid;
        }
        case Value::Type::LIST: {
          const auto &listVal = args[0].get().getList();
          auto &lastVal = listVal.values.back();
          if (lastVal.isEdge()) {
            return lastVal.getEdge().dst;
          } else if (lastVal.isVertex()) {
            return lastVal.getVertex().vid;
          } else {
            return Value::kNullBadType;
          }
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["rank"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::EDGE: {
          return args[0].get().getEdge().ranking;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["startnode"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::EDGE: {
          return Vertex(args[0].get().getEdge().src, {});
        }
        case Value::Type::PATH: {
          return args[0].get().getPath().src;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["endnode"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::EDGE: {
          return Vertex(args[0].get().getEdge().dst, {});
        }
        case Value::Type::PATH: {
          auto &path = args[0].get().getPath();
          if (path.steps.empty()) {
            return path.src;
          }
          return path.steps.back().dst;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["head"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::LIST: {
          return args[0].get().getList().values.front();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["last"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::LIST: {
          return args[0].get().getList().values.back();
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["coalesce"];
    attr.minArity_ = 1;
    attr.maxArity_ = INT64_MAX;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      for (size_t i = 0; i < args.size(); ++i) {
        if (args[i].get().type() != Value::Type::NULLVALUE) {
          return args[i].get();
        }
      }
      return Value::kNullValue;
    };
  }
  {
    auto &attr = functions_["keys"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      std::set<std::string> tmp;
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::VERTEX: {
          for (auto &tag : args[0].get().getVertex().tags) {
            for (auto &prop : tag.props) {
              tmp.emplace(prop.first);
            }
          }
          break;
        }
        case Value::Type::EDGE: {
          for (auto &prop : args[0].get().getEdge().props) {
            tmp.emplace(prop.first);
          }
          break;
        }
        case Value::Type::MAP: {
          for (auto &kv : args[0].get().getMap().kvs) {
            tmp.emplace(kv.first);
          }
          break;
        }
        default: {
          return Value::kNullBadType;
        }
      }
      List result;
      result.values.assign(tmp.cbegin(), tmp.cend());
      return result;
    };
  }
  {
    auto &attr = functions_["nodes"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::PATH: {
          auto &path = args[0].get().getPath();
          List result;
          result.emplace_back(path.src);
          for (auto &step : path.steps) {
            result.emplace_back(step.dst);
          }
          return result;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["tail"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::LIST: {
          auto &list = args[0].get().getList();
          if (list.empty()) {
            return List();
          }
          return List(std::vector<Value>(list.values.begin() + 1, list.values.end()));
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["relationships"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      switch (args[0].get().type()) {
        case Value::Type::NULLVALUE: {
          return Value::kNullValue;
        }
        case Value::Type::PATH: {
          auto &path = args[0].get().getPath();
          List result;
          auto src = path.src.vid;
          for (size_t i = 0; i < path.steps.size(); ++i) {
            Edge edge;
            edge.src = src;
            edge.dst = path.steps[i].dst.vid;
            edge.type = path.steps[i].type;
            edge.name = path.steps[i].name;
            edge.ranking = path.steps[i].ranking;
            edge.props = path.steps[i].props;

            src = edge.dst;
            result.values.emplace_back(std::move(edge));
          }
          return result;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["hassameedgeinpath"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isPath()) {
        return Value::kNullBadType;
      }
      auto &path = args[0].get().getPath();
      return path.hasDuplicateEdges();
    };
  }
  {
    auto &attr = functions_["hassamevertexinpath"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isPath()) {
        return Value::kNullBadType;
      }
      auto &path = args[0].get().getPath();
      return path.hasDuplicateVertices();
    };
  }
  {
    auto &attr = functions_["reversepath"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isPath()) {
        return Value::kNullBadType;
      }
      auto path = args[0].get().getPath();
      path.reverse();
      return path;
    };
  }
  {
    auto &attr = functions_["datasetrowcol"];
    attr.minArity_ = 3;
    attr.maxArity_ = 3;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isDataSet() || !args[1].get().isInt() ||
          !(args[2].get().isInt() || args[2].get().isStr())) {
        return Value::kNullBadType;
      }
      const auto &ds = args[0].get().getDataSet();
      if (ds.rowSize() < 1 || ds.colSize() < 1) {
        return Value::kNullBadData;
      }
      const auto &colNames = ds.colNames;
      int64_t rowIndex = args[1].get().getInt();
      int64_t colIndex =
          args[2].get().isInt()
              ? args[2].get().getInt()
              : std::distance(colNames.begin(),
                              std::find(colNames.begin(), colNames.end(), args[2].get().getStr()));
      if (rowIndex < 0 || colIndex < 0) {
        return Value::kNullBadData;
      }
      if (static_cast<size_t>(rowIndex) >= ds.rowSize() ||
          static_cast<size_t>(colIndex) >= ds.colSize()) {
        return Value::kNullBadData;
      }
      return ds.rows[rowIndex][colIndex];
    };
  }
  {
    auto &attr = functions_["concat"];
    attr.minArity_ = 1;
    attr.maxArity_ = INT64_MAX;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      std::stringstream os;
      for (size_t i = 0; i < args.size(); ++i) {
        switch (args[i].get().type()) {
          case Value::Type::NULLVALUE: {
            return Value::kNullValue;
          }
          case Value::Type::BOOL: {
            os << (args[i].get().getBool() ? "true" : "false");
            break;
          }
          case Value::Type::INT: {
            os << args[i].get().getInt();
            break;
          }
          case Value::Type::FLOAT: {
            os << args[i].get().getFloat();
            break;
          }
          case Value::Type::STRING: {
            os << args[i].get().getStr();
            break;
          }
          case Value::Type::DATETIME: {
            os << args[i].get().getDateTime();
            break;
          }
          case Value::Type::DATE: {
            os << args[i].get().getDate();
            break;
          }
          case Value::Type::TIME: {
            os << args[i].get().getTime();
            break;
          }
          default: {
            return Value::kNullBadData;
          }
        }
      }
      return os.str();
    };
  }
  {
    auto &attr = functions_["concat_ws"];
    attr.minArity_ = 2;
    attr.maxArity_ = INT64_MAX;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (args[0].get().isNull() || !args[0].get().isStr()) {
        return Value::kNullValue;
      }
      std::vector<std::string> result;
      result.reserve(args.size() - 1);
      for (size_t i = 1; i < args.size(); ++i) {
        switch (args[i].get().type()) {
          case Value::Type::NULLVALUE: {
            continue;
          }
          case Value::Type::BOOL:
          case Value::Type::INT:
          case Value::Type::FLOAT:
          case Value::Type::DATE:
          case Value::Type::DATETIME:
          case Value::Type::TIME: {
            result.emplace_back(args[i].get().toString());
            break;
          }
          case Value::Type::STRING: {
            result.emplace_back(args[i].get().getStr());
            break;
          }
          default: {
            return Value::kNullBadData;
          }
        }
      }
      return folly::join(args[0].get().getStr(), result);
    };
  }
  // geo constructors
  {
    auto &attr = functions_["st_point"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isNumeric() || !args[1].get().isNumeric()) {
        return Value::kNullBadType;
      }
      double x = args[0].get().isInt() ? args[0].get().getInt() : args[0].get().getFloat();
      double y = args[1].get().isInt() ? args[1].get().getInt() : args[1].get().getFloat();
      Point point(Coordinate(x, y));
      return Geography(point);
    };
  }
  // geo parsers
  {
    auto &attr = functions_["st_geogfromtext"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isStr()) {
        return Value::kNullBadType;
      }
      const std::string &wkt = args[0].get().getStr();
      // Parse a geography from the wkt, normalize it and then verify its validity.
      auto geogRet = Geography::fromWKT(wkt, true, true);
      if (!geogRet.ok()) {
        LOG(ERROR) << "ST_GeogFromText error: " << geogRet.status();
        return Value::kNullBadData;
      }
      return std::move(geogRet).value();
    };
  }
  // {
  //   auto &attr = functions_["st_geogfromwkb"];
  //   attr.minArity_ = 1;
  //   attr.maxArity_ = 1;
  //   attr.isPure_ = true;
  //   attr.body_ = [](const auto &args) -> Value {
  //     if (!args[0].get().isStr()) {  // wkb is byte sequence
  //       return Value::kNullBadType;
  //     }
  //     const std::string &wkb = args[0].get().getStr();
  //     auto geogRet = Geography::fromWKB(wkb, true, true);
  //     if (!geogRet.ok()) {
  //       return Value::kNullBadData;
  //     }
  //     auto geog = std::move(geogRet).value();
  //     return geog;
  //   };
  // }
  // geo formatters
  {
    auto &attr = functions_["st_astext"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography()) {
        return Value::kNullBadType;
      }
      const Geography &g = args[0].get().getGeography();
      return g.asWKT();
    };
  }
  // {
  //   auto &attr = functions_["st_asbinary"];
  //   attr.minArity_ = 1;
  //   attr.maxArity_ = 1;
  //   attr.isPure_ = true;
  //   attr.body_ = [](const auto &args) -> Value {
  //     if (!args[0].get().isGeography()) {
  //       return Value::kNullBadType;
  //     }
  //     const Geography &g = args[0].get().getGeography();
  //     return g.asWKBHex();
  //   };
  // }
  // geo transformations
  {
    auto &attr = functions_["st_centroid"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography()) {
        return Value::kNullBadType;
      }
      return Geography(args[0].get().getGeography().centroid());
    };
  }
  // geo accessors
  {
    auto &attr = functions_["st_isvalid"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography()) {
        return Value::kNullBadType;
      }
      auto status = args[0].get().getGeography().isValid();
      if (!status.ok()) {
        LOG(ERROR) << "ST_IsValid error: " << status;
      }
      return status.ok();
    };
  }
  // geo predicates
  {
    auto &attr = functions_["st_intersects"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography() || !args[1].get().isGeography()) {
        return Value::kNullBadType;
      }
      return geo::GeoFunction::intersects(args[0].get().getGeography(),
                                          args[1].get().getGeography());
    };
  }
  {
    auto &attr = functions_["st_covers"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography() || !args[1].get().isGeography()) {
        return Value::kNullBadType;
      }
      return geo::GeoFunction::covers(args[0].get().getGeography(), args[1].get().getGeography());
    };
  }
  {
    auto &attr = functions_["st_coveredby"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography() || !args[1].get().isGeography()) {
        return Value::kNullBadType;
      }
      return geo::GeoFunction::coveredBy(args[0].get().getGeography(),
                                         args[1].get().getGeography());
    };
  }
  {
    auto &attr = functions_["st_dwithin"];
    attr.minArity_ = 3;
    attr.maxArity_ = 4;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography() || !args[1].get().isGeography() ||
          !args[2].get().isNumeric()) {
        return Value::kNullBadType;
      }
      bool exclusive = false;
      if (args.size() == 4) {
        if (!args[3].get().isBool()) {
          return Value::kNullBadType;
        }
        exclusive = args[3].get().getBool();
      }
      return geo::GeoFunction::dWithin(
          args[0].get().getGeography(),
          args[1].get().getGeography(),
          args[2].get().isFloat() ? args[2].get().getFloat() : args[2].get().getInt(),
          exclusive);
    };
  }
  // geo measures
  {
    auto &attr = functions_["st_distance"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography() || !args[1].get().isGeography()) {
        return Value::kNullBadType;
      }
      return geo::GeoFunction::distance(args[0].get().getGeography(), args[1].get().getGeography());
    };
  }
  // geo s2 functions
  {
    auto &attr = functions_["s2_cellidfrompoint"];
    attr.minArity_ = 1;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography()) {
        return Value::kNullBadType;
      }
      int level = 30;
      if (args.size() == 2) {
        if (!args[1].get().isInt()) {
          return Value::kNullBadType;
        }
        level = args[1].get().getInt();
        if (level < 0 || level > 30) {
          return Value::kNullBadData;
        }
      }
      const auto &geog = args[0].get().getGeography();
      if (geog.shape() != GeoShape::POINT) {
        LOG(ERROR) << "S2_CellIdFromPoint only accepts point argument";
        return Value::kNullBadData;
      }
      // TODO(jie) Should return uint64_t Value
      uint64_t cellId = geo::GeoFunction::s2CellIdFromPoint(args[0].get().getGeography(), level);
      const char *tmp = reinterpret_cast<const char *>(&cellId);
      int64_t cellId2 = *reinterpret_cast<const int64_t *>(tmp);
      return cellId2;
    };
  }
  {
    auto &attr = functions_["s2_coveringcellids"];
    attr.minArity_ = 1;
    attr.maxArity_ = 5;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isGeography()) {
        return Value::kNullBadType;
      }
      int minLevel = 0;
      int maxLevel = 30;
      int maxCells = 8;
      double bufferInMeters = 0.0;
      if (args.size() > 1) {
        if (!args[1].get().isInt()) {
          return Value::kNullBadType;
        }
        minLevel = args[1].get().getInt();
        if (minLevel < 0 || minLevel > 30) {
          return Value::kNullBadData;
        }
      }
      if (args.size() > 2) {
        if (!args[2].get().isInt()) {
          return Value::kNullBadType;
        }
        maxLevel = args[2].get().getInt();
        if (maxLevel < 0 || maxLevel > 30) {
          return Value::kNullBadData;
        }
        if (maxLevel < minLevel) {
          return Value::kNullBadData;
        }
      }
      if (args.size() > 3) {
        if (!args[3].get().isInt()) {
          return Value::kNullBadType;
        }
        maxCells = args[3].get().getInt();
        if (maxCells <= 0) {
          return Value::kNullBadData;
        }
      }
      if (args.size() > 4) {
        if (!args[4].get().isNumeric()) {
          return Value::kNullBadType;
        }
        bufferInMeters = args[4].get().isInt() ? args[4].get().getInt() : args[4].get().getFloat();
        if (bufferInMeters < 0.0) {
          return Value::kNullBadData;
        }
      }
      std::vector<uint64_t> cellIds = geo::GeoFunction::s2CoveringCellIds(
          args[0].get().getGeography(), minLevel, maxLevel, maxCells, bufferInMeters);
      // TODO(jie) Should return uint64_t List
      std::vector<Value> vals;
      vals.reserve(cellIds.size());
      for (uint64_t cellId : cellIds) {
        const char *tmp = reinterpret_cast<const char *>(&cellId);
        int64_t cellId2 = *reinterpret_cast<const int64_t *>(tmp);
        vals.emplace_back(cellId2);
      }
      return List(vals);
    };
  }
  {
    auto &attr = functions_["is_edge"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value { return args[0].get().isEdge(); };
  }
  {
    auto &attr = functions_["duration"];
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      const auto &arg = args[0].get();
      switch (arg.type()) {
        case Value::Type::MAP: {
          auto result = time::TimeUtils::durationFromMap(arg.getMap());
          if (result.ok()) {
            return result.value();
          } else {
            return Value::kNullBadData;
          }
        }
        case Value::Type::STRING: {
          // TODO
          return Value::kNullBadType;
        }
        default: {
          return Value::kNullBadType;
        }
      }
    };
  }
  {
    auto &attr = functions_["extract"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isStr() || !args[1].get().isStr()) {
        return Value::kNullBadType;
      }

      const auto &s = args[0].get().getStr();
      std::regex rgx(args[1].get().getStr());
      List res;
      for (std::sregex_iterator beg(s.begin(), s.end(), rgx), end{}; beg != end; ++beg) {
        res.emplace_back(beg->str());
      }
      return res;
    };
  }
  {
    auto &attr = functions_["_nodeid"];
    attr.minArity_ = 2;
    attr.maxArity_ = 2;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isPath() || !args[1].get().isInt()) {
        return Value::kNullBadType;
      }

      const auto &p = args[0].get().getPath();
      const std::size_t nodeIndex = args[1].get().getInt();
      if (nodeIndex < 0 || nodeIndex >= (1 + p.steps.size())) {
        DLOG(FATAL) << "Out of range node index.";
        return Value::kNullBadData;
      }
      if (nodeIndex == 0) {
        return p.src.vid;
      } else {
        return p.steps[nodeIndex - 1].dst.vid;
      }
    };
  }
  {
    auto &attr = functions_["json_extract"];
    // note, we don't support second argument(path) like MySQL JSON_EXTRACT for now
    attr.minArity_ = 1;
    attr.maxArity_ = 1;
    attr.isAlwaysPure_ = true;
    attr.body_ = [](const auto &args) -> Value {
      if (!args[0].get().isStr()) {
        return Value::kNullBadType;
      }
      auto json = args[0].get().getStr();

      // invalid string to json will be caught and returned as null
      try {
        auto obj = folly::parseJson(json);
        if (!obj.isObject()) {
          return Value::kNullBadData;
        }
        // if obj is empty, i.e. "{}", return empty map
        if (obj.empty()) {
          return Map();
        }
        return Map(obj);
      } catch (const std::exception &e) {
        return Value::kNullBadData;
      }
    };
  }
}  // NOLINT

// static
StatusOr<FunctionManager::Function> FunctionManager::get(const std::string &func, size_t arity) {
  auto result = instance().getInternal(func, arity);
  NG_RETURN_IF_ERROR(result);
  return result.value().body_;
}

// static
Status FunctionManager::find(const std::string &func, const size_t arity) {
  auto result = instance().getInternal(func, arity);
  NG_RETURN_IF_ERROR(result);
  return Status::OK();
}

/*static*/ StatusOr<bool> FunctionManager::getIsPure(const std::string &func, size_t arity) {
  auto result = instance().getInternal(func, arity);
  NG_RETURN_IF_ERROR(result);
  auto attr = std::move(result.value());

  if (attr.isAlwaysPure_) {
    return true;
  }

  // If the function is not always pure, lookup the map to find purity.
  return attr.isPure_.at(arity);
}

/*static*/ StatusOr<const FunctionManager::FunctionAttributes> FunctionManager::getInternal(
    std::string func, size_t arity) const {
  // check existence
  std::transform(func.begin(), func.end(), func.begin(), ::tolower);
  auto iter = functions_.find(func);
  if (iter == functions_.end()) {
    return Status::Error("Function `%s' not defined", func.c_str());
  }
  // check arity
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

// static
Status FunctionManager::load(const std::string &name, const std::vector<std::string> &funcs) {
  return instance().loadInternal(name, funcs);
}

Status FunctionManager::loadInternal(const std::string &, const std::vector<std::string> &) {
  return Status::Error("Dynamic function loading not supported yet");
}

// static
Status FunctionManager::unload(const std::string &name, const std::vector<std::string> &funcs) {
  return instance().loadInternal(name, funcs);
}

Status FunctionManager::unloadInternal(const std::string &, const std::vector<std::string> &) {
  return Status::Error("Dynamic function unloading not supported yet");
}

}  // namespace nebula
