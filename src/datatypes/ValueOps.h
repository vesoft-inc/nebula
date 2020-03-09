/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_VALUEOPS_H_
#define DATATYPES_VALUEOPS_H_

#include "base/Base.h"

#include "interface/gen-cpp2/common_types.h"

#include <thrift/lib/cpp2/GeneratedSerializationCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "datatypes/Value.h"
#include "datatypes/Map.h"
#include "datatypes/List.h"

namespace apache {
namespace thrift {

namespace detail {

template<>
struct TccStructTraits<nebula::Value> {
    static void translateFieldName(
            FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
            FOLLY_MAYBE_UNUSED int16_t& fid,
            FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "nVal") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_I32;
        } else if (_fname == "bVal") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_BOOL;
        } else if (_fname == "iVal") {
            fid = 3;
            _ftype = apache::thrift::protocol::T_I64;
        } else if (_fname == "fVal") {
            fid = 4;
            _ftype = apache::thrift::protocol::T_DOUBLE;
        } else if (_fname == "sVal") {
            fid = 5;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "dVal") {
            fid = 6;
            _ftype = apache::thrift::protocol::T_STRUCT;
        } else if (_fname == "tVal") {
            fid = 7;
            _ftype = apache::thrift::protocol::T_STRUCT;
        } else if (_fname == "pVal") {
            fid = 8;
            _ftype = apache::thrift::protocol::T_STRUCT;
        } else if (_fname == "lVal") {
            fid = 9;
            _ftype = apache::thrift::protocol::T_STRUCT;
        } else if (_fname == "mVal") {
            fid = 10;
            _ftype = apache::thrift::protocol::T_STRUCT;
        }
    }
};

}  // namespace detail


template<>
inline void Cpp2Ops<nebula::Value>::clear(nebula::Value* obj) {
    return obj->clear();
}


template<>
inline constexpr apache::thrift::protocol::TType Cpp2Ops<nebula::Value>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::Value>::write(Protocol* proto, nebula::Value const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("Value");
    switch (obj->type()) {
        case nebula::Value::Type::NULLVALUE:
        {
            xfer += proto->writeFieldBegin("nVal", apache::thrift::protocol::T_I32, 1);
            xfer += detail::pm::protocol_methods<type_class::enumeration,
                                                 nebula::NullType>
                ::write(*proto, obj->value_.nVal);
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::BOOL:
        {
            xfer += proto->writeFieldBegin("bVal", apache::thrift::protocol::T_BOOL, 2);
            xfer += proto->writeBool(obj->value_.bVal);
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::INT:
        {
            xfer += proto->writeFieldBegin("iVal", apache::thrift::protocol::T_I64, 3);
            xfer += detail::pm::protocol_methods<type_class::integral, int64_t>
                ::write(*proto, obj->value_.iVal);
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::FLOAT:
        {
            xfer += proto->writeFieldBegin("fVal", apache::thrift::protocol::T_DOUBLE, 4);
            xfer += proto->writeDouble(obj->value_.fVal);
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::STRING:
        {
            xfer += proto->writeFieldBegin("sVal", apache::thrift::protocol::T_STRING, 5);
            xfer += proto->writeBinary(obj->value_.sVal);
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::DATE:
        {
            xfer += proto->writeFieldBegin("dVal", apache::thrift::protocol::T_STRUCT, 6);
            xfer += Cpp2Ops<nebula::Date>::write(proto, &obj->value_.dVal);
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::DATETIME:
        {
            xfer += proto->writeFieldBegin("tVal", apache::thrift::protocol::T_STRUCT, 7);
            xfer += Cpp2Ops<nebula::DateTime>::write(proto, &obj->value_.tVal);
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::PATH:
        {
            xfer += proto->writeFieldBegin("pVal", apache::thrift::protocol::T_STRUCT, 8);
            xfer += Cpp2Ops<nebula::Path>::write(proto, &obj->value_.pVal);
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::LIST:
        {
            xfer += proto->writeFieldBegin("lVal", apache::thrift::protocol::T_STRUCT, 9);
            if (obj->value_.lVal) {
                xfer += Cpp2Ops<nebula::List>::write(proto, obj->value_.lVal.get());
            } else {
                xfer += proto->writeStructBegin("List");
                xfer += proto->writeStructEnd();
                xfer += proto->writeFieldStop();
            }
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::MAP:
        {
            xfer += proto->writeFieldBegin("mVal", apache::thrift::protocol::T_STRUCT, 10);
            if (obj->value_.mVal) {
                xfer += Cpp2Ops<nebula::Map>::write(proto, obj->value_.mVal.get());
            } else {
                xfer += proto->writeStructBegin("Map");
                xfer += proto->writeStructEnd();
                xfer += proto->writeFieldStop();
            }
            xfer += proto->writeFieldEnd();
            break;
        }
        case nebula::Value::Type::__EMPTY__: {}
    }
    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template<>
template<class Protocol>
void Cpp2Ops<nebula::Value>::read(Protocol* proto, nebula::Value* obj) {
    detail::ProtocolReaderStructReadState<Protocol> readState;
    readState.fieldId = 0;

    readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;

    readState.readFieldBegin(proto);
    if (readState.fieldType == protocol::T_STOP) {
        obj->clear();
    } else {
        if (proto->kUsesFieldNames()) {
            detail::TccStructTraits<nebula::Value>
                  ::translateFieldName(readState.fieldName(),
                                       readState.fieldId,
                                       readState.fieldType);
        }
        switch (readState.fieldId) {
            case 1:
            {
                if (readState.fieldType == apache::thrift::protocol::T_I32) {
                    obj->setNull(nebula::NullType());
                    detail::pm::protocol_methods<type_class::enumeration, nebula::NullType>
                        ::read(*proto, obj->value_.nVal);
              } else {
                    proto->skip(readState.fieldType);
              }
              break;
            }
            case 2:
            {
                if (readState.fieldType == apache::thrift::protocol::T_BOOL) {
                    obj->setBool(false);
                    proto->readBool(obj->value_.bVal);
                } else {
                    proto->skip(readState.fieldType);
                }
                break;
            }
            case 3:
            {
                if (readState.fieldType == apache::thrift::protocol::T_I64) {
                    obj->setInt(0);
                    detail::pm::protocol_methods<type_class::integral, int64_t>
                        ::read(*proto, obj->value_.iVal);
                } else {
                    proto->skip(readState.fieldType);
                }
                break;
            }
            case 4:
            {
                if (readState.fieldType == apache::thrift::protocol::T_DOUBLE) {
                    obj->setFloat(0.0);
                    proto->readDouble(obj->value_.fVal);
                } else {
                    proto->skip(readState.fieldType);
                }
                break;
            }
            case 5:
            {
                if (readState.fieldType == apache::thrift::protocol::T_STRING) {
                    obj->setStr(std::string());
                    proto->readBinary(obj->value_.sVal);
                } else {
                    proto->skip(readState.fieldType);
                }
                break;
            }
            case 6:
            {
                if (readState.fieldType == apache::thrift::protocol::T_STRUCT) {
                    obj->setDate(nebula::Date());
                    Cpp2Ops<nebula::Date>::read(proto, &obj->value_.dVal);
                } else {
                    proto->skip(readState.fieldType);
                }
                break;
            }
            case 7:
            {
                if (readState.fieldType == apache::thrift::protocol::T_STRUCT) {
                    obj->setDateTime(nebula::DateTime());
                    Cpp2Ops<nebula::DateTime>::read(proto, &obj->value_.tVal);
                } else {
                    proto->skip(readState.fieldType);
                }
                break;
            }
            case 8:
            {
                if (readState.fieldType == apache::thrift::protocol::T_STRUCT) {
                    obj->setPath(nebula::Path());
                    Cpp2Ops<nebula::Path>::read(proto, &obj->value_.pVal);
                } else {
                    proto->skip(readState.fieldType);
                }
                break;
            }
            case 9:
            {
                if (readState.fieldType == apache::thrift::protocol::T_STRUCT) {
                    obj->setList(nebula::List());
                    std::unique_ptr<nebula::List> ptr = std::make_unique<nebula::List>();
                    Cpp2Ops<nebula::List>::read(proto, ptr.get());
                    obj->value_.lVal = std::move(ptr);
                } else {
                    proto->skip(readState.fieldType);
                }
                break;
            }
            case 10:
            {
                if (readState.fieldType == apache::thrift::protocol::T_STRUCT) {
                    obj->setMap(nebula::Map());
                    std::unique_ptr<nebula::Map> ptr = std::make_unique<nebula::Map>();
                    Cpp2Ops<nebula::Map>::read(proto, ptr.get());
                    obj->value_.mVal = std::move(ptr);
                } else {
                    proto->skip(readState.fieldType);
                }
                break;
            }
            default:
            {
                proto->skip(readState.fieldType);
                break;
            }
        }
        readState.readFieldEnd(proto);
        readState.readFieldBegin(proto);
        if (UNLIKELY(readState.fieldType != apache::thrift::protocol::T_STOP)) {
            using apache::thrift::protocol::TProtocolException;
            TProtocolException::throwUnionMissingStop();
        }
    }
    readState.readStructEnd(proto);
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::Value>::serializedSize(Protocol const* proto,
                                                nebula::Value const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Value");
    switch (obj->type()) {
        case nebula::Value::Type::NULLVALUE:
        {
            xfer += proto->serializedFieldSize("nVal", apache::thrift::protocol::T_I32, 1);
            xfer += detail::pm::protocol_methods<type_class::enumeration, nebula::NullType>
                ::serializedSize<false>(*proto, obj->value_.nVal);
            break;
        }
        case nebula::Value::Type::BOOL:
        {
            xfer += proto->serializedFieldSize("bVal", apache::thrift::protocol::T_BOOL, 2);
            xfer += proto->serializedSizeBool(obj->value_.bVal);
            break;
        }
        case nebula::Value::Type::INT:
        {
            xfer += proto->serializedFieldSize("iVal", apache::thrift::protocol::T_I64, 3);
            xfer += detail::pm::protocol_methods<type_class::integral, int64_t>
                ::serializedSize<false>(*proto, obj->value_.iVal);
          break;
        }
        case nebula::Value::Type::FLOAT:
        {
            xfer += proto->serializedFieldSize("fVal", apache::thrift::protocol::T_DOUBLE, 4);
            xfer += proto->serializedSizeDouble(obj->value_.fVal);
            break;
        }
        case nebula::Value::Type::STRING:
        {
            xfer += proto->serializedFieldSize("sVal", apache::thrift::protocol::T_STRING, 5);
            xfer += proto->serializedSizeBinary(obj->value_.sVal);
            break;
        }
        case nebula::Value::Type::DATE:
        {
            xfer += proto->serializedFieldSize("dVal", apache::thrift::protocol::T_STRUCT, 6);
            xfer += Cpp2Ops<nebula::Date>::serializedSize(proto, &obj->value_.dVal);
            break;
        }
        case nebula::Value::Type::DATETIME:
        {
            xfer += proto->serializedFieldSize("tVal", apache::thrift::protocol::T_STRUCT, 7);
            xfer += Cpp2Ops<nebula::DateTime>::serializedSize(proto, &obj->value_.tVal);
            break;
        }
        case nebula::Value::Type::PATH:
        {
            xfer += proto->serializedFieldSize("pVal", apache::thrift::protocol::T_STRUCT, 8);
            xfer += Cpp2Ops<nebula::Path>::serializedSize(proto, &obj->value_.pVal);
            break;
        }
        case nebula::Value::Type::LIST:
        {
            xfer += proto->serializedFieldSize("lVal", apache::thrift::protocol::T_STRUCT, 9);
            if (obj->value_.lVal) {
                xfer += Cpp2Ops<nebula::List>::serializedSize(proto, obj->value_.lVal.get());
            } else {
                xfer += proto->serializedStructSize("List");
                xfer += proto->serializedSizeStop();
            }
            break;
        }
        case nebula::Value::Type::MAP:
        {
            xfer += proto->serializedFieldSize("mVal", apache::thrift::protocol::T_STRUCT, 10);
            if (obj->value_.mVal) {
                xfer += Cpp2Ops<nebula::Map>::serializedSize(proto, obj->value_.mVal.get());
            } else {
                xfer += proto->serializedStructSize("Map");
                xfer += proto->serializedSizeStop();
            }
            break;
        }
        case nebula::Value::Type::__EMPTY__: {}
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::Value>::serializedSizeZC(Protocol const* proto,
                                                  nebula::Value const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Value");
    switch (obj->type()) {
        case nebula::Value::Type::NULLVALUE:
        {
            xfer += proto->serializedFieldSize("nVal", apache::thrift::protocol::T_I32, 1);
            xfer += detail::pm::protocol_methods<type_class::enumeration, nebula::NullType>
                ::serializedSize<false>(*proto, obj->value_.nVal);
            break;
        }
        case nebula::Value::Type::BOOL:
        {
            xfer += proto->serializedFieldSize("bVal", apache::thrift::protocol::T_BOOL, 2);
            xfer += proto->serializedSizeBool(obj->value_.bVal);
            break;
        }
        case nebula::Value::Type::INT:
        {
            xfer += proto->serializedFieldSize("iVal", apache::thrift::protocol::T_I64, 3);
            xfer += detail::pm::protocol_methods<type_class::integral, int64_t>
                ::serializedSize<false>(*proto, obj->value_.iVal);
            break;
        }
        case nebula::Value::Type::FLOAT:
        {
            xfer += proto->serializedFieldSize("fVal", apache::thrift::protocol::T_DOUBLE, 4);
            xfer += proto->serializedSizeDouble(obj->value_.fVal);
            break;
        }
        case nebula::Value::Type::STRING:
        {
            xfer += proto->serializedFieldSize("sVal", apache::thrift::protocol::T_STRING, 5);
            xfer += proto->serializedSizeZCBinary(obj->value_.sVal);
            break;
        }
        case nebula::Value::Type::DATE:
        {
            xfer += proto->serializedFieldSize("dVal", apache::thrift::protocol::T_STRUCT, 6);
            xfer += Cpp2Ops<nebula::Date>::serializedSizeZC(proto, &obj->value_.dVal);
            break;
        }
        case nebula::Value::Type::DATETIME:
        {
            xfer += proto->serializedFieldSize("tVal", apache::thrift::protocol::T_STRUCT, 7);
            xfer += Cpp2Ops<nebula::DateTime>::serializedSizeZC(proto, &obj->value_.tVal);
            break;
        }
        case nebula::Value::Type::PATH:
        {
            xfer += proto->serializedFieldSize("pVal", apache::thrift::protocol::T_STRUCT, 8);
            xfer += Cpp2Ops<nebula::Path>::serializedSizeZC(proto, &obj->value_.pVal);
            break;
        }
        case nebula::Value::Type::LIST:
        {
            xfer += proto->serializedFieldSize("lVal", apache::thrift::protocol::T_STRUCT, 9);
            if (obj->value_.lVal) {
                xfer += Cpp2Ops<nebula::List>::serializedSizeZC(proto, obj->value_.lVal.get());
            } else {
                xfer += proto->serializedStructSize("List");
                xfer += proto->serializedSizeStop();
            }
            break;
        }
        case nebula::Value::Type::MAP:
        {
            xfer += proto->serializedFieldSize("mVal", apache::thrift::protocol::T_STRUCT, 10);
            if (obj->value_.mVal) {
                xfer += Cpp2Ops<nebula::Map>::serializedSizeZC(proto, obj->value_.mVal.get());
            } else {
                xfer += proto->serializedStructSize("Map");
                xfer += proto->serializedSizeStop();
            }
            break;
        }
        case nebula::Value::Type::__EMPTY__: {}
    }
    xfer += proto->serializedSizeStop();
    return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // DATATYPES_VALUEOPS_H_

