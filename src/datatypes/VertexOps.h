/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_VERTEXOPS_H_
#define DATATYPES_VERTEXOPS_H_

#include "base/Base.h"

#include <thrift/lib/cpp2/GeneratedSerializationCodeHelper.h>
#include <thrift/lib/cpp2/gen/module_types_tcc.h>
#include <thrift/lib/cpp2/protocol/ProtocolReaderStructReadState.h>

#include "datatypes/Vertex.h"

namespace apache {
namespace thrift {

namespace detail {

template <>
struct TccStructTraits<nebula::Vertex> {
    static void translateFieldName(
            FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
            FOLLY_MAYBE_UNUSED int16_t& fid,
            FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
        if (_fname == "vid") {
            fid = 1;
            _ftype = apache::thrift::protocol::T_STRING;
        } else if (_fname == "tags") {
            fid = 2;
            _ftype = apache::thrift::protocol::T_LIST;
        }
    }
};

}  // namespace detail


template<>
inline void Cpp2Ops<nebula::Vertex>::clear(nebula::Vertex* obj) {
    return obj->clear();
}


template<>
inline constexpr protocol::TType Cpp2Ops<nebula::Vertex>::thriftType() {
    return apache::thrift::protocol::T_STRUCT;
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::Vertex>::write(Protocol* proto, nebula::Vertex const* obj) {
    uint32_t xfer = 0;
    xfer += proto->writeStructBegin("Vertex");
    xfer += proto->writeFieldBegin("vid", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->writeBinary(obj->vid);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldBegin("tags", apache::thrift::protocol::T_LIST, 2);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Tag>
        >::write(*proto, obj->tags);
    xfer += proto->writeFieldEnd();
    xfer += proto->writeFieldStop();
    xfer += proto->writeStructEnd();
    return xfer;
}


template<>
template<class Protocol>
void Cpp2Ops<nebula::Vertex>::read(Protocol* proto, nebula::Vertex* obj) {
    detail::ProtocolReaderStructReadState<Protocol> readState;

    readState.readStructBegin(proto);

    using apache::thrift::TProtocolException;


    if (UNLIKELY(!readState.advanceToNextField(proto, 0, 1, protocol::T_STRING))) {
        goto _loop;
    }

_readField_vid:
    {
        proto->readBinary(obj->vid);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 1, 2, protocol::T_LIST))) {
        goto _loop;
    }

_readField_tags:
    {
        obj->tags = std::vector<nebula::Tag>();
        detail::pm::protocol_methods<
                type_class::list<type_class::structure>,
                std::vector<nebula::Tag>
            >::read(*proto, obj->tags);
    }

    if (UNLIKELY(!readState.advanceToNextField(proto, 2, 0, protocol::T_STOP))) {
        goto _loop;
    }

_end:
    readState.readStructEnd(proto);

    return;

_loop:
    if (readState.fieldType == apache::thrift::protocol::T_STOP) {
        goto _end;
    }

    if (proto->kUsesFieldNames()) {
        detail::TccStructTraits<nebula::Vertex>::translateFieldName(readState.fieldName(),
                                                                    readState.fieldId,
                                                                    readState.fieldType);
    }

    switch (readState.fieldId) {
        case 1:
        {
            if (LIKELY(readState.fieldType == apache::thrift::protocol::T_STRING)) {
                goto _readField_vid;
            } else {
                goto _skip;
            }
        }
        case 2:
        {
          if (LIKELY(readState.fieldType == apache::thrift::protocol::T_LIST)) {
                goto _readField_tags;
          } else {
                goto _skip;
          }
        }
        default:
        {
_skip:
          proto->skip(readState.fieldType);
          readState.readFieldEnd(proto);
          readState.readFieldBeginNoInline(proto);
          goto _loop;
        }
    }
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::Vertex>::serializedSize(Protocol const* proto,
                                                 nebula::Vertex const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Vertex");
    xfer += proto->serializedFieldSize("vid", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeBinary(obj->vid);
    xfer += proto->serializedFieldSize("tags", apache::thrift::protocol::T_LIST, 2);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Tag>
        >::serializedSize<false>(*proto, obj->tags);
    xfer += proto->serializedSizeStop();
    return xfer;
}


template<>
template<class Protocol>
uint32_t Cpp2Ops<nebula::Vertex>::serializedSizeZC(Protocol const* proto,
                                                   nebula::Vertex const* obj) {
    uint32_t xfer = 0;
    xfer += proto->serializedStructSize("Vertex");
    xfer += proto->serializedFieldSize("vid", apache::thrift::protocol::T_STRING, 1);
    xfer += proto->serializedSizeZCBinary(obj->vid);
    xfer += proto->serializedFieldSize("tags", apache::thrift::protocol::T_LIST, 2);
    xfer += detail::pm::protocol_methods<
            type_class::list<type_class::structure>,
            std::vector<nebula::Tag>
        >::serializedSize<false>(*proto, obj->tags);
    xfer += proto->serializedSizeStop();
    return xfer;
}

}  // namespace thrift
}  // namespace apache
#endif  // DATATYPES_VERTEXOPS_H_

