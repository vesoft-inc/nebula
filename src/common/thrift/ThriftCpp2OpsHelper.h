/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_THRIFT_THRIFTCPP2OPS_HELPER_H_
#define COMMON_THRIFT_THRIFTCPP2OPS_HELPER_H_
#include <thrift/lib/cpp2/GeneratedCodeHelper.h>

#define SPECIALIZE_CPP2OPS(X)                                                           \
    template <>                                                                         \
    class Cpp2Ops<X> {                                                                  \
    public:                                                                             \
        using Type = X;                                                                 \
        inline static constexpr protocol::TType thriftType();                           \
        template <class Protocol>                                                       \
        static uint32_t write(Protocol *proto, const Type *obj);                        \
        template <class Protocol>                                                       \
        static void read(Protocol *proto, Type *obj);                                   \
        template <class Protocol>                                                       \
        static uint32_t serializedSize(const Protocol *proto, const Type *obj);         \
        template <class Protocol>                                                       \
        static uint32_t serializedSizeZC(const Protocol *proto, const Type *obj);       \
    }




#endif  // COMMON_THRIFT_THRIFTCPP2OPS_HELPER_H_
