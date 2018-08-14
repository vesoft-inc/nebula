/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace vesoft {
namespace vgraph {

template<typename T>
typename std::enable_if<std::is_integral<T>::value, RowWriter&>::type
RowWriter::operator<<(T v) noexcept {
    RW_GET_COLUMN_TYPE(INT)

    switch (type->get_type()) {
        case cpp2::SupportedType::INT: {
            writeInt(v);
            break;
        }
        case cpp2::SupportedType::VID: {
            cord_ << (uint64_t)v;
            break;
        }
        default: {
            LOG(ERROR) << "Incompatible value type \"int\"";
            writeInt(0);
            break;
        }
    }

    RW_CLEAN_UP_WRITE()
    return *this;
}


template<typename T>
typename std::enable_if<std::is_integral<T>::value>::type
RowWriter::writeInt(T v) {
    uint8_t buf[10];
    size_t len = folly::encodeVarint(v, buf);
    DCHECK_GT(len, 0);
    cord_ << folly::ByteRange(buf, len);
}

}  // namespace vgraph
}  // namespace vesoft

