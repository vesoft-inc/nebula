/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace nebula {

template<typename T>
typename std::enable_if<std::is_integral<T>::value, RowWriter&>::type
RowWriter::operator<<(T v) noexcept {
    RW_GET_COLUMN_TYPE(INT)

    switch (type->get_type()) {
        case cpp2::SupportedType::INT:
        case cpp2::SupportedType::TIMESTAMP: {
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
    DCHECK_GT(len, 0UL);
    cord_.write(reinterpret_cast<const char*>(&buf[0]), len);
}

}  // namespace nebula

