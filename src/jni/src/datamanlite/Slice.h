/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_CODEC_SLICE_H_
#define DATAMAN_CODEC_SLICE_H_

#include <stdlib.h>
#include <string>
#include <cstring>
#include <cassert>

namespace nebula {
namespace dataman {
namespace codec {

/**
 * It is copied from rocksdb::Slice
*/
class Slice {
public:
    // Create an empty slice.
    Slice() : data_(""), size_(0) {}

    // Create a slice that refers to d[0,n-1].
    Slice(const char* d, size_t n) : data_(d), size_(n) {}

    // Create a slice that refers to the contents of "s"
    /* implicit */
    explicit Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

    // Create a slice that refers to s[0,strlen(s)-1]
    /* implicit */
    explicit Slice(const char* s) : data_(s) { size_ = (s == nullptr) ? 0 : strlen(s); }

    // Return a pointer to the beginning of the referenced data
    const char* data() const { return data_; }

    // Return the length (in bytes) of the referenced data
    size_t size() const { return size_; }

    // Return true iff the length of the referenced data is zero
    bool empty() const { return size_ == 0; }

    const char& operator[](size_t n) const {
        assert(n < size());
        return data_[n];
    }

    // Change this slice to refer to an empty array
    void clear() {
        data_ = "";
        size_ = 0;
    }

    // Drop the first "n" bytes from this slice.
    void remove_prefix(size_t n) {
        assert(n <= size());
        data_ += n;
        size_ -= n;
    }

    void remove_suffix(size_t n) {
        assert(n <= size());
        size_ -= n;
    }

    // Return a string that contains the copy of the referenced data.
    // when hex is true, returns a string of twice the length hex encoded (0-9A-F)
    std::string toString(bool hex = false) const;

    // Decodes the current slice interpreted as an hexadecimal string into result,
    // if successful returns true, if this isn't a valid hex string
    // (e.g not coming from Slice::toString(true)) DecodeHex returns false.
    // This slice is expected to have an even number of 0-9A-F characters
    // also accepts lowercase (a-f)
    bool DecodeHex(std::string* result) const;

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const Slice& b) const;

    // Return true iff "x" is a prefix of "*this"
    bool starts_with(const Slice& x) const {
        return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
    }

    bool ends_with(const Slice& x) const {
        return ((size_ >= x.size_) &&
                (memcmp(data_ + size_ - x.size_, x.data_, x.size_) == 0));
    }

    // Compare two slices and returns the first byte where they differ
    size_t difference_offset(const Slice& b) const;

    const char* begin() const {
        return data_;
    }

    const char* end() const {
        return data_ + size_;
    }

private:
    const char* data_;
    size_t size_;

  // Intentionally copyable
};

inline bool operator==(const Slice& x, const Slice& y) {
    return ((x.size() == y.size()) &&
           (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }


inline bool strToBool(Slice str) {
    return str == Slice("Y") || str == Slice("y") || str == Slice("T") || str == Slice("t") ||
           str == Slice("yes") || str == Slice("Yes") || str == Slice("YES") ||
           str == Slice("true") || str == Slice("True") || str == Slice("TRUE");
}

inline int Slice::compare(const Slice& b) const {
    assert(data_ != nullptr && b.data_ != nullptr);
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0) {
    if (size_ < b.size_)
        r = -1;
    else if (size_ > b.size_)
        r = +1;
    }
    return r;
}

inline size_t Slice::difference_offset(const Slice& b) const {
  size_t off = 0;
  const size_t len = (size_ < b.size_) ? size_ : b.size_;
  for (; off < len; off++) {
      if (data_[off] != b.data_[off]) break;
  }
  return off;
}

}  // namespace codec
}  // namespace dataman
}  // namespace nebula

#endif  // DATAMAN_CODEC_SLICE_H_

