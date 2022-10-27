// Copyright [2022] <nebula>
//
// Get from Dr. Lemire.
//

#include "bitsetoperation.h"

void BitsetOperation::setBitsetList(void *bitset, const uint32_t *list, uint32_t length) {
  uint64_t pos;
  const uint32_t *end = list + length;

  uint64_t shift = 6;
  uint64_t offset;
  uint64_t load;
  for (; list + 3 < end; list += 4) {
    pos = list[0];
    __asm volatile(
        "shrx %[shift], %[pos], %[offset]\n"
        "mov (%[bitset],%[offset],8), %[load]\n"
        "bts %[pos], %[load]\n"
        "mov %[load], (%[bitset],%[offset],8)"
        : [ load ] "=&r"(load), [ offset ] "=&r"(offset)
        : [ bitset ] "r"(bitset), [ shift ] "r"(shift), [ pos ] "r"(pos));
    pos = list[1];
    __asm volatile(
        "shrx %[shift], %[pos], %[offset]\n"
        "mov (%[bitset],%[offset],8), %[load]\n"
        "bts %[pos], %[load]\n"
        "mov %[load], (%[bitset],%[offset],8)"
        : [ load ] "=&r"(load), [ offset ] "=&r"(offset)
        : [ bitset ] "r"(bitset), [ shift ] "r"(shift), [ pos ] "r"(pos));
    pos = list[2];
    __asm volatile(
        "shrx %[shift], %[pos], %[offset]\n"
        "mov (%[bitset],%[offset],8), %[load]\n"
        "bts %[pos], %[load]\n"
        "mov %[load], (%[bitset],%[offset],8)"
        : [ load ] "=&r"(load), [ offset ] "=&r"(offset)
        : [ bitset ] "r"(bitset), [ shift ] "r"(shift), [ pos ] "r"(pos));
    pos = list[3];
    __asm volatile(
        "shrx %[shift], %[pos], %[offset]\n"
        "mov (%[bitset],%[offset],8), %[load]\n"
        "bts %[pos], %[load]\n"
        "mov %[load], (%[bitset],%[offset],8)"
        : [ load ] "=&r"(load), [ offset ] "=&r"(offset)
        : [ bitset ] "r"(bitset), [ shift ] "r"(shift), [ pos ] "r"(pos));
  }

  while (list != end) {
    pos = list[0];
    __asm volatile(
        "shrx %[shift], %[pos], %[offset]\n"
        "mov (%[bitset],%[offset],8), %[load]\n"
        "bts %[pos], %[load]\n"
        "mov %[load], (%[bitset],%[offset],8)"
        : [ load ] "=&r"(load), [ offset ] "=&r"(offset)
        : [ bitset ] "r"(bitset), [ shift ] "r"(shift), [ pos ] "r"(pos));
    list++;
  }
}

void BitsetOperation::intersectBitsetWithBitset(const uint64_t *__restrict__ a,
                                                const uint64_t *__restrict__ b,
                                                uint64_t *__restrict__ output,
                                                const uint32_t count) {
  for (uint32_t i = 0; i < count; ++i) {
    output[i] = a[i] & b[i];
  }
}

uint32_t BitsetOperation::intersectArrayWithBitset(const uint32_t *a,
                                                   const uint32_t a_count,
                                                   const uint64_t *b,
                                                   uint32_t *output) {
  uint32_t count = 0;
  for (uint32_t i = 0; i < a_count; ++i) {
    uint32_t element = a[i];
    output[count] = element;
    count += checkBitset(b, element);
  }
  return count;
}

bool BitsetOperation::checkBitset(const uint64_t *bitset, uint32_t pos) {
  uint64_t word = bitset[pos >> 6];
  const uint64_t p = pos;
  __asm volatile("shrx %1, %0, %0"
                 : "+r"(word)
                 :      /* read/write */
                 "r"(p) /* read only */
  );
  return static_cast<bool>(word & 1);
}

uint32_t BitsetOperation::intersectArrayWithArray(
    const uint32_t *a, uint32_t a_count, const uint32_t *b, uint32_t b_count, uint32_t *output) {
  uint32_t count;
  const uint32_t skew_threshold = 32;
  if (a_count * skew_threshold < b_count) {
    count = skewIntersection(a, a_count, b, b_count, output);
  } else if (b_count * skew_threshold < a_count) {
    count = skewIntersection(b, b_count, a, a_count, output);
  } else {
    count = mergeIntersection(a, a_count, b, b_count, output);
  }
  return count;
}

uint32_t BitsetOperation::mergeIntersection(
    const uint32_t *a, uint32_t a_count, const uint32_t *b, uint32_t b_count, uint32_t *output) {
  const uint32_t *initout = output;
  if (a_count == 0 || b_count == 0) return 0;
  const uint32_t *endA = a + a_count;
  const uint32_t *endB = b + b_count;

  while (true) {
    while (*a < *b) {
SKIP_FIRST_COMPARE:
      if (++a == endA) return (output - initout);
    }
    while (*a > *b) {
      if (++b == endB) return (output - initout);
    }
    if (*a == *b) {
      *output++ = *a;
      if (++a == endA || ++b == endB) return (output - initout);
    } else {
      goto SKIP_FIRST_COMPARE;
    }
  }
  return (output - initout);  // NOTREACHED
}

uint32_t BitsetOperation::skewIntersection(const uint32_t *small,
                                           uint32_t small_count,
                                           const uint32_t *large,
                                           uint32_t large_count,
                                           uint32_t *output) {
  uint32_t count = 0;
  if (0 == small_count) {
    return count;
  }

  for (uint32_t i = 0; i < small_count; ++i) {
    output[count] = small[i];
    count += binarySearch(large, 0, large_count, small[i]);
  }

  return count;
}

bool BitsetOperation::binarySearch(const uint32_t *src,
                                   uint32_t begin,
                                   uint32_t end,
                                   uint32_t target) {
  int32_t temp_begin = begin;
  int32_t temp_end = end - 1;
  while (temp_begin <= temp_end) {
    int mid = temp_begin + ((temp_end - temp_begin) >> 1);
    if (src[mid] > target)
      temp_end = mid - 1;
    else if (src[mid] < target)
      temp_begin = mid + 1;
    else
      return true;
  }

  return false;
}

uint32_t BitsetOperation::extractBitset(const uint64_t *bitset, uint32_t length, uint32_t *output) {
  int outpos = 0;
  int base = 0;
  for (uint32_t i = 0; i < length; ++i) {
    uint64_t w = bitset[i];
    while (w != 0) {
      uint64_t t = w & (~w + 1);
      int r = __builtin_ctzll(w);
      output[outpos++] = static_cast<uint32_t>(r + base);
      w ^= t;
    }
    base += 64;
  }
  return static_cast<uint32_t>(outpos);
}
