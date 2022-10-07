// Copyright [2022] <nebula>
//
// Get from Dr. Lemire.
//

#ifndef SUBGRAPHMATCHING_BITSETOPERATION_H
#define SUBGRAPHMATCHING_BITSETOPERATION_H

#include <cstdint>

class BitsetOperation {
 public:
  static uint32_t extractBitset(const uint64_t *bitset, uint32_t length, uint32_t *output);
  static void setBitsetList(void *bitset, const uint32_t *list, uint32_t length);
  static bool checkBitset(const uint64_t *bitset, uint32_t pos);
  static void intersectBitsetWithBitset(const uint64_t *a,
                                        const uint64_t *b,
                                        uint64_t *output,
                                        uint32_t count);
  static uint32_t intersectArrayWithBitset(const uint32_t *a,
                                           uint32_t a_count,
                                           const uint64_t *b,
                                           uint32_t *output);
  static uint32_t intersectArrayWithArray(
      const uint32_t *a, uint32_t a_count, const uint32_t *b, uint32_t b_count, uint32_t *output);
  static uint32_t mergeIntersection(
      const uint32_t *a, uint32_t a_count, const uint32_t *b, uint32_t b_count, uint32_t *output);
  static uint32_t skewIntersection(const uint32_t *small,
                                   uint32_t small_count,
                                   const uint32_t *large,
                                   uint32_t large_count,
                                   uint32_t *output);
  static bool binarySearch(const uint32_t *src, uint32_t begin, uint32_t end, uint32_t target);
};

#endif  // SUBGRAPHMATCHING_BITSETOPERATION_H
