// Copyright [2022] <nebula>
//
// Created by ssunah on 11/30/17.
//

#include "computesetintersection.h"

#include <cstdint>

void ComputeSetIntersection::ComputeCandidates(const V_ID *larray,
                                               const ui l_count,
                                               const V_ID *rarray,
                                               const ui r_count,
                                               V_ID *cn,
                                               ui &cn_count) {
#if HYBRID == 0
#if SI == 0
  if (l_count / 50 > r_count || r_count / 50 > l_count) {
    return ComputeCNGallopingAVX2(larray, l_count, rarray, r_count, cn, cn_count);
  } else {
    return ComputeCNMergeBasedAVX2(larray, l_count, rarray, r_count, cn, cn_count);
  }
#elif SI == 1
  if (l_count / 50 > r_count || r_count / 50 > l_count) {
    return ComputeCNGallopingAVX512(larray, l_count, rarray, r_count, cn, cn_count);
  } else {
    return ComputeCNMergeBasedAVX512(larray, l_count, rarray, r_count, cn, cn_count);
  }
#elif SI == 2
  if (l_count / 50 > r_count || r_count / 50 > l_count) {
    return ComputeCNGalloping(larray, l_count, rarray, r_count, cn, cn_count);
  } else {
    return ComputeCNNaiveStdMerge(larray, l_count, rarray, r_count, cn, cn_count);
  }
#endif
#elif HYBRID == 1
#if SI == 0
  return ComputeCNMergeBasedAVX2(larray, l_count, rarray, r_count, cn, cn_count);
#elif SI == 1
  return ComputeCNMergeBasedAVX512(larray, l_count, rarray, r_count, cn, cn_count);
#elif SI == 2
  return ComputeCNNaiveStdMerge(larray, l_count, rarray, r_count, cn, cn_count);
#endif
#endif
}

void ComputeSetIntersection::ComputeCandidates(
    const V_ID *larray, const ui l_count, const V_ID *rarray, const ui r_count, ui &cn_count) {
#if HYBRID == 0
#if SI == 0
  if (l_count / 50 > r_count || r_count / 50 > l_count) {
    return ComputeCNGallopingAVX2(larray, l_count, rarray, r_count, cn_count);
  } else {
    return ComputeCNMergeBasedAVX2(larray, l_count, rarray, r_count, cn_count);
  }
#elif SI == 1
  if (l_count / 50 > r_count || r_count / 50 > l_count) {
    return ComputeCNGallopingAVX512(larray, l_count, rarray, r_count, cn_count);
  } else {
    return ComputeCNMergeBasedAVX512(larray, l_count, rarray, r_count, cn_count);
  }
#elif SI == 2
  if (l_count / 50 > r_count || r_count / 50 > l_count) {
    return ComputeCNGalloping(larray, l_count, rarray, r_count, cn_count);
  } else {
    return ComputeCNNaiveStdMerge(larray, l_count, rarray, r_count, cn_count);
  }
#endif
#elif HYBRID == 1
#if SI == 0
  return ComputeCNMergeBasedAVX2(larray, l_count, rarray, r_count, cn_count);
#elif SI == 1
  return ComputeCNMergeBasedAVX512(larray, l_count, rarray, r_count, cn_count);
#elif SI == 2
  return ComputeCNNaiveStdMerge(larray, l_count, rarray, r_count, cn_count);
#endif
#endif
}

#if SI == 0
void ComputeSetIntersection::ComputeCNGallopingAVX2(const V_ID *larray,
                                                    const ui l_count,
                                                    const V_ID *rarray,
                                                    const ui r_count,
                                                    V_ID *cn,
                                                    ui &cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  while (true) {
    while (larray[li] < rarray[ri]) {
      li += 1;
      if (li >= lc) {
        return;
      }
    }

    ri = GallopingSearchAVX2(rarray, ri, rc, larray[li]);
    if (ri >= rc) {
      return;
    }

    if (larray[li] == rarray[ri]) {
      cn[cn_count++] = larray[li];
      li += 1;
      ri += 1;
      if (li >= lc || ri >= rc) {
        return;
      }
    }
  }
}

void ComputeSetIntersection::ComputeCNGallopingAVX2(
    const V_ID *larray, const ui l_count, const V_ID *rarray, const ui r_count, ui &cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  while (true) {
    while (larray[li] < rarray[ri]) {
      li += 1;
      if (li >= lc) {
        return;
      }
    }

    ri = GallopingSearchAVX2(rarray, ri, rc, larray[li]);
    if (ri >= rc) {
      return;
    }

    if (larray[li] == rarray[ri]) {
      cn_count += 1;
      li += 1;
      ri += 1;
      if (li >= lc || ri >= rc) {
        return;
      }
    }
  }
}

void ComputeSetIntersection::ComputeCNMergeBasedAVX2(const V_ID *larray,
                                                     const ui l_count,
                                                     const V_ID *rarray,
                                                     const ui r_count,
                                                     V_ID *cn,
                                                     ui &cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  __m256i per_u_order = _mm256_set_epi32(1, 1, 1, 1, 0, 0, 0, 0);
  __m256i per_v_order = _mm256_set_epi32(3, 2, 1, 0, 3, 2, 1, 0);
  V_ID *cur_back_ptr = cn;

  auto size_ratio = (rc) / (lc);
  if (size_ratio > 2) {
    if (li < lc && ri + 7 < rc) {
      __m256i u_elements = _mm256_set1_epi32(larray[li]);
      __m256i v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));

      while (true) {
        __m256i mask = _mm256_cmpeq_epi32(u_elements, v_elements);
        auto real_mask = _mm256_movemask_epi8(mask);
        if (real_mask != 0) {
          // at most 1 element
          *cur_back_ptr = larray[li];
          cur_back_ptr += 1;
        }
        if (larray[li] > rarray[ri + 7]) {
          ri += 8;
          if (ri + 7 >= rc) {
            break;
          }
          v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));
        } else {
          li++;
          if (li >= lc) {
            break;
          }
          u_elements = _mm256_set1_epi32(larray[li]);
        }
      }
    }
  } else {
    if (li + 1 < lc && ri + 3 < rc) {
      __m256i u_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(larray + li));
      __m256i u_elements_per = _mm256_permutevar8x32_epi32(u_elements, per_u_order);
      __m256i v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));
      __m256i v_elements_per = _mm256_permutevar8x32_epi32(v_elements, per_v_order);

      while (true) {
        __m256i mask = _mm256_cmpeq_epi32(u_elements_per, v_elements_per);
        auto real_mask = _mm256_movemask_epi8(mask);
        if (real_mask << 16 != 0) {
          *cur_back_ptr = larray[li];
          cur_back_ptr += 1;
        }
        if (real_mask >> 16 != 0) {
          *cur_back_ptr = larray[li + 1];
          cur_back_ptr += 1;
        }

        if (larray[li + 1] == rarray[ri + 3]) {
          li += 2;
          ri += 4;
          if (li + 1 >= lc || ri + 3 >= rc) {
            break;
          }
          u_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(larray + li));
          u_elements_per = _mm256_permutevar8x32_epi32(u_elements, per_u_order);
          v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));
          v_elements_per = _mm256_permutevar8x32_epi32(v_elements, per_v_order);
        } else if (larray[li + 1] > rarray[ri + 3]) {
          ri += 4;
          if (ri + 3 >= rc) {
            break;
          }
          v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));
          v_elements_per = _mm256_permutevar8x32_epi32(v_elements, per_v_order);
        } else {
          li += 2;
          if (li + 1 >= lc) {
            break;
          }
          u_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(larray + li));
          u_elements_per = _mm256_permutevar8x32_epi32(u_elements, per_u_order);
        }
      }
    }
  }

  cn_count = (ui)(cur_back_ptr - cn);
  if (li < lc && ri < rc) {
    while (true) {
      while (larray[li] < rarray[ri]) {
        ++li;
        if (li >= lc) {
          return;
        }
      }
      while (larray[li] > rarray[ri]) {
        ++ri;
        if (ri >= rc) {
          return;
        }
      }
      if (larray[li] == rarray[ri]) {
        // write back
        cn[cn_count++] = larray[li];

        ++li;
        ++ri;
        if (li >= lc || ri >= rc) {
          return;
        }
      }
    }
  }
  return;
}

void ComputeSetIntersection::ComputeCNMergeBasedAVX2(
    const V_ID *larray, const ui l_count, const V_ID *rarray, const ui r_count, ui &cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  constexpr int parallelism = 8;

  int cn_countv[parallelism] = {0, 0, 0, 0, 0, 0, 0, 0};
  __m256i sse_cn_countv = _mm256_load_si256(reinterpret_cast<__m256i *>(cn_countv));
  __m256i sse_countplus = _mm256_set1_epi32(1);
  __m256i per_u_order = _mm256_set_epi32(1, 1, 1, 1, 0, 0, 0, 0);
  __m256i per_v_order = _mm256_set_epi32(3, 2, 1, 0, 3, 2, 1, 0);

  auto size_ratio = (rc) / (lc);
  if (size_ratio > 2) {
    if (li < lc && ri + 7 < rc) {
      __m256i u_elements = _mm256_set1_epi32(larray[li]);
      __m256i v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));

      while (true) {
        __m256i mask = _mm256_cmpeq_epi32(u_elements, v_elements);
        mask = _mm256_and_si256(sse_countplus, mask);
        sse_cn_countv = _mm256_add_epi32(sse_cn_countv, mask);
        if (larray[li] > rarray[ri + 7]) {
          ri += 8;
          if (ri + 7 >= rc) {
            break;
          }
          v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));
        } else {
          li++;
          if (li >= lc) {
            break;
          }
          u_elements = _mm256_set1_epi32(larray[li]);
        }
      }
      _mm256_store_si256(reinterpret_cast<__m256i *> cn_countv, sse_cn_countv);
      for (int cn_countvplus : cn_countv) {
        cn_count += cn_countvplus;
      }
    }
  } else {
    if (li + 1 < lc && ri + 3 < rc) {
      __m256i u_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(larray + li));
      __m256i u_elements_per = _mm256_permutevar8x32_epi32(u_elements, per_u_order);
      __m256i v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));
      __m256i v_elements_per = _mm256_permutevar8x32_epi32(v_elements, per_v_order);

      while (true) {
        __m256i mask = _mm256_cmpeq_epi32(u_elements_per, v_elements_per);
        mask = _mm256_and_si256(sse_countplus, mask);
        sse_cn_countv = _mm256_add_epi32(sse_cn_countv, mask);

        if (larray[li + 1] == rarray[ri + 3]) {
          li += 2;
          ri += 4;
          if (li + 1 >= lc || ri + 3 >= rc) {
            break;
          }
          u_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(larray + li));
          u_elements_per = _mm256_permutevar8x32_epi32(u_elements, per_u_order);
          v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));
          v_elements_per = _mm256_permutevar8x32_epi32(v_elements, per_v_order);
        } else if (larray[li + 1] > rarray[ri + 3]) {
          ri += 4;
          if (ri + 3 >= rc) {
            break;
          }
          v_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(rarray + ri));
          v_elements_per = _mm256_permutevar8x32_epi32(v_elements, per_v_order);
        } else {
          li += 2;
          if (li + 1 >= lc) {
            break;
          }
          u_elements = _mm256_loadu_si256(reinterpret_cast<__m256i *>(larray + li));
          u_elements_per = _mm256_permutevar8x32_epi32(u_elements, per_u_order);
        }
      }
    }
    _mm256_store_si256(reinterpret_cast<__m256i *> cn_countv, sse_cn_countv);
    for (int cn_countvplus : cn_countv) {
      cn_count += cn_countvplus;
    }
  }

  if (li < lc && ri < rc) {
    while (true) {
      while (larray[li] < rarray[ri]) {
        ++li;
        if (li >= lc) {
          return;
        }
      }
      while (larray[li] > rarray[ri]) {
        ++ri;
        if (ri >= rc) {
          return;
        }
      }
      if (larray[li] == rarray[ri]) {
        cn_count++;
        ++li;
        ++ri;
        if (li >= lc || ri >= rc) {
          return;
        }
      }
    }
  }
  return;
}

const ui ComputeSetIntersection::BinarySearchForGallopingSearchAVX2(const V_ID *array,
                                                                    ui offset_beg,
                                                                    ui offset_end,
                                                                    ui val) {
  while (offset_end - offset_beg >= 16) {
    auto mid = static_cast<uint32_t>((static_cast<uint64_t>(offset_beg) + offset_end) / 2);
    _mm_prefetch(
        reinterpret_cast<char *>(&array[(static_cast<uint64_t>(mid + 1) + offset_end) / 2]),
        _MM_HINT_T0);
    _mm_prefetch(reinterpret_cast<char *>(&array[(static_cast<uint64_t>(offset_beg) + mid) / 2]),
                 _MM_HINT_T0);
    if (array[mid] == val) {
      return mid;
    } else if (array[mid] < val) {
      offset_beg = mid + 1;
    } else {
      offset_end = mid;
    }
  }

  // linear search fallback, be careful with operator>> && operation+ priority
  __m256i pivot_element = _mm256_set1_epi32(val);
  for (; offset_beg + 7 < offset_end; offset_beg += 8) {
    __m256i elements = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(array + offset_beg));
    __m256i cmp_res = _mm256_cmpgt_epi32(pivot_element, elements);
    int mask = _mm256_movemask_epi8(cmp_res);
    if (mask != 0xffffffff) {
      return offset_beg + (_popcnt32(mask) >> 2);
    }
  }
  if (offset_beg < offset_end) {
    auto left_size = offset_end - offset_beg;
    __m256i elements = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(array + offset_beg));
    __m256i cmp_res = _mm256_cmpgt_epi32(pivot_element, elements);
    int mask = _mm256_movemask_epi8(cmp_res);
    int cmp_mask = 0xffffffff >> ((8 - left_size) << 2);
    mask &= cmp_mask;
    if (mask != cmp_mask) {
      return offset_beg + (_popcnt32(mask) >> 2);
    }
  }
  return offset_end;
}

const ui ComputeSetIntersection::GallopingSearchAVX2(const V_ID *array,
                                                     ui offset_beg,
                                                     ui offset_end,
                                                     ui val) {
  if (array[offset_end - 1] < val) {
    return offset_end;
  }

  // linear search
  __m256i pivot_element = _mm256_set1_epi32(val);
  if (offset_end - offset_beg >= 8) {
    __m256i elements = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(array + offset_beg));
    __m256i cmp_res = _mm256_cmpgt_epi32(pivot_element, elements);
    int mask = _mm256_movemask_epi8(cmp_res);
    if (mask != 0xffffffff) {
      return offset_beg + (_popcnt32(mask) >> 2);
    }
  } else {
    auto left_size = offset_end - offset_beg;
    __m256i elements = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(array + offset_beg));
    __m256i cmp_res = _mm256_cmpgt_epi32(pivot_element, elements);
    int mask = _mm256_movemask_epi8(cmp_res);
    int cmp_mask = 0xffffffff >> ((8 - left_size) << 2);
    mask &= cmp_mask;
    if (mask != cmp_mask) {
      return offset_beg + (_popcnt32(mask) >> 2);
    }
  }

  // galloping, should add pre-fetch later
  auto jump_idx = 8u;
  while (true) {
    auto peek_idx = offset_beg + jump_idx;
    if (peek_idx >= offset_end) {
      return BinarySearchForGallopingSearchAVX2(
          array, (jump_idx >> 1) + offset_beg + 1, offset_end, val);
    }
    if (array[peek_idx] < val) {
      jump_idx <<= 1;
    } else {
      return array[peek_idx] == val
                 ? peek_idx
                 : BinarySearchForGallopingSearchAVX2(
                       array, (jump_idx >> 1) + offset_beg + 1, peek_idx + 1, val);
    }
  }
}

#elif SI == 1
void ComputeSetIntersection::ComputeCNGallopingAVX512(const V_ID *larray,
                                                      const ui l_count,
                                                      const V_ID *rarray,
                                                      const ui r_count,
                                                      V_ID *cn,
                                                      ui &cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  while (true) {
    while (larray[li] < rarray[ri]) {
      li += 1;
      if (li >= lc) {
        return;
      }
    }

    ri = Utility::GallopingSearchAVX512(rarray, ri, rc, larray[li]);
    if (ri >= rc) {
      return;
    }

    if (larray[li] == rarray[ri]) {
      cn[cn_count++] = larray[li];
      li += 1;
      ri += 1;
      if (li >= lc || ri >= rc) {
        return;
      }
    }
  }
}

void ComputeSetIntersection::ComputeCNGallopingAVX512(
    const V_ID *larray, const ui l_count, const V_ID *rarray, const ui r_count, ui &cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  while (true) {
    while (larray[li] < rarray[ri]) {
      li += 1;
      if (li >= lc) {
        return;
      }
    }

    ri = Utility::GallopingSearchAVX512(rarray, ri, rc, larray[li]);
    if (ri >= rc) {
      return;
    }

    if (larray[li] == rarray[ri]) {
      cn_count += 1;
      li += 1;
      ri += 1;
      if (li >= lc || ri >= rc) {
        return;
      }
    }
  }
}

void ComputeSetIntersection::ComputeCNMergeBasedAVX512(const V_ID *larray,
                                                       const ui l_count,
                                                       const V_ID *rarray,
                                                       const ui r_count,
                                                       V_ID *cn,
                                                       ui &cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  __m512i st = _mm512_set_epi32(3, 3, 3, 3, 2, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0);

  V_ID *cur_back_ptr = cn;

  auto size1 = (rc) / (lc);
  if (size1 > 2) {
    if (li < lc && ri + 15 < rc) {
      __m512i u_elements = _mm512_set1_epi32(larray[li]);
      __m512i v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));

      while (true) {
        __mmask16 mask = _mm512_cmpeq_epi32_mask(u_elements, v_elements);
        if (mask != 0x0000) {
          // write back
          _mm512_mask_compressstoreu_epi32(cur_back_ptr, mask, u_elements);
          cur_back_ptr += _popcnt32(mask);
        }

        if (larray[li] > rarray[ri + 15]) {
          ri += 16;
          if (ri + 15 >= rc) {
            break;
          }
          v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));
        } else {
          li += 1;
          if (li >= lc) {
            break;
          }
          u_elements = _mm512_set1_epi32(larray[li]);
        }
      }
    }
  } else {
    if (li + 3 < lc && ri + 3 < rc) {
      __m512i u_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(larray + li));
      __m512i u_elements_per = _mm512_permutevar_epi32(st, u_elements);
      __m512i v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));
      __m512i v_elements_per = _mm512_permute4f128_epi32(v_elements, 0b00000000);

      while (true) {
        __mmask16 mask = _mm512_cmpeq_epi32_mask(u_elements_per, v_elements_per);
        if (mask != 0x0000) {
          // write back
          _mm512_mask_compressstoreu_epi32(cur_back_ptr, mask, u_elements_per);
          cur_back_ptr += _popcnt32(mask);
        }

        if (larray[li + 3] > rarray[ri + 3]) {
          ri += 4;
          if (ri + 3 >= rc) {
            break;
          }
          v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));
          v_elements_per = _mm512_permute4f128_epi32(v_elements, 0b00000000);
        } else if (larray[li + 3] < rarray[ri + 3]) {
          li += 4;
          if (li + 3 >= lc) {
            break;
          }
          u_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(larray + li));
          u_elements_per = _mm512_permutevar_epi32(st, u_elements);
        } else {
          li += 4;
          ri += 4;
          if (li + 3 >= lc || ri + 3 >= rc) {
            break;
          }
          u_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(larray + li));
          u_elements_per = _mm512_permutevar_epi32(st, u_elements);
          v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));
          v_elements_per = _mm512_permute4f128_epi32(v_elements, 0b00000000);
        }
      }
    }
  }

  cn_count = (ui)(cur_back_ptr - cn);

  if (li < lc && ri < rc) {
    while (true) {
      while (larray[li] < rarray[ri]) {
        li += 1;
        if (li >= lc) {
          return;
        }
      }
      while (larray[li] > rarray[ri]) {
        ri += 1;
        if (ri >= rc) {
          return;
        }
      }
      if (larray[li] == rarray[ri]) {
        // write back
        cn[cn_count++] = larray[li];

        li += 1;
        ri += 1;
        if (li >= lc || ri >= rc) {
          return;
        }
      }
    }
  }
  return;
}

void ComputeSetIntersection::ComputeCNMergeBasedAVX512(
    const V_ID *larray, const ui l_count, const V_ID *rarray, const ui r_count, ui &cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  constexpr int parallelism = 16;
  __m512i st = _mm512_set_epi32(3, 3, 3, 3, 2, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0);
  __m512i ssecountplus = _mm512_set1_epi32(1);
  int cn_countv[parallelism] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

  __m512i ssecn_countv = _mm512_set1_epi32(0);
  auto size1 = (rc) / (lc);

  if (size1 > 2) {
    if (li < lc && ri + 15 < rc) {
      __m512i u_elements = _mm512_set1_epi32(larray[li]);
      __m512i v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));

      while (true) {
        __mmask16 mask = _mm512_cmpeq_epi32_mask(u_elements, v_elements);
        ssecn_countv = _mm512_mask_add_epi32(ssecn_countv, mask, ssecn_countv, ssecountplus);

        if (larray[li] > rarray[ri + 15]) {
          ri += 16;
          if (ri + 15 >= rc) {
            break;
          }
          v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));
        } else {
          li += 1;
          if (li >= lc) {
            break;
          }
          u_elements = _mm512_set1_epi32(larray[li]);
        }
      }
      _mm512_storeu_si512(reinterpret_cast<__m512i *> cn_countv, ssecn_countv);
      for (int cn_countvplus : cn_countv) {
        cn_count += cn_countvplus;
      }
    }
  } else {
    if (li + 3 < lc && ri + 3 < rc) {
      __m512i u_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(larray + li));
      __m512i u_elements_per = _mm512_permutevar_epi32(st, u_elements);
      __m512i v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));
      __m512i v_elements_per = _mm512_permute4f128_epi32(v_elements, 0b00000000);

      while (true) {
        __mmask16 mask = _mm512_cmpeq_epi32_mask(u_elements_per, v_elements_per);
        ssecn_countv = _mm512_mask_add_epi32(ssecn_countv, mask, ssecn_countv, ssecountplus);

        if (larray[li + 3] > rarray[ri + 3]) {
          ri += 4;
          if (ri + 3 >= rc) {
            break;
          }
          v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));
          v_elements_per = _mm512_permute4f128_epi32(v_elements, 0b00000000);
        } else if (larray[li + 3] < rarray[ri + 3]) {
          li += 4;
          if (li + 3 >= lc) {
            break;
          }
          u_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(larray + li));
          u_elements_per = _mm512_permutevar_epi32(st, u_elements);
        } else {
          li += 4;
          ri += 4;
          if (li + 3 >= lc || ri + 3 >= rc) {
            break;
          }
          u_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(larray + li));
          u_elements_per = _mm512_permutevar_epi32(st, u_elements);
          v_elements = _mm512_loadu_si512(reinterpret_cast<__m512i *>(rarray + ri));
          v_elements_per = _mm512_permute4f128_epi32(v_elements, 0b00000000);
        }
      }
      _mm512_storeu_si512(reinterpret_cast<__m512i *> cn_countv, ssecn_countv);
      for (int cn_countvplus : cn_countv) {
        cn_count += cn_countvplus;
      }
    }
  }

  if (li < lc && ri < rc) {
    while (true) {
      while (larray[li] < rarray[ri]) {
        li += 1;
        if (li >= lc) {
          return;
        }
      }
      while (larray[li] > rarray[ri]) {
        ri += 1;
        if (ri >= rc) {
          return;
        }
      }
      if (larray[li] == rarray[ri]) {
        cn_count += 1;
        li += 1;
        ri += 1;
        if (li >= lc || ri >= rc) {
          return;
        }
      }
    }
  }
}

#elif SI == 2
void ComputeSetIntersection::ComputeCNNaiveStdMerge(const V_ID* larray,
                                                    const ui l_count,
                                                    const V_ID* rarray,
                                                    const ui r_count,
                                                    V_ID* cn,
                                                    ui& cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  while (true) {
    if (larray[li] < rarray[ri]) {
      li += 1;
      if (li >= lc) {
        return;
      }
    } else if (larray[li] > rarray[ri]) {
      ri += 1;
      if (ri >= rc) {
        return;
      }
    } else {
      cn[cn_count++] = larray[li];

      li += 1;
      ri += 1;
      if (li >= lc || ri >= rc) {
        return;
      }
    }
  }
}

void ComputeSetIntersection::ComputeCNNaiveStdMerge(
    const V_ID* larray, const ui l_count, const V_ID* rarray, const ui r_count, ui& cn_count) {
  cn_count = 0;

  if (l_count == 0 || r_count == 0) return;

  ui lc = l_count;
  ui rc = r_count;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  while (true) {
    if (larray[li] < rarray[ri]) {
      li += 1;
      if (li >= lc) {
        return;
      }
    } else if (larray[li] > rarray[ri]) {
      ri += 1;
      if (ri >= rc) {
        return;
      }
    } else {
      cn_count += 1;
      li += 1;
      ri += 1;
      if (li >= lc || ri >= rc) {
        return;
      }
    }
  }
}

void ComputeSetIntersection::ComputeCNGalloping(const V_ID* larray,
                                                const ui l_count,
                                                const V_ID* rarray,
                                                const ui r_count,
                                                V_ID* cn,
                                                ui& cn_count) {
  ui lc = l_count;
  ui rc = r_count;
  cn_count = 0;
  if (lc == 0 || rc == 0) return;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  while (true) {
    while (larray[li] < rarray[ri]) {
      li += 1;
      if (li >= lc) {
        return;
      }
    }

    ri = GallopingSearch(rarray, ri, rc, larray[li]);
    if (ri >= rc) {
      return;
    }

    if (larray[li] == rarray[ri]) {
      cn[cn_count++] = larray[li];

      li += 1;
      ri += 1;
      if (li >= lc || ri >= rc) {
        return;
      }
    }
  }
}

void ComputeSetIntersection::ComputeCNGalloping(
    const V_ID* larray, const ui l_count, const V_ID* rarray, const ui r_count, ui& cn_count) {
  ui lc = l_count;
  ui rc = r_count;
  cn_count = 0;
  if (lc == 0 || rc == 0) return;

  if (lc > rc) {
    auto tmp = larray;
    larray = rarray;
    rarray = tmp;

    ui tmp_count = lc;
    lc = rc;
    rc = tmp_count;
  }

  ui li = 0;
  ui ri = 0;

  while (true) {
    while (larray[li] < rarray[ri]) {
      li += 1;
      if (li >= lc) {
        return;
      }
    }

    ri = GallopingSearch(rarray, ri, rc, larray[li]);
    if (ri >= rc) {
      return;
    }

    if (larray[li] == rarray[ri]) {
      cn_count += 1;

      li += 1;
      ri += 1;
      if (li >= lc || ri >= rc) {
        return;
      }
    }
  }
}

ui ComputeSetIntersection::GallopingSearch(const V_ID* src,
                                           const ui begin,
                                           const ui end,
                                           const ui target) {
  if (src[end - 1] < target) {
    return end;
  }
  // galloping
  if (src[begin] >= target) {
    return begin;
  }
  if (src[begin + 1] >= target) {
    return begin + 1;
  }
  if (src[begin + 2] >= target) {
    return begin + 2;
  }

  ui jump_idx = 4;
  ui offset_beg = begin;
  while (true) {
    ui peek_idx = offset_beg + jump_idx;
    if (peek_idx >= end) {
      return BinarySearch(src, (jump_idx >> 1) + offset_beg + 1, end, target);
    }
    if (src[peek_idx] < target) {
      jump_idx <<= 1;
    } else {
      return src[peek_idx] == target
                 ? peek_idx
                 : BinarySearch(src, (jump_idx >> 1) + offset_beg + 1, peek_idx + 1, target);
    }
  }
}

ui ComputeSetIntersection::BinarySearch(const V_ID* src,
                                        const ui begin,
                                        const ui end,
                                        const ui target) {
  int offset_begin = begin;
  int offset_end = end;
  while (offset_end - offset_begin >= 16) {
    auto mid = static_cast<uint32_t>((static_cast<uint64_t>(offset_begin) + offset_end) / 2);
    _mm_prefetch(reinterpret_cast<const char*>(&src[(mid + 1 + offset_end) / 2]), _MM_HINT_T0);
    _mm_prefetch(reinterpret_cast<const char*>(&src[(mid - 1 + offset_begin) / 2]), _MM_HINT_T0);
    if (src[mid] == target) {
      return mid;
    } else if (src[mid] < target) {
      offset_begin = mid + 1;
    } else {
      offset_end = mid;
    }
  }

  // linear search fallback
  for (auto offset = offset_begin; offset < offset_end; ++offset) {
    if (src[offset] >= target) {
      return static_cast<ui>(offset);
    }
  }

  return static_cast<ui>(offset_end);
}
#endif
