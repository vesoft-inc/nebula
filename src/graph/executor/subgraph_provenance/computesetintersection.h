// Copyright [2022] <nebula>
//
// Created by ssunah on 11/30/17.
//

#ifndef SUBGRAPHMATCHING_COMPUTE_SET_INTERSECTION_H
#define SUBGRAPHMATCHING_COMPUTE_SET_INTERSECTION_H

#include <immintrin.h>
#include <x86intrin.h>

#include "config.h"
#include "graph.h"

/*
 * Because the set intersection is designed for computing common neighbors, the target is uieger.
 */

class ComputeSetIntersection {
 public:
#if HYBRID == 0
  static size_t galloping_cnt_;
  static size_t merge_cnt_;
#endif

  static void ComputeCandidates(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, V_ID* cn, ui& cn_count);
  static void ComputeCandidates(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, ui& cn_count);

#if SI == 0
  static void ComputeCNGallopingAVX2(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, V_ID* cn, ui& cn_count);
  static void ComputeCNGallopingAVX2(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, ui& cn_count);

  static void ComputeCNMergeBasedAVX2(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, V_ID* cn, ui& cn_count);
  static void ComputeCNMergeBasedAVX2(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, ui& cn_count);
  static const ui BinarySearchForGallopingSearchAVX2(const V_ID* array,
                                                     ui offset_beg,
                                                     ui offset_end,
                                                     ui val);
  static const ui GallopingSearchAVX2(const V_ID* array, ui offset_beg, ui offset_end, ui val);
#elif SI == 1

  static void ComputeCNGallopingAVX512(const V_ID* larray,
                                       const ui l_count,
                                       const V_ID* rarray,
                                       const ui r_count,
                                       V_ID* cn,
                                       ui& cn_count);
  static void ComputeCNGallopingAVX512(
      const V_ID* larray, const ui l_count, const V_ID* rarray, const ui r_count, ui& cn_count);

  static void ComputeCNMergeBasedAVX512(const V_ID* larray,
                                        const ui l_count,
                                        const V_ID* rarray,
                                        const ui r_count,
                                        V_ID* cn,
                                        ui& cn_count);
  static void ComputeCNMergeBasedAVX512(
      const V_ID* larray, const ui l_count, const V_ID* rarray, const ui r_count, ui& cn_count);

#elif SI == 2

  static void ComputeCNNaiveStdMerge(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, V_ID* cn, ui& cn_count);
  static void ComputeCNNaiveStdMerge(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, ui& cn_count);

  static void ComputeCNGalloping(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, V_ID* cn, ui& cn_count);
  static void ComputeCNGalloping(
      const V_ID* larray, ui l_count, const V_ID* rarray, ui r_count, ui& cn_count);
  static ui GallopingSearch(const V_ID* src, ui begin, ui end, ui target);
  static ui BinarySearch(const V_ID* src, ui begin, ui end, ui target);

#endif
};

#endif  // FSE_COMPUTESETINTERSECTION_H
