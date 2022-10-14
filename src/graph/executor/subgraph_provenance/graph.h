// Copyright [2022] <nebula>
#ifndef CECI_GRAPH_H
#define CECI_GRAPH_H
#include <iostream>
#include <unordered_map>
#include <vector>

#include "trees.h"

class Graph {
 private:
  ui v_count;  // vertices count
  ui e_count;  // edges count
  ui l_count;  // Label count
  ui max_degree;
  ui max_label_frequency;

  ui* offsets;

  V_ID* neighbors;
  L_ID* labels;

  ui* reverse_index_offsets;
  ui* reverse_index;

  std::unordered_map<L_ID, ui> labels_frequency;
  // ui* labels_offsets;
  std::unordered_map<L_ID, ui>* nlf;
  std::unordered_map<L_ID, std::vector<ui>>* nl;

 private:
  void BuildReverseIndex();
  void BuildNLCF();
  void BuildLabelOffset();

 public:
  Graph() {
    v_count = 0;  // vertices count
    e_count = 0;  // edges count
    l_count = 0;  // Label count
    max_degree = 0;
    max_label_frequency = 0;

    offsets = nullptr;
    reverse_index_offsets = nullptr;
    reverse_index = nullptr;
    neighbors = nullptr;
    labels = nullptr;

    labels_frequency.clear();
    // labels_offsets = nullptr;
    nlf = nullptr;
    nl = nullptr;
  }
  ~Graph() {
    delete[] offsets;
    delete[] neighbors;
    delete[] reverse_index_offsets;
    delete[] reverse_index;
    delete[] labels;
    // delete[] labels_offsets;
    delete[] nlf;
    delete[] nl;
  }

 public:
  void loadGraph(const std::string& file_path);
  void loadGraphFromExecutor(unsigned int v_count,
                             unsigned int l_count,
                             unsigned int e_count,
                             unsigned int* offset,
                             unsigned int* neighbors,
                             unsigned int* labels);

  void printGraph();

  ui getLabelsCount() {
    return l_count;
  }

  ui getGraphMaxLabelFrequency() {
    return max_label_frequency;
  }

  ui* getVerticesByLabel(const L_ID id, ui& count) const {
    count = reverse_index_offsets[id + 1] - reverse_index_offsets[id];
    return reverse_index + reverse_index_offsets[id];
  }

  ui getVertexDegree(const V_ID id) const {
    return offsets[id + 1] - offsets[id];
  }

  ui getVerticesCount() {
    return v_count;
  }

  ui getEdgesCount() {
    return e_count;
  }

  ui getMaxDegree() {
    return max_degree;
  }

  L_ID getVertexLabel(const V_ID id) {
    return labels[id];
  }

  ui* getVertexNeighbors(const V_ID id, ui& count) {
    count = offsets[id + 1] - offsets[id];
    return neighbors + offsets[id];
  }

  bool checkEdgeExistence(V_ID u, V_ID v) {
    if (getVertexDegree(u) < getVertexDegree(v)) {
      std::swap(u, v);
    }

    ui count = 0;
    neighbors = getVertexNeighbors(v, count);

    int begin = 0;
    int end = count - 1;
    while (begin <= end) {
      int mid = begin + ((end - begin) >> 1);
      if (neighbors[mid] == u) {
        return true;
      } else if (neighbors[mid] > u) {
        end = mid - 1;
      } else {
        begin = mid + 1;
      }
    }

    return false;
  }

  std::unordered_map<L_ID, ui>* getVertexNLF(V_ID i) {
    return nlf + i;
  }

  std::unordered_map<L_ID, std::vector<ui>>* getVertexNL(V_ID i) {
    return nl + i;
  }
};

#endif
