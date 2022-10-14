// Copyright [2022] <nebula>
#ifndef CECI_TYPES_H
#define CECI_TYPES_H

#include <cstdint>
#include <cstdlib>

using ui = unsigned int;  // u is the query node.

using V_ID = ui;
using L_ID = ui;

class TreeNode {
 public:
  V_ID id;
  V_ID parent;
  ui level;
  // ui under_level_count;
  ui children_count;
  // V_ID* under_level;
  V_ID* children;

  // This field tracks the nodes that should participate in the intersection to generate the
  // candidates for node "id". A.k.a, the nodes that are visited earlier than node "id", and should
  // be intersected to generate the candidates for "node id". Note, this field excludes the "parent
  // node" of node "id".
  ui bn_count;
  V_ID* bn;

  ui fn_count;
  V_ID* fn;
  size_t estimated_embeddings_num;

 public:
  TreeNode() {
    id = 0;
    // under_level = nullptr;
    bn = nullptr;
    fn = nullptr;
    children = nullptr;
    parent = 99999;
    level = 0;
    // under_level_count = 0;
    children_count = 0;
    bn_count = 0;
    fn_count = 0;
    estimated_embeddings_num = 0;
  }

  ~TreeNode() {
    // delete[] under_level;
    delete[] bn;
    delete[] fn;
    delete[] children;
  }

  void initialize(const ui size) {
    // under_level = new V_ID[size];
    bn = new V_ID[size];
    fn = new V_ID[size];
    children = new V_ID[size];
  }
};

class Edges {
 public:
  ui* offset;
  ui* edge;
  ui v_count;
  ui e_count;
  ui max_degree;

 public:
  Edges() {
    offset = nullptr;
    edge = nullptr;
    v_count = 0;
    e_count = 0;
    max_degree = 0;
  }

  ~Edges() {
    delete[] offset;
    delete[] edge;
  }
};

#endif  // CECI_TYPES_H
