// Copyright [2022] <nebula>
#ifndef SUBGRAPH_H
#define SUBGRAPH_H

#include <vector>

#include "graph.h"
#include "trees.h"

bool CECIFunction(Graph *data_graph,
                  Graph *query_graph,
                  ui **&candidates,
                  ui *&candidates_count,
                  ui *&order,
                  ui *&provenance,
                  TreeNode *&tree,
                  std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Candidates,
                  std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Provenance);

// static void FilterProvenance(std::unordered_map<L_ID, ui> *u_nlf,
//                        ui u_l,
//                        ui u_d,
//                        V_ID &u,
//                        V_ID &v,
//                        std::unordered_map<V_ID, V_ID> &Provenance,
//                        Graph *data_graph,
//                        std::vector<ui> &data_visited,
//                        ui **&candidates,
//                        ui *&candidates_count);
// static void Insertion(V_ID u,
//                V_ID &tmp_first,
//                V_ID &tmp_second,
//                std::unordered_map<V_ID, std::vector<std::pair<V_ID, V_ID>>> &intersetion);

// static V_ID ParentNode(V_ID first, V_ID second, TreeNode *&tree);

// static void Initial(Graph *data_graph, Graph *query_graph, ui &candidates, ui
// *&candidates_count); static V_ID InitialStartVertex(Graph *data_graph, Graph *query_graph);

void ZFComputeNLF(Graph *data_graph, Graph *query_graph, V_ID i, ui &count, ui *tmp = nullptr);

#endif  // SUBGRAPH_H
