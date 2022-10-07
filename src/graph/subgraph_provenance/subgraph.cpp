// Copyright [2022] <nebula>
#include "subgraph.h"

#include <memory.h>
#include <omp.h>
#include <pthread.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "computesetintersection.h"
#include "trees.h"
#define INVALID_VERTEX_ID 99999
#undef DEBUG
#define DEBUG 0
#define Q_LIMIT (((int64_t)1) << 31)

V_ID InitialStartVertex(Graph *data_graph, Graph *query_graph) {
  // Using method of finding minimum scores = count/degree;
  double min_score = data_graph->getVerticesCount();
  V_ID start_vertex = 0;

  for (ui i = 0; i < query_graph->getVerticesCount(); ++i) {
    ui degree = query_graph->getVertexDegree(i);
    ui count = 0;
    // Compare the NLF with data graoh and query graph.
    ZFComputeNLF(data_graph, query_graph, i, count);
    double cur_score = count / static_cast<double>(degree);
    if (cur_score < min_score && count > 1) {
      min_score = cur_score;
      start_vertex = i;
    }
  }
  return start_vertex;
}

void bfs2(Graph *graph,
          V_ID root_v,
          TreeNode *&tree,
          V_ID *&order,
          __attribute__((unused)) V_ID *&provenance) {
  // How many vertices.
  ui count = graph->getVerticesCount();
  std::queue<V_ID> queue;
  std::vector<bool> visited(count, false);
  // Initialize the tree
  tree = new TreeNode[count];
  for (ui i = 0; i < count; ++i) {
    tree[i].initialize(count);
  }
  // This is
  order = new V_ID[count];
  ui visited_count = 0;  // how many nodes have been visited.
  queue.push(root_v);    // push the first root node into the BFS queue.
  visited[root_v] = true;
  tree[root_v].level = 0;
  tree[root_v].id = root_v;  // node id.
  order[visited_count++] = root_v;

  ui u_nbrs_count_1;
  // How many neighbors of the node
  // get the neighbors informations
  V_ID *u_nbrs_1 = graph->getVertexNeighbors(root_v, u_nbrs_count_1);
  std::vector<std::pair<ui, ui>> tmp_2;
  for (ui i = 0; i < u_nbrs_count_1; ++i) {
    V_ID u_nbr = u_nbrs_1[i];
    ui u_nbrs_count;
    __attribute__((unused)) V_ID *u_nbrs = graph->getVertexNeighbors(u_nbr, u_nbrs_count);
    tmp_2.emplace_back(u_nbrs_count, u_nbr);
  }

  // Second node ->
  sort(tmp_2.begin(), tmp_2.end());
  ui u_nbr = tmp_2[tmp_2.size() - 1].second;

  if (!visited[u_nbr]) {
    visited[u_nbr] = true;
    tree[u_nbr].id = u_nbr;
    tree[u_nbr].parent = root_v;
    tree[u_nbr].level = tree[root_v].level + 1;
    tree[root_v].children[tree[root_v].children_count++] = u_nbr;
    order[visited_count++] = u_nbr;
  }
  // Third node ->
  if (count > 2) {
    u_nbr = tmp_2[tmp_2.size() - 2].second;

    if (!visited[u_nbr]) {
      visited[u_nbr] = true;
      tree[u_nbr].id = u_nbr;
      tree[u_nbr].parent = root_v;
      tree[u_nbr].level = tree[root_v].level + 1;
      tree[root_v].children[tree[root_v].children_count++] = u_nbr;
      order[visited_count++] = u_nbr;
    }
  }
  bool next = true;
  while (next) {
    std::vector<std::pair<ui, ui>> tmp;
    next = false;
    for (ui i = 0; i < count; ++i) {
      if (visited[i] == false) {
        next = true;
        ui u_nbrs_count;
        V_ID *u_nbrs = graph->getVertexNeighbors(i, u_nbrs_count);
        ui u_count = 0;
        for (ui j = 0; j < u_nbrs_count; j++) {
          if (visited[u_nbrs[j]] == true) {
            u_count += 1;
          }
        }
        tmp.emplace_back(u_count, i);
      }
    }
    if (next) {
      sort(tmp.begin(), tmp.end());
      ui u_t = tmp[tmp.size() - 1].second;
      order[visited_count++] = u_t;
      visited[u_t] = true;
      ui u_nbrs_count;
      V_ID *u_nbrs = graph->getVertexNeighbors(u_t, u_nbrs_count);
      // ui u_count = 0;
      bool find_child = false;
      for (ui i = 0; i < visited_count; i++) {
        for (ui j = 0; j < u_nbrs_count; j++) {
          if (order[i] == u_nbrs[j]) {
            find_child = true;
            tree[u_t].id = u_t;
            tree[u_t].parent = order[i];
            tree[u_t].level = tree[order[i]].level + 1;
            tree[order[i]].children[tree[order[i]].children_count++] = u_t;
            break;
          }
        }
        if (find_child) {
          break;
        }
      }
    }
  }
}

void generateValidCandidates(Graph *data_graph,
                             ui depth,
                             ui *embedding,
                             ui *idx_count,
                             ui **valid_candidate,
                             bool *visited_vertices,
                             ui **bn,
                             ui *bn_cnt,
                             ui *order,
                             ui **candidates,
                             ui *candidates_count) {
  V_ID u = order[depth];
  idx_count[depth] = 0;
  for (ui i = 0; i < candidates_count[u]; ++i) {
    V_ID v = candidates[u][i];

    if (!visited_vertices[v]) {
      bool valid = true;

      for (ui j = 0; j < bn_cnt[depth]; ++j) {
        V_ID u_nbr = bn[depth][j];
        V_ID u_nbr_v = embedding[u_nbr];

        if (!data_graph->checkEdgeExistence(v, u_nbr_v)) {
          valid = false;
          break;
        }
      }

      if (valid) {
        valid_candidate[depth][idx_count[depth]++] = v;
      }
    }
  }
}

int64_t exploreGraphQLStyle(Graph *data_graph,
                            Graph *query_graph,
                            ui **candidates,
                            ui *candidates_count,
                            ui *order,
                            size_t output_limit_num,
                            int64_t &call_count) {
  uint64_t embedding_cnt = 0;
  int cur_depth = 0;
  int max_depth = query_graph->getVerticesCount();
  V_ID start_vertex = order[0];

  // Generate the bn.
  ui **bn;
  ui *bn_count;

  bn = new ui *[max_depth];
  for (int i = 0; i < max_depth; ++i) {
    bn[i] = new ui[max_depth];
  }

  bn_count = new ui[max_depth];
  std::fill(bn_count, bn_count + max_depth, 0);

  std::vector<bool> visited_query_vertices(max_depth, false);
  visited_query_vertices[start_vertex] = true;
  for (int i = 1; i < max_depth; ++i) {
    V_ID cur_vertex = order[i];
    ui nbr_cnt;
    V_ID *nbrs = query_graph->getVertexNeighbors(cur_vertex, nbr_cnt);

    for (ui j = 0; j < nbr_cnt; ++j) {
      V_ID nbr = nbrs[j];

      if (visited_query_vertices[nbr]) {
        bn[i][bn_count[i]++] = nbr;
      }
    }

    visited_query_vertices[cur_vertex] = true;
  }

  // Allocate the memory buffer.
  ui *idx;
  ui *idx_count;
  ui *embedding;
  V_ID **valid_candidate;
  bool *visited_vertices;

  idx = new ui[max_depth];
  idx_count = new ui[max_depth];
  embedding = new ui[max_depth];
  visited_vertices = new bool[data_graph->getVerticesCount()];
  std::fill(visited_vertices, visited_vertices + data_graph->getVerticesCount(), false);
  valid_candidate = new ui *[max_depth];

  for (int i = 0; i < max_depth; ++i) {
    V_ID cur_vertex = order[i];
    ui max_candidate_count = candidates_count[cur_vertex];
    valid_candidate[i] = new V_ID[max_candidate_count];
  }

  idx[cur_depth] = 0;
  idx_count[cur_depth] = candidates_count[start_vertex];
  std::copy(candidates[start_vertex],
            candidates[start_vertex] + candidates_count[start_vertex],
            valid_candidate[cur_depth]);

  while (true) {
    while (idx[cur_depth] < idx_count[cur_depth]) {
      V_ID u = order[cur_depth];
      V_ID v = valid_candidate[cur_depth][idx[cur_depth]];
      embedding[u] = v;
      visited_vertices[v] = true;
      idx[cur_depth] += 1;

      if (cur_depth == max_depth - 1) {
        embedding_cnt += 1;
        visited_vertices[v] = false;
        if (embedding_cnt >= output_limit_num) {
          goto EXIT;
        }
      } else {
        call_count += 1;
        cur_depth += 1;
        idx[cur_depth] = 0;
        generateValidCandidates(data_graph,
                                cur_depth,
                                embedding,
                                idx_count,
                                valid_candidate,
                                visited_vertices,
                                bn,
                                bn_count,
                                order,
                                candidates,
                                candidates_count);
      }
    }

    cur_depth -= 1;
    if (cur_depth < 0)
      break;
    else
      visited_vertices[embedding[order[cur_depth]]] = false;
  }

// Release the buffer.
EXIT:
  delete[] bn_count;
  delete[] idx;
  delete[] idx_count;
  delete[] embedding;
  delete[] visited_vertices;
  for (int i = 0; i < max_depth; ++i) {
    delete[] bn[i];
    delete[] valid_candidate[i];
  }
  delete[] bn;
  delete[] valid_candidate;
  return embedding_cnt;
}

void bfs(Graph *graph,
         V_ID root_v,
         TreeNode *&tree,
         V_ID *&order,
         __attribute__((unused)) V_ID *&provenance) {
  // How many vertices.
  ui count = graph->getVerticesCount();
  std::queue<V_ID> queue;
  std::vector<bool> visited(count, false);
  // Initialize the tree
  tree = new TreeNode[count];
  for (ui i = 0; i < count; ++i) {
    tree[i].initialize(count);
  }
  // This is
  order = new V_ID[count];
  ui visited_count = 0;  // how many nodes have been visited.
  queue.push(root_v);    // push the first root node into the BFS queue.
  visited[root_v] = true;
  tree[root_v].level = 0;
  tree[root_v].id = root_v;  // node id.
  while (!queue.empty()) {
    V_ID u = queue.front();
    queue.pop();
    order[visited_count++] = u;  // records this as the next order.
    ui u_nbrs_count;
    // How many neighbors of the node
    // get the neighbors informations
    V_ID *u_nbrs = graph->getVertexNeighbors(u, u_nbrs_count);
    for (ui i = 0; i < u_nbrs_count; ++i) {
      V_ID u_nbr = u_nbrs[i];
      // for each unvisited neighbors, if unvisited:
      // (1) push in th query
      // (2) build it in the tree
      // (3) record it's parent-id
      // (4) the level should add 1
      // (5) the parent node will add this node as child.
      if (!visited[u_nbr]) {
        queue.push(u_nbr);
        visited[u_nbr] = true;
        tree[u_nbr].id = u_nbr;
        tree[u_nbr].parent = u;
        tree[u_nbr].level = tree[u].level + 1;
        tree[u].children[tree[u].children_count++] = u_nbr;
      }
    }
  }
}

void Insertion(V_ID u,
               V_ID &tmp_first,
               V_ID &tmp_second,
               std::unordered_map<V_ID, std::vector<std::pair<V_ID, V_ID>>> &intersetion) {
  /*if (tmp_first > tmp_second) {
          V_ID tmp = tmp_first;
          tmp_first = tmp_second;
          tmp_second = tmp;
  }*/
  std::pair<V_ID, V_ID> p1(tmp_first, tmp_second);
  // change to the vector next time.
  if (intersetion.find(u) == intersetion.end()) {
    std::vector<std::pair<V_ID, V_ID>> q;
    q.push_back(p1);
    intersetion.insert(std::pair<V_ID, std::vector<std::pair<V_ID, V_ID>>>(u, q));
  } else {
    intersetion[u].push_back(p1);
  }
}

V_ID ParentNode(V_ID first, V_ID second, TreeNode *&tree) {
  std::vector<V_ID> frontier;
  frontier.push_back(first);
  // std::cout <<"(" << first << ","<< second << ")"<< std::endl;
  while (tree[first].parent != 99999) {
    first = tree[first].parent;
    frontier.push_back(first);
  }
  // std::cout <<"2: and size: " <<  frontier.size() << " " <<  second << std::endl;
  // std::cout <<tree[second].parent  << std::endl;
  if (std::find(frontier.begin(), frontier.end(), second) != frontier.end()) {
    // std::cout <<"Found->second-> " << std::endl;
    return second;
  }
  while (tree[second].parent != 99999) {
    second = tree[second].parent;
    // std::cout <<"parent-> " <<  second << std::endl;
    if (std::find(frontier.begin(), frontier.end(), second) != frontier.end()) {
      // std::cout <<"Found->second->parent-> " << std::endl;
      return second;
    }
  }
  // std::cout <<"End of ParentNode" << std::endl;
  return 99999;
}

void add_provenance(std::unordered_map<V_ID, std::vector<V_ID>> &query_provenance,
                    V_ID &query_node,
                    V_ID &provenance_node) {
  if (query_provenance.find(query_node) != query_provenance.end()) {
    // insert new value and key in the query_provenance.
    std::vector<V_ID>::iterator iter;
    iter = find(
        query_provenance[query_node].begin(), query_provenance[query_node].end(), provenance_node);
    if (iter == query_provenance[query_node].end()) {
      query_provenance[query_node].push_back(provenance_node);
    }
  } else {
    std::unordered_map<V_ID, std::vector<V_ID>>::iterator iter;
    iter = query_provenance.begin();
    std::vector<V_ID> node_pro;
    node_pro.push_back(provenance_node);
    query_provenance.insert(iter, std::pair<V_ID, std::vector<V_ID>>(query_node, node_pro));
  }
}

int64_t enumeration_ress_bk(
    bool *visited,
    int64_t &res_all,
    std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Candidates,
    std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &NTE_Candidates,
    V_ID *order,
    TreeNode *tree,
    V_ID *res,
    ui current_order,
    ui &query_count,
    Graph *query_graph,
    Graph *data_graph,
    std::vector<ui> &order_index,
    V_ID *connection,
    V_ID *offset,
    ui **candidates_2,
    ui **parent_offset,
    ui *candidates_l,
    ui *parent_l) {
  V_ID u = order[current_order];
  V_ID v_f = res[order_index[tree[u].parent]];
  std::vector<V_ID> local_c = P_Candidates[u][v_f];
  int64_t total = 0;
  bool over_all;
  if (current_order == query_count) {
    for (unsigned int j : local_c) {
      if (visited[j]) {
        continue;
      }
      over_all = true;
      res_all += 1;
      ui data_nbrs_count_1;
      V_ID *data_nbrs_1 = data_graph->getVertexNeighbors(j, data_nbrs_count_1);
      // std::unordered_map<L_ID, std::vector<ui>>* v_nl = data_graph->getVertexNL(local_c[j]);
      for (ui k = offset[current_order - 1]; k < offset[current_order]; k++) {
        // L_ID l = query_graph->getVertexLabel(order[connection[k]]);
        if (std::find(data_nbrs_1, data_nbrs_1 + data_nbrs_count_1, res[connection[k]]) ==
            data_nbrs_1 + data_nbrs_count_1) {
          over_all = false;
          break;
        }
      }
      if (over_all == true) {
        total += 1;
      }
    }

    return total;
  } else {
    for (unsigned int j : local_c) {
      // V_ID v = local_c[j];
      if (visited[j]) {
        continue;
      }
      over_all = true;
      res_all += 1;
      ui data_nbrs_count_1;
      V_ID *data_nbrs_1 = data_graph->getVertexNeighbors(j, data_nbrs_count_1);

      for (ui k = offset[current_order - 1]; k < offset[current_order]; k++) {
        if (std::find(data_nbrs_1, data_nbrs_1 + data_nbrs_count_1, res[connection[k]]) ==
            data_nbrs_1 + data_nbrs_count_1) {
          over_all = false;
          break;
        }
      }
      if (over_all == true) {
        visited[j] = true;
        res[current_order] = j;
        total += enumeration_ress_bk(visited,
                                     res_all,
                                     P_Candidates,
                                     NTE_Candidates,
                                     order,
                                     tree,
                                     res,
                                     current_order + 1,
                                     query_count,
                                     query_graph,
                                     data_graph,
                                     order_index,
                                     connection,
                                     offset,
                                     candidates_2,
                                     parent_offset,
                                     candidates_l,
                                     parent_l);
        visited[j] = false;
      }
    }
    return total;
  }
}

void visit_pre_3(__attribute__((unused)) bool *visited,
                 ui **candidates_2,
                 int64_t **parent_offset,
                 __attribute__((unused)) int64_t *candidates_l,
                 V_ID &current_order,
                 __attribute__((unused)) V_ID &p_order,
                 int64_t &k,
                 V_ID *res,
                 __attribute__((unused)) bool &run) {
  int64_t j = k;
  res[current_order] = candidates_2[current_order][j];

  visited[candidates_2[current_order][j]] = true;

  for (int i = current_order; i > 0; i--) {
    j = parent_offset[i][j];
    visited[candidates_2[i - 1][j]] = true;
    res[i - 1] = candidates_2[i - 1][j];
  }
}

void visit_pre(__attribute__((unused)) bool *visited,
               ui **candidates_2,
               int64_t **parent_offset,
               __attribute__((unused)) int64_t *candidates_l,
               V_ID &current_order,
               __attribute__((unused)) V_ID &p_order,
               int64_t &k,
               V_ID *res) {
  int64_t j = k;
  visited[candidates_2[current_order - 1][j]] = true;
  res[current_order - 1] = candidates_2[current_order - 1][j];

  for (int i = current_order - 1; i > 0; i--) {
    j = parent_offset[i][j];
    visited[candidates_2[i - 1][j]] = true;
    res[i - 1] = candidates_2[i - 1][j];
  }
}

void visit_pre_2(__attribute__((unused)) bool *visited,
                 ui **candidates_2,
                 int64_t **parent_offset,
                 __attribute__((unused)) int64_t *candidates_l,
                 V_ID &current_order,
                 __attribute__((unused)) V_ID &p_order,
                 int64_t &k,
                 V_ID *res) {
  int64_t j = k;
  res[current_order - 1] = candidates_2[current_order - 1][j];

  for (int i = current_order - 1; i > 0; i--) {
    j = parent_offset[i][j];
    res[i - 1] = candidates_2[i - 1][j];
  }
}

void visit_can(bool *visited,
               ui **candidates_2,
               int64_t **parent_offset,
               __attribute__((unused)) int64_t *candidates_l,
               V_ID &current_order,
               __attribute__((unused)) V_ID &p_order,
               int64_t &k) {
  int64_t j = k;
  visited[candidates_2[current_order - 1][j]] = false;

  for (int i = current_order - 1; i > 0; i--) {
    j = parent_offset[i][j];
    visited[candidates_2[i - 1][j]] = false;
  }
}

ui &find_v_f(__attribute__((unused)) ui **candidates_2,
             __attribute__((unused)) int64_t **parent_offset,
             __attribute__((unused)) int64_t *candidates_l,
             V_ID &current_order,
             __attribute__((unused)) V_ID &p_order,
             int64_t &k) {
  if (current_order - 1 == p_order) {
    return candidates_2[p_order][k];
  } else {
    int64_t j = k;
    for (ui i = current_order - 1; i > p_order; i--) {
      j = parent_offset[i][j];
    }
    return candidates_2[p_order][j];
  }
}
void find_children_range(int64_t **children_1,
                         int64_t *candidates_l,
                         V_ID &ord_count,
                         V_ID &current_count,
                         int64_t &start,
                         int64_t &end,
                         int64_t &p) {
  // start and end.
  // std::cout <<"###sss" << std::endl;
  if (current_count + 1 == ord_count) {
    start = p + 0;
    end = p + 1;
  } else {
    start = children_1[current_count][p];
    if (p >= candidates_l[current_count]) {
      end = start;
      return;
    }
    end = children_1[current_count][p + 1];
    for (ui i = current_count + 1; i < ord_count - 1; i++) {
      start = children_1[i][start];
      if (end < candidates_l[i]) {
        end = children_1[i][end];
      } else {
        end = children_1[i][candidates_l[i]];
      }
    }
  }
}
int64_t morphism_next(bool *visited_j,
                      bool *&visited,
                      int64_t **children_offset_2,
                      int64_t *candidates_l_2,
                      ui q_add,
                      int64_t &j,
                      ui order_count_2,
                      std::vector<std::pair<ui, ui>> &i_e,
                      ui t,
                      int64_t *start_2,
                      int64_t *end_2,
                      ui **res_2) {
  t += 1;
  int64_t total = 0;
  ui order_s = q_add - 1;
  if (t < i_e.size()) {
    ui q = i_e[t].second;
    q_add = q + 1;
    find_children_range(children_offset_2, candidates_l_2, q_add, order_s, start_2[t], end_2[t], j);
    for (int64_t jj = start_2[t]; jj < end_2[t]; jj++) {
      if (visited[res_2[q][jj]] == false) {
        if (q_add == order_count_2) {
          // std::cout <<"333" << std::endl;
          total += 1;
        } else {
          total += morphism_next(visited_j,
                                 visited,
                                 children_offset_2,
                                 candidates_l_2,
                                 q_add,
                                 jj,
                                 order_count_2,
                                 i_e,
                                 t,
                                 start_2,
                                 end_2,
                                 res_2);
          // if (total != 0) {
          // std::cout <<total << "," << start_2[t+1] << "," << end_2[t+1] << std::endl;
          // }
        }
      }
    }
    return total;
  } else {
    find_children_range(
        children_offset_2, candidates_l_2, order_count_2, order_s, start_2[t], end_2[t], j);
    return (end_2[t] - start_2[t]);
  }
}
int64_t edge_next(V_ID **res_2_r,
                  bool *visited_j,
                  ui *visit_local_4,
                  ui &data_count,
                  ui *con_2,
                  bool *visited,
                  int64_t **children_offset_2,
                  int64_t *candidates_l_2,
                  ui q_add,
                  int64_t &j,
                  ui order_count_2,
                  std::vector<std::pair<ui, ui>> &i_e,
                  std::vector<ui> q_all,
                  ui t,
                  int64_t *start_2,
                  int64_t *end_2,
                  ui **res_2,
                  ui q_1,
                  int64_t &call_count_2) {
  t += 1;
  bool all_exist = false;
  int64_t total_2 = 0;
  ui order_s = q_add - 1;
  if (t < q_all.size()) {
    ui q = q_all[t];
    q_add = q + 1;
    int64_t tmp = data_count * q;
    // ui* & visit_local_5 = visit_local_4[tmp];
    // std::cout <<order_s << "," << q_add << std::endl;
    find_children_range(children_offset_2, candidates_l_2, q_add, order_s, start_2[t], end_2[t], j);
    if (q_add == order_count_2) {
      // std::cout <<"no" << std::endl;
      for (int64_t jj = start_2[t]; jj < end_2[t]; jj++) {
        V_ID *res_t_2 = res_2_r[jj];
        call_count_2++;
        if (visit_local_4[tmp + res_t_2[q]] == con_2[q]) {
          all_exist = true;
          for (auto &t_2 : i_e) {
            ui q_3 = t_2.second;
            if (visited[res_t_2[q_3]]) {
              all_exist = false;
              break;
            }
          }
          total_2 += all_exist;
        }
      }
      return total_2;
    } else {
      for (int64_t jj = start_2[t]; jj < end_2[t]; jj++) {
        call_count_2++;
        if (visit_local_4[tmp + res_2[q][jj]] == con_2[q]) {
          total_2 += edge_next(res_2_r,
                               visited_j,
                               visit_local_4,
                               data_count,
                               con_2,
                               visited,
                               children_offset_2,
                               candidates_l_2,
                               q_add,
                               jj,
                               order_count_2,
                               i_e,
                               q_all,
                               t,
                               start_2,
                               end_2,
                               res_2,
                               q_1,
                               call_count_2);
        }
      }
    }

    return total_2;
  } else {
    // std::cout <<"b" << std::endl;
    if (q_add == order_count_2) {
      V_ID *res_t_2 = res_2_r[j];
      all_exist = true;
      call_count_2++;
      for (auto &t_2 : i_e) {
        ui q_3 = t_2.second;
        if (visited[res_t_2[q_3]]) {
          all_exist = false;
          break;
        }
      }
      total_2 += all_exist;
    } else {
      // bool all_exist = false;
      // std::cout <<"kkk" << std::endl;
      find_children_range(
          children_offset_2, candidates_l_2, order_count_2, order_s, start_2[t], end_2[t], j);
      if (i_e.size() > 0) {
        for (int64_t jj = start_2[t]; jj < end_2[t]; jj++) {
          V_ID *res_t_2 = res_2_r[jj];
          call_count_2++;
          all_exist = true;
          for (auto &t_2 : i_e) {
            ui q_3 = t_2.second;
            if (visited[res_t_2[q_3]]) {
              all_exist = false;
              break;
            }
          }
          total_2 += all_exist;
        }
      } else {
        call_count_2 += 1;
        total_2 = end_2[t] - start_2[t];
      }
    }
    return total_2;
  }
}

void reverse_cuts(__attribute__((unused)) ui **can_1,
                  __attribute__((unused)) int64_t **parent_1,
                  __attribute__((unused)) int64_t **children_1,
                  __attribute__((unused)) int64_t *candidates_l,
                  __attribute__((unused)) V_ID &ord_count,
                  __attribute__((unused)) ui **can_1_n,
                  __attribute__((unused)) int64_t **parent_1_n,
                  __attribute__((unused)) int64_t *candidates_l_n,
                  __attribute__((unused)) bool **valid) {
  __attribute__((unused)) int64_t p, pp;
  int64_t j, j_old, k, l;

  l = candidates_l[ord_count - 1];

  for (int64_t d = 0; d < l; d++) {
    can_1_n[ord_count - 1][d] = can_1[ord_count - 1][d];
  }

  candidates_l[ord_count - 1] = l;

  std::cout << "Finish Reverse" << std::endl;
  for (ui i = ord_count - 1; i > 0; i--) {
    std::cout << i << std::endl;
    l = candidates_l[i];
    p = parent_1[i][0];
    for (int64_t q = 0; q <= p; q++) {
      children_1[i - 1][q] = 0;
    }
    k = p + 1;
    j_old = 0;
    // std::cout <<"l->" << l << std::endl;
    for (j = 1; j < l; j++) {
      if (parent_1[i][j] != p) {
        j_old = j;
        p = parent_1[i][j];
        for (int64_t q = k; q <= p; q++) {
          children_1[i - 1][q] = j_old;
        }
        k = p + 1;
      }
    }
    p = parent_1[i][l - 1];
    // k = p + 1;
    for (int64_t q = k; q <= candidates_l[i - 1] + 1; q++) {
      children_1[i - 1][q] = l;
    }
  }
}

int64_t find_key(__attribute__((unused)) ui **can_1,
                 int64_t **parent_1,
                 V_ID ord_1,
                 V_ID &ord_count_1,
                 int64_t p) {
  for (ui i = ord_count_1 - 1; i > ord_1; i--) {
    p = parent_1[i][p];
  }
  return p;
}

void find_res(ui **can_1, int64_t **parent_1, V_ID &ord_count_1, int64_t p, V_ID *res) {
  for (ui i = ord_count_1 - 1; i > 0; i--) {
    res[i] = can_1[i][p];
    p = parent_1[i][p];
  }
  res[0] = can_1[0][p];
}

V_ID &find_value(ui **can_1, int64_t **parent_1, V_ID &ord_1, V_ID &ord_count_1, int64_t p) {
  for (ui i = ord_count_1 - 1; i > ord_1; i--) {
    p = parent_1[i][p];
  }
  return can_1[ord_1][p];
}

bool check_morphism(ui **can_1,
                    ui **can_2,
                    int64_t **parent_1,
                    int64_t **parent_2,
                    V_ID &ord_1,
                    V_ID &ord_2,
                    V_ID &ord_count_1,
                    V_ID &ord_count_2,
                    int64_t p,
                    int64_t q) {
  if (find_value(can_1, parent_1, ord_1, ord_count_1, p) ==
      find_value(can_2, parent_2, ord_2, ord_count_2, q)) {
    return true;
  }
  return false;
}

void enumeration_bfs2(bool *visited,
                      int64_t &res_all_1,
                      ui **candidates,
                      ui *candidates_count,
                      V_ID *order,
                      TreeNode *tree,
                      V_ID *res,
                      ui current_order,
                      ui &query_count,
                      Graph *query_graph,
                      Graph *data_graph,
                      std::vector<ui> &order_index,
                      V_ID *connection,
                      V_ID *offset,
                      ui **candidates_2,
                      int64_t **parent_offset,
                      int64_t *candidates_l,
                      V_ID *visit_local_2,
                      ui &c_length,
                      bool *morphism) {
  // Current Order = current_order
  // Get the query node of current order
  V_ID &u = order[current_order];
  // ui p_node = tree[u].parent;
  if (current_order == 1) {
    candidates_l[0] = static_cast<int64_t>(candidates_count[order[0]]);
    for (int64_t i = 0; i < candidates_l[0]; i++) {
      candidates_2[0][i] = candidates[order[0]][i];
    }
  }
  ui p_order = 0;
  int64_t total = 0;

  for (ui ii = 0; ii < candidates_count[u]; ii++) {
    visit_local_2[candidates[u][ii]] = 0;
  }

  if (true) {
    int64_t c_l = candidates_l[current_order - 1];
    ui i, j;
    ui *&candidate_3 = candidates_2[current_order];
    int64_t *&parent_3 = parent_offset[current_order];
    L_ID l = query_graph->getVertexLabel(u);
    V_ID d = offset[current_order] - offset[current_order - 1];

    ui candidates_tmp_count = candidates_count[u];
    ui *candidates_tmp = new ui[1000000];

    if (current_order == query_count - 1) {
      if (morphism[current_order] == true) {
        for (int64_t k = 0; k < c_l; k++) {
          visit_pre(
              visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k, res);
          // for (i = 0; i < candidates_count[u]; i++) {
          //     visit_local_2[candidates[u][i]] = 0;
          // }

          candidates_tmp_count = 0;
          for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
            std::unordered_map<L_ID, std::vector<ui>> *v_nl =
                data_graph->getVertexNL(res[connection[i]]);
            std::vector<V_ID> &tmp = (*v_nl)[l];
            for (j = 0; j < tmp.size(); j++) {
              visit_local_2[tmp[j]] += 1;
              if (visit_local_2[tmp[j]] == 1) {
                candidates_tmp[candidates_tmp_count++] = tmp[j];
              }
            }
          }
          for (j = 0; j < candidates_tmp_count; j++) {
            res_all_1 += 1;
            if (visit_local_2[candidates_tmp[j]] == d && visited[candidates_tmp[j]] == false) {
              candidate_3[total] = candidates_tmp[j];
              parent_3[total] = k;
              total += 1;
            }
          }

          for (i = 0; i < candidates_tmp_count; i++) {
            visit_local_2[candidates_tmp[i]] = 0;
          }

          visit_can(visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k);
        }
      } else {
        for (int64_t k = 0; k < c_l; k++) {
          visit_pre_2(
              visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k, res);
          candidates_tmp_count = 0;
          // for (i = 0; i < candidates_count[u]; i++) {
          //     visit_local_2[candidates[u][i]] = 0;
          // }

          for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
            std::unordered_map<L_ID, std::vector<ui>> *v_nl =
                data_graph->getVertexNL(res[connection[i]]);
            std::vector<V_ID> &tmp = (*v_nl)[l];
            for (j = 0; j < tmp.size(); j++) {
              visit_local_2[tmp[j]] += 1;
              if (visit_local_2[tmp[j]] == 1) {
                candidates_tmp[candidates_tmp_count++] = tmp[j];
              }
            }
          }
          for (j = 0; j < candidates_tmp_count; j++) {
            res_all_1 += 1;
            if (visit_local_2[candidates_tmp[j]] == d) {
              candidate_3[total] = candidates_tmp[j];
              parent_3[total] = k;
              total += 1;
            }
          }

          for (i = 0; i < candidates_tmp_count; i++) {
            visit_local_2[candidates_tmp[i]] = 0;
          }
        }
      }
    } else {
      if (morphism[current_order] == true) {
        for (int64_t k = 0; k < c_l; k++) {
          visit_pre(
              visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k, res);

          // candidates_tmp_count = 0;
          for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
            std::unordered_map<L_ID, std::vector<ui>> *v_nl =
                data_graph->getVertexNL(res[connection[i]]);
            std::vector<V_ID> &tmp = (*v_nl)[l];
            for (j = 0; j < tmp.size(); j++) {
              visit_local_2[tmp[j]] += 1;
            }
          }

          for (j = 0; j < candidates_count[u]; j++) {
            res_all_1 += 1;
            if (visit_local_2[candidates[u][j]] == d && visited[candidates[u][j]] == false) {
              candidate_3[total] = candidates[u][j];
              parent_3[total] = k;
              total += 1;
            }
          }

          for (i = 0; i < candidates_count[u]; i++) {
            visit_local_2[candidates[u][i]] = 0;
          }

          visit_can(visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k);
        }
      } else {
        for (int64_t k = 0; k < c_l; k++) {
          visit_pre_2(
              visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k, res);
          // ui t = (current_order-1)*c_length;

          // candidates_tmp_count = 0;

          for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
            std::unordered_map<L_ID, std::vector<ui>> *v_nl =
                data_graph->getVertexNL(res[connection[i]]);
            std::vector<V_ID> &tmp = (*v_nl)[l];
            for (j = 0; j < tmp.size(); j++) {
              visit_local_2[tmp[j]] += 1;
            }
          }

          for (j = 0; j < candidates_count[u]; j++) {
            res_all_1 += 1;
            if (visit_local_2[candidates[u][j]] == d) {
              candidate_3[total] = candidates[u][j];
              parent_3[total] = k;
              total += 1;
            }
          }

          for (i = 0; i < candidates_count[u]; i++) {
            visit_local_2[candidates[u][i]] = 0;
          }
        }
      }
    }
    // std::cout <<"total:" << total << std::endl;
  }
  candidates_l[current_order] = total;
  if (current_order != query_count - 1) {
    enumeration_bfs2(visited,
                     res_all_1,
                     candidates,
                     candidates_count,
                     order,
                     tree,
                     res,
                     current_order + 1,
                     query_count,
                     query_graph,
                     data_graph,
                     order_index,
                     connection,
                     offset,
                     candidates_2,
                     parent_offset,
                     candidates_l,
                     visit_local_2,
                     c_length,
                     morphism);
  }
  std::cout << "End " << total << std::endl;
}

void enumeration_bfs(bool *visited,
                     int64_t &res_all,
                     std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Candidates,
                     V_ID *order,
                     TreeNode *tree,
                     V_ID *res,
                     ui current_order,
                     ui &query_count,
                     Graph *query_graph,
                     Graph *data_graph,
                     std::vector<ui> &order_index,
                     V_ID *connection,
                     V_ID *offset,
                     ui **candidates_2,
                     int64_t **parent_offset,
                     int64_t *candidates_l,
                     V_ID *visit_local_2,
                     ui &c_length,
                     bool *morphism) {
  // Current Order = current_order
  // Get the query node of current order
  V_ID &u = order[current_order];
  V_ID &p_order = order_index[tree[u].parent];
  // Next is to build the current order index.
  int64_t c_l = candidates_l[current_order - 1];
  // std::cout <<"Current #order->" << current_order << "," << c_l << "," << " P_order->" << p_order
  // <<
  //  ", u:" << u << std::endl;
  int64_t total = 0;
  ui i, j;
  ui *&candidate_3 = candidates_2[current_order];
  int64_t *&parent_3 = parent_offset[current_order];

  L_ID l = query_graph->getVertexLabel(u);
  V_ID d = offset[current_order] - offset[current_order - 1];

  if (current_order == query_count - 1) {
    if (morphism[current_order] == true) {
      for (int64_t k = 0; k < c_l; k++) {
        visit_pre(
            visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k, res);
        V_ID &v_f = res[p_order];

        std::vector<V_ID> &local_c = P_Candidates[u][v_f];

        for (i = 0; i < local_c.size(); i++) {
          visit_local_2[local_c[i]] = 0;
        }
        for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
          std::unordered_map<L_ID, std::vector<ui>> *v_nl =
              data_graph->getVertexNL(res[connection[i]]);
          std::vector<V_ID> &tmp = (*v_nl)[l];
          for (j = 0; j < tmp.size(); j++) {
            visit_local_2[tmp[j]] += 1;
          }
        }

        for (j = 0; j < local_c.size(); j++) {
          if (visit_local_2[local_c[j]] == d && visited[local_c[j]] == false) {
            candidate_3[total] = local_c[j];
            parent_3[total] = k;
            total += 1;
          }
        }
        visit_can(visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k);
      }
    } else {
      // std::cout <<"Here" << std::endl;
      for (int64_t k = 0; k < c_l; k++) {
        visit_pre_2(
            visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k, res);
        V_ID &v_f = res[p_order];
        std::vector<V_ID> &local_c = P_Candidates[u][v_f];

        for (i = 0; i < local_c.size(); i++) {
          visit_local_2[local_c[i]] = 0;
        }
        for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
          std::unordered_map<L_ID, std::vector<ui>> *v_nl =
              data_graph->getVertexNL(res[connection[i]]);
          std::vector<V_ID> &tmp = (*v_nl)[l];
          for (j = 0; j < tmp.size(); j++) {
            visit_local_2[tmp[j]] += 1;
          }
        }

        for (j = 0; j < local_c.size(); j++) {
          // std::cout <<"Local->" << visit_local_2[local_c[j]] << ",d->" << d << std::endl;
          if (visit_local_2[local_c[j]] == d) {
            candidate_3[total] = local_c[j];
            parent_3[total] = k;
            total += 1;
          }
        }
      }
    }
  } else {
    if (morphism[current_order] == true) {
      for (int64_t k = 0; k < c_l; k++) {
        visit_pre(
            visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k, res);
        V_ID &v_f = res[p_order];
        std::vector<V_ID> &local_c = P_Candidates[u][v_f];
        // ui t = (current_order-1)*c_length;
        for (i = 0; i < local_c.size(); i++) {
          visit_local_2[local_c[i]] = 0;
        }

        for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
          std::unordered_map<L_ID, std::vector<ui>> *v_nl =
              data_graph->getVertexNL(res[connection[i]]);
          std::vector<V_ID> &tmp = (*v_nl)[l];
          for (j = 0; j < tmp.size(); j++) {
            visit_local_2[tmp[j]] += 1;
          }
        }

        // abc
        for (j = 0; j < local_c.size(); j++) {
          if (visited[local_c[j]] == false && visit_local_2[local_c[j]] == d) {
            candidate_3[total] = local_c[j];
            parent_3[total] = k;
            total += 1;
          }
        }

        visit_can(visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k);
      }
    } else {
      for (int64_t k = 0; k < c_l; k++) {
        visit_pre_2(
            visited, candidates_2, parent_offset, candidates_l, current_order, p_order, k, res);
        V_ID &v_f = res[p_order];
        std::vector<V_ID> &local_c = P_Candidates[u][v_f];

        // ui t = (current_order-1)*c_length;

        for (i = 0; i < local_c.size(); i++) {
          visit_local_2[local_c[i]] = 0;
        }

        for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
          std::unordered_map<L_ID, std::vector<ui>> *v_nl =
              data_graph->getVertexNL(res[connection[i]]);
          std::vector<V_ID> &tmp = (*v_nl)[l];
          for (j = 0; j < tmp.size(); j++) {
            visit_local_2[tmp[j]] += 1;
          }
        }
        for (j = 0; j < local_c.size(); j++) {
          if (visit_local_2[local_c[j]] == d) {
            candidate_3[total] = local_c[j];
            parent_3[total] = k;
            total += 1;
          }
        }
      }
    }
  }

  candidates_l[current_order] = total;
  if (current_order != query_count - 1) {
    enumeration_bfs(visited,
                    res_all,
                    P_Candidates,
                    order,
                    tree,
                    res,
                    current_order + 1,
                    query_count,
                    query_graph,
                    data_graph,
                    order_index,
                    connection,
                    offset,
                    candidates_2,
                    parent_offset,
                    candidates_l,
                    visit_local_2,
                    c_length,
                    morphism);
  }

  // std::cout <<"End " << total << std::endl;
}

int64_t enumeration_ress_bk_2(
    bool *visited,
    int64_t &res_all,
    std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Candidates,
    V_ID *order,
    TreeNode *tree,
    V_ID *res,
    ui current_order,
    ui &query_count,
    Graph *query_graph,
    Graph *data_graph,
    std::vector<ui> &order_index,
    V_ID *connection,
    V_ID *offset,
    V_ID *visit_local,
    V_ID *visit_local_2,
    ui &c_length,
    bool *morphism) {
  V_ID &u = order[current_order];
  V_ID &v_f = res[order_index[tree[u].parent]];
  std::vector<V_ID> &local_c = P_Candidates[u][v_f];
  V_ID d = offset[current_order] - offset[current_order - 1];
  int64_t total = 0;
  ui i, j;

  if (current_order == query_count) {
    for (i = 0; i < local_c.size(); i++) {
      visit_local_2[local_c[i]] = 0;
    }
    L_ID l = query_graph->getVertexLabel(u);
    for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
      std::unordered_map<L_ID, std::vector<ui>> *v_nl = data_graph->getVertexNL(res[connection[i]]);
      std::vector<V_ID> &tmp = (*v_nl)[l];
      for (j = 0; j < tmp.size(); j++) {
        visit_local_2[tmp[j]] += 1;
      }
    }

    if (morphism[current_order] == true) {
      for (j = 0; j < local_c.size(); j++) {
        if (visit_local_2[local_c[j]] == d) {
          total += 1 - visited[local_c[j]];
        }
      }
    } else {
      for (j = 0; j < local_c.size(); j++) {
        if (visit_local_2[local_c[j]] == d) {
          total += 1;
        }
      }
    }
    return total;
  } else {
    ui t = (current_order - 1) * c_length;
    V_ID *visit_local_3 = visit_local + t;
    for (i = 0; i < local_c.size(); i++) {
      visit_local_3[local_c[i]] = 0;
    }
    L_ID l = query_graph->getVertexLabel(u);
    for (i = offset[current_order - 1]; i < offset[current_order]; i++) {
      std::unordered_map<L_ID, std::vector<ui>> *v_nl = data_graph->getVertexNL(res[connection[i]]);
      std::vector<V_ID> &tmp = (*v_nl)[l];
      for (j = 0; j < tmp.size(); j++) {
        visit_local_3[tmp[j]] += 1;
      }
    }
    if (morphism[current_order] == true) {
      for (j = 0; j < local_c.size(); j++) {
        if (visited[local_c[j]] == false && visit_local_3[local_c[j]] == d) {
          res[current_order] = local_c[j];
          visited[local_c[j]] = true;
          total += enumeration_ress_bk_2(visited,
                                         res_all,
                                         P_Candidates,
                                         order,
                                         tree,
                                         res,
                                         current_order + 1,
                                         query_count,
                                         query_graph,
                                         data_graph,
                                         order_index,
                                         connection,
                                         offset,
                                         visit_local,
                                         visit_local_2,
                                         c_length,
                                         morphism);
          visited[local_c[j]] = false;
        }
      }
    } else {
      for (j = 0; j < local_c.size(); j++) {
        if (visit_local_3[local_c[j]] == d) {
          res[current_order] = local_c[j];
          total += enumeration_ress_bk_2(visited,
                                         res_all,
                                         P_Candidates,
                                         order,
                                         tree,
                                         res,
                                         current_order + 1,
                                         query_count,
                                         query_graph,
                                         data_graph,
                                         order_index,
                                         connection,
                                         offset,
                                         visit_local,
                                         visit_local_2,
                                         c_length,
                                         morphism);
        }
      }
    }
    return total;
  }
}

int64_t enumeration_res(int &res_all,
                        std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Candidates,
                        V_ID v_f,
                        V_ID *&order,
                        TreeNode *&tree,
                        V_ID *res,
                        ui current_order,
                        int64_t total,
                        int size_all,
                        int query_count,
                        Graph *&query_graph,
                        Graph *&data_graph,
                        std::vector<ui> &order_index) {
  V_ID u = order[current_order];
  // std::cout <<"####In enumeration function" << std::endl;
  bool over_all = true;
  int64_t res_p = 0;
  v_f = res[total * size_all + order_index[tree[u].parent]];
  int p = P_Candidates[u][v_f].size();
  // std::cout <<"p:->" << p << ", u:"<< u << ", order:"<< current_order << ",v_f" << v_f <<  ",
  // Order
  //  index of parent" << order_index[tree[u].parent] << ", tree.parent->" << tree[u].parent <<
  //  std::endl;

  if (static_cast<int>(current_order) == query_count - 1) {
    // std::cout <<"** in if" << std::endl;
    for (int j = 0; j < p; j++) {
      over_all = true;
      ui nbrs_count_1;
      V_ID *nbrs_1 = query_graph->getVertexNeighbors(order[current_order], nbrs_count_1);
      for (ui k = 0; k < current_order; k++) {
        if (res[total * size_all + k] == P_Candidates[u][v_f][j]) {
          over_all = false;
          break;
        }
      }
      res_all += 1;

      if (over_all == true) {
        ui data_nbrs_count_1;
        V_ID *data_nbrs_1 =
            data_graph->getVertexNeighbors(P_Candidates[u][v_f][j], data_nbrs_count_1);
        for (ui k = 0; k < nbrs_count_1; k++) {
          if (order_index[nbrs_1[k]] < current_order &&
              (nbrs_1[k] == order[0] || order_index[nbrs_1[k]] != 0)) {
            over_all = false;
            V_ID tmp = res[total * size_all + order_index[nbrs_1[k]]];
            for (int64_t t = 0; t < data_nbrs_count_1; t++) {
              if (data_nbrs_1[t] == tmp) {
                over_all = true;
                // std::cout <<"Last True here:::" << std::endl;
                //  break;
              }
            }
            if (over_all == false) {
              break;
            }
          }
        }
      }

      if (over_all == true) {
        // std::cout <<"Res_p:" << res_p << "," << P_Candidates[u][v_f][j] << std::endl;
        res[total * size_all + (res_p)*size_all + current_order] = P_Candidates[u][v_f][j];
        for (ui t = 0; t < current_order; t++) {
          res[total * size_all + res_p * size_all + t] = res[total * size_all + t];
        }
        res_p += 1;
      }
    }

    return res_p;
  } else {
    int64_t total_2 = 0;
    for (uint64_t j = 0; j < P_Candidates[u][v_f].size(); j++) {
      over_all = true;
      int64_t single_res = 0;
      // std::cout <<std::endl <<"#### Current vertex->" << P_Candidates[u][v_f][j] << std::endl;
      ui nbrs_count_1;
      V_ID *nbrs_1 = query_graph->getVertexNeighbors(order[current_order], nbrs_count_1);
      // std::cout <<"Query count:" << nbrs_count_1 << std::endl;
      for (ui k = 0; k < current_order; k++) {
        if (res[total * size_all + k] == P_Candidates[u][v_f][j]) {
          over_all = false;
          // std::cout <<"current_node" << P_Candidates[u][v_f][j] << ", expected node:" <<
          //  res[total*size_all + k] << ", order ->" << k << std::endl;std::cout <<"<<---->> Erase
          //  <<---->>"
          //  << std::endl;
          break;
        }
      }
      if (over_all == true) {
        ui data_nbrs_count_1;
        V_ID *data_nbrs_1 =
            data_graph->getVertexNeighbors(P_Candidates[u][v_f][j], data_nbrs_count_1);
        for (ui k = 0; k < nbrs_count_1; k++) {
          if (order_index[nbrs_1[k]] < current_order &&
              (nbrs_1[k] == order[0] || order_index[nbrs_1[k]] != 0)) {
            over_all = false;
            V_ID tmp_v = res[total * size_all + order_index[nbrs_1[k]]];

            // std::cout <<"NB ID:" << nbrs_1[k] << ",Data Count:" << data_nbrs_count_1 << ", V: "
            // <<
            //  tmp_v << "," << nbrs_1[k] << "," << order_index[nbrs_1[k]] << std::endl;
            for (int64_t t = 0; t < data_nbrs_count_1; t++) {
              if (data_nbrs_1[t] == tmp_v) {
                over_all = true;
                // std::cout <<"True here" << std::endl;
                //  break;
              }
            }

            if (over_all == false) {
              break;
            }
          }
        }
      }
      res_all += 1;
      if (over_all == true) {
        // std::cout <<std::endl <<"#### Current vertex->" << P_Candidates[u][v_f][j] << ",
        // &&&current
        //  order***: " << current_order << std::endl;std::cout <<"Over ALL Game Over" << std::endl;
        //  res_p += 1;
        res[total * size_all + total_2 * size_all + current_order] = P_Candidates[u][v_f][j];
        for (ui t = 0; t < current_order; t++) {
          res[total * size_all + total_2 * size_all + t] = res[total * size_all + t];
        }
        // std::cout <<")))In function --->" << ", Order and p->vertex->" << P_Candidates[u][v_f][j]
        // <<
        //  endl;
        single_res = enumeration_res(res_all,
                                     P_Candidates,
                                     P_Candidates[u][v_f][j],
                                     order,
                                     tree,
                                     res,
                                     current_order + 1,
                                     total + total_2,
                                     size_all,
                                     query_count,
                                     query_graph,
                                     data_graph,
                                     order_index);
        // std::cout <<"--->Finish single_res" << single_res << std::endl;
        // std::cout <<"next Single res:" << single_res << std::endl;
        //   This is the one
        for (int64_t k = 0; k < single_res; k++) {
          // std::cout <<"all" << std::endl;
          res[total * size_all + total_2 * size_all + k * size_all + current_order] =
              P_Candidates[u][v_f][j];
          //      break;
          // std::cout <<"Current order and data vertex: " << current_order << ", " <<
          // P_Candidates[u][v_f][j] << std::endl;
        }

        total_2 += single_res;
      }
    }
    // std::cout <<"Total_2->" << total_2 << std::endl;
    return total_2;
  }
}

// Assume we have all the information in the graph.
void local_optimization(
    std::unordered_map<V_ID, std::vector<V_ID>> &res,
    ui query_count,
    __attribute__((unused)) std::vector<ui> order_index,
    __attribute__((unused))
    std::unordered_map<V_ID, std::vector<std::pair<V_ID, V_ID>>> &intersetion,
    ui *&order,
    __attribute__((unused)) TreeNode *&tree,
    __attribute__((unused)) std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Candidates,
    __attribute__((unused))
    std::unordered_map<V_ID, std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>>
        data_provenance,
    __attribute__((unused)) Graph *data_graph,
    Graph *query_graph) {
  for (ui i = 0; i < query_count; i++) {
    ui u = order[i];
    std::vector<ui> local_res;
    ui label = query_graph->getVertexLabel(u);
    local_res.push_back(label);
    for (ui j = i + 1; j < query_count; j++) {
      ui tmp = order[j];
      if (res.find(label) == res.end() && query_graph->getVertexLabel(tmp) == label) {
        local_res.push_back(tmp);
      }
    }
    if (local_res.size() > 1) {
      auto tmp = res.emplace(u, std::vector<ui>());
      tmp.first->second = local_res;
    }
  }
}

void Refinement(
    std::unordered_map<V_ID, std::vector<std::pair<V_ID, V_ID>>> &intersetion,
    ui *&order,
    TreeNode *&tree,
    ui u,
    std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Candidates,
    std::unordered_map<V_ID, std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>>
        data_provenance,
    Graph *data_graph) {
  // TreeNode& u_node = tree[u];
  ui u_f = INVALID_VERTEX_ID;
  std::unordered_map<V_ID, std::vector<V_ID>> frontiers_2;
  if (u != order[0]) {
    u_f = tree[u].parent;
    frontiers_2 = P_Candidates[u_f];
  }
  std::unordered_map<V_ID, std::vector<V_ID>> frontiers_1 = P_Candidates[u];
  __attribute__((unused)) bool do_interset;
  do_interset = false;
  bool update_index = false;
  if (u != order[0] && intersetion.find(u) != intersetion.end()) {
    do_interset = true;
    for (ui j = 0; j < tree[u].bn_count; j++) {
      update_index = false;

      if (tree[u].bn[j] != tree[u].parent) {
        V_ID u_nb = tree[u].bn[j];
        std::unordered_map<V_ID, std::vector<V_ID>> frontiers_3 = P_Candidates[u_nb];
        V_ID u_nb_f = tree[u_nb].parent;
        V_ID pro_node = ParentNode(u, u_nb, tree);

        for (const auto &it_3 : frontiers_3) {
          ui v_3_f = it_3.first;
          bool find_n = false;
          std::vector<V_ID> c_2 = it_3.second;
          std::vector<bool> find_intersect;
          for (uint64_t l_2 = 0; l_2 < c_2.size(); l_2++) {
            find_intersect.push_back(false);
          }

          for (const auto &it_1 : frontiers_1) {
            ui v_1_f = it_1.first;
            bool share_pro = false;

            for (ui k_1 = 0; k_1 < data_provenance[u_f][v_1_f][pro_node].size(); k_1++) {
              for (ui k_2 = 0; k_2 < data_provenance[u_nb_f][v_3_f][pro_node].size(); k_2++) {
                if (data_provenance[u_f][v_1_f][pro_node][k_1] ==
                    data_provenance[u_nb_f][v_3_f][pro_node][k_2]) {
                  share_pro = true;
                  break;
                }
              }
            }

            if (share_pro == true) {
              // int count = 0;
              std::vector<V_ID> c_1 = it_1.second;
              for (uint64_t l_2 = 0; l_2 < c_2.size(); l_2++) {
                ui v_2 = c_2[l_2];
                if (find_intersect[l_2] == true) continue;
                for (unsigned int v_1 : c_1) {
                  ui data_nbrs_count_1;
                  V_ID *data_nbrs_1 = data_graph->getVertexNeighbors(v_1, data_nbrs_count_1);
                  if (DEBUG) {
                    // std::cout <<"Neighbors are: " ;
                  }
                  for (ui p = 0; p < data_nbrs_count_1; p++) {
                    if (data_nbrs_1[p] == v_2) {
                      find_intersect[l_2] = true;
                      find_n = true;
                      break;
                    }
                  }
                  if (find_intersect[l_2] == true) {
                    break;
                  }
                }
              }
            }
          }
          for (uint64_t l_2 = 0; l_2 < c_2.size(); l_2++) {
            ui v_2 = c_2[l_2];
            if (find_intersect[l_2] == false) {
              for (uint64_t ll = 0; ll < P_Candidates[u_nb][v_3_f].size(); ll++) {
                if (P_Candidates[u_nb][v_3_f][ll] == v_2) {
                  if (DEBUG) {
                    std::cout << "Erase intersect, v_3_f: " << v_3_f << std::endl;
                  }
                  P_Candidates[u_nb][v_3_f].erase(P_Candidates[u_nb][v_3_f].begin() + ll);
                  update_index = true;
                  break;
                }
              }
            }
          }
          if (DEBUG) {
            std::cout << "Judge" << std::endl;
          }
          if (find_n == false) {
            if (DEBUG) {
              std::cout << "Erase Candidates" << std::endl;
            }
            P_Candidates[u_nb].erase(v_3_f);
            update_index = true;
            if (DEBUG) {
              std::cout << "Erase C-Done";
            }
          }
        }
        if (update_index == true) {
          Refinement(intersetion, order, tree, u_nb, P_Candidates, data_provenance, data_graph);
        }
      }
    }
  }
  if (DEBUG) {
    std::cout << "Refine the parents" << std::endl;
  }
  // BFS reduce it's parent:

  update_index = false;
  if (order[0] != u) {
    frontiers_2 = P_Candidates[u_f];
    for (const auto &it : frontiers_2) {
      V_ID v_f_f = it.first;
      int count = 0;
      if (P_Candidates[u_f].find(v_f_f) != P_Candidates[u_f].end()) {
        for (uint64_t j = 0; j < it.second.size(); j++) {
          V_ID v_f = it.second[j];
          if (frontiers_1.find(v_f) == frontiers_1.end()) {
            P_Candidates[u_f][v_f_f].erase(P_Candidates[u_f][v_f_f].begin() + j - count);
            count += 1;
          }
          // This is all
        }
        if (P_Candidates[u_f][v_f_f].size() == 0) {
          P_Candidates[u_f].erase(v_f_f);
          update_index = true;
        }
      }
    }
  }

  if (DEBUG) {
    std::cout << "Refine the parents End" << std::endl;
  }
  if (update_index == true) {
    Refinement(intersetion, order, tree, u_f, P_Candidates, data_provenance, data_graph);
  }

  // std::cout <<"3" << std::endl;

  update_index = false;
  frontiers_1 = P_Candidates[u];
  do_interset = false;
  // Intersection erase their frontier neighbourhoods:
  if (true) {
    if (DEBUG) {
      std::cout << " In the key of Intersetion." << std::endl;
    }
    do_interset = true;
    if (u == order[0]) {
      // return;
      u_f = u;
    }
    for (ui j = 0; j < tree[u].fn_count; j++) {
      update_index = false;
      V_ID u_nb = tree[u].fn[j];

      if (u_nb == u_f ||
          std::find(tree[u].children, tree[u].children + tree[u].children_count, u_nb) !=
              tree[u].children + tree[u].children_count)
        continue;
      if (DEBUG) {
        std::cout << "Nbors N:" << u_nb << std::endl;
      }
      std::unordered_map<V_ID, std::vector<V_ID>> frontiers_3 = P_Candidates[u_nb];
      V_ID u_nb_f = tree[u_nb].parent;
      V_ID pro_node = ParentNode(u, u_nb, tree);

      for (const auto &it_3 : frontiers_3) {
        ui v_3_f = it_3.first;
        bool find_n = false;
        std::vector<V_ID> c_2 = it_3.second;
        std::vector<bool> find_intersect;
        // Temp array and vec.
        // Initialize the find intersect bool list.

        for (uint64_t l_2 = 0; l_2 < c_2.size(); l_2++) {
          find_intersect.push_back(false);
        }

        for (const auto &it_1 : frontiers_1) {
          ui v_1_f = it_1.first;
          bool share_pro = false;
          for (ui k_1 = 0; k_1 < data_provenance[u_f][v_1_f][pro_node].size(); k_1++) {
            for (ui k_2 = 0; k_2 < data_provenance[u_nb_f][v_3_f][pro_node].size(); k_2++) {
              if (data_provenance[u_f][v_1_f][pro_node][k_1] ==
                  data_provenance[u_nb_f][v_3_f][pro_node][k_2]) {
                share_pro = true;
                break;
              }
            }
          }

          if (share_pro == true) {
            // int count = 0;
            std::vector<V_ID> c_1 = it_1.second;
            for (uint64_t l_2 = 0; l_2 < c_2.size(); l_2++) {
              ui v_2 = c_2[l_2];
              if (find_intersect[l_2] == true) continue;
              for (unsigned int v_1 : c_1) {
                ui data_nbrs_count_1;
                V_ID *data_nbrs_1 = data_graph->getVertexNeighbors(v_1, data_nbrs_count_1);

                for (ui p = 0; p < data_nbrs_count_1; p++) {
                  if (data_nbrs_1[p] == v_2) {
                    if (DEBUG) {
                      std::cout << "True###" << std::endl;
                    }
                    find_intersect[l_2] = true;
                    find_n = true;
                    break;
                  }
                }
                if (find_intersect[l_2] == true) {
                  break;
                }
              }
            }
          }
        }

        for (uint64_t l_2 = 0; l_2 < c_2.size(); l_2++) {
          ui v_2 = c_2[l_2];
          if (find_intersect[l_2] == false) {
            for (uint64_t ll = 0; ll < P_Candidates[u_nb][v_3_f].size(); ll++) {
              if (P_Candidates[u_nb][v_3_f][ll] == v_2) {
                if (DEBUG) {
                  std::cout << "Erase intersect, v_3_f: " << v_3_f << std::endl;
                }
                P_Candidates[u_nb][v_3_f].erase(P_Candidates[u_nb][v_3_f].begin() + ll);
                update_index = true;
                if (DEBUG) {
                  std::cout << "Erase inter-Done" << std::endl;
                }
                break;
              }
            }
            if (DEBUG) {
              std::cout << "Current Node: " << 2 << " ; ";
              for (const auto &iter : P_Candidates[2]) {
                std::cout << "Vector Size: " << iter.second.size() << " ";
                std::cout << "Parent node: " << iter.first << " [";
                for (unsigned int d : iter.second) {
                  std::cout << d << ",";
                }
                std::cout << " ] " << std::endl;
              }
            }
          }
        }

        if (find_n == false) {
          P_Candidates[u_nb].erase(v_3_f);
          if (DEBUG) {
            std::cout << "Erase C-Done";
          }
        }
      }
      if (update_index == true) {
        Refinement(intersetion, order, tree, u_nb, P_Candidates, data_provenance, data_graph);
      }
    }
  }
  if (DEBUG) {
    std::cout << "Refine the children::" << std::endl;
  }

  // BFS reduce it's children:

  for (V_ID k = 0; k < tree[u].children_count; k++) {
    update_index = false;
    V_ID u_c = tree[u].children[k];
    frontiers_2 = P_Candidates[u_c];
    std::vector<bool> child;
    int count = 0;
    for (const auto &it : frontiers_2) {
      V_ID v_f = it.first;
      bool find_children = false;
      for (const auto &it_1 : frontiers_1) {
        if (std::find(it_1.second.begin(), it_1.second.end(), v_f) != it_1.second.end()) {
          find_children = true;
          break;
        }
        // This is all
      }
      if (find_children == false && P_Candidates[u_c].find(v_f) != P_Candidates[u_c].end()) {
        P_Candidates[u_c].erase(v_f);
        update_index = true;
      }
      count += 1;
    }

    if (update_index == true) {
      Refinement(intersetion, order, tree, u_c, P_Candidates, data_provenance, data_graph);
    }
  }
  if (DEBUG) {
    std::cout << std::endl << "end--> )))" << u << std::endl << std::endl;
  }
}

int local_cartesian(std::vector<ui> &order_index,
                    TreeNode *&tree,
                    std::vector<int> &candidates_count,
                    ui u,
                    std::vector<V_ID> &node_tmp) {
  int64_t total = 1;
  int num_group = tree[u].children_count;

  if (num_group == 0) {
    return candidates_count[order_index[u]];
  }

  for (int i = 0; i < num_group; i++) {
    ui u_2 = tree[u].children[i];
    node_tmp.push_back(u_2);
    total *= local_cartesian(order_index, tree, candidates_count, u_2, node_tmp);
    // std::cout <<"Test->" << total << std::endl;
  }

  return total;
}

void cartesian_s(int *cartesian,
                 V_ID *&order,
                 std::vector<ui> order_index,
                 TreeNode *&tree,
                 std::vector<int> &candidates_count,
                 ui root,
                 std::vector<std::vector<V_ID>> &pattern_e,
                 ui &final_group,
                 V_ID *&order_1,
                 V_ID *&order_2,
                 ui &order_count_1,
                 ui &order_count_2,
                 ui &query_count) {
  int num_group = tree[root].children_count;
  std::cout << "potential partition num:" << num_group << std::endl;

  auto e_g = new int[100][100];

  std::vector<std::vector<V_ID>> node_l;

  int64_t total_cartesian = 1;
  for (int i = 0; i < num_group; i++) {
    std::vector<V_ID> node_tmp;
    node_tmp.push_back(root);
    cartesian[i] = 0;
    ui u = tree[root].children[i];
    node_tmp.push_back(u);
    cartesian[i] = local_cartesian(order_index, tree, candidates_count, u, node_tmp);
    std::cout << " # cartesian result->" << u << "->" << cartesian[i] << std::endl;
    node_l.push_back(node_tmp);
    total_cartesian *= cartesian[i];
  }

  std::cout << "Finished cartesian" << std::endl;
  // (1) Edge information
  for (int i = 0; i < num_group; i++) {
    for (int j = 0; j < num_group; j++) {
      e_g[i][j] = 999;
      if (i != j) {
        e_g[i][j] = 0;
        for (uint64_t p = 1; p < node_l[i].size(); p++) {
          for (uint64_t q = 1; q < node_l[j].size(); q++) {
            // std::cout <<node_l[i][p] << "," << node_l[j][q] << ", group(i,j)->"<< i << ","<< j <<
            //  endl;
            if (order_index[node_l[i][p]] < order_index[node_l[j][q]]) {
              for (uint64_t t = 0; t < pattern_e[order_index[node_l[i][p]]].size(); t++) {
                if (node_l[j][q] == pattern_e[order_index[node_l[i][p]]][t]) {
                  e_g[i][j] += 1;
                  break;
                }
              }
            } else if (order_index[node_l[i][p]] > order_index[node_l[j][q]]) {
              for (uint64_t t = 0; t < pattern_e[order_index[node_l[j][q]]].size(); t++) {
                if (node_l[i][p] == pattern_e[order_index[node_l[j][q]]][t]) {
                  e_g[i][j] += 1;
                  break;
                }
              }
            }
          }
        }
      }
      // std::cout <<"group->" << i << ", and group->" << j << ", edge->" << e_g[i][j] << std::endl;
    }
  }
  // (2) Devide the edge.
  // Partition 1 edge.
  int64_t min_e = 0;
  for (int j = 1; j < num_group; j++) {
    min_e += e_g[0][j];
  }
  ui order_g = 0;
  for (int i = 1; i < num_group; i++) {
    int local_e = 0;
    for (int j = 0; j < num_group; j++) {
      if (i != j) {
        local_e += e_g[i][j];
      }
    }
    int64_t tmp_t = cartesian[i] * num_group - total_cartesian;
    // std::cout <<"Tmp:" << tmp << std::endl;
    if (local_e < min_e && tmp_t >= 0) {
      order_g = i;
      min_e = local_e;
    }
  }
  // std::cout <<"order_g->" << order_g << ", min_e:" << min_e << std::endl;
  final_group = order_g;

  std::cout << "Finished prepare" << std::endl;
  // (3) partition with all the cases.

  int cases = num_group / 2;
  std::vector<std::set<ui>> all_balance;
  std::vector<int64_t> p_cartesian;
  std::vector<int64_t> p_edges;
  std::vector<std::tuple<int64_t, int64_t, int64_t>> l_c_all;

  // There are i kinds of the combination C(1,n) ... C(i,n), C(|num_group|/2,n).
  // int tmp[30];
  // for (int i = 0 ; i < 30; i ++) {
  //    tmp[i] = 0;
  // }
  for (int i = 1; i <= cases; i++) {
    std::vector<std::set<ui>> cases_balance;
    for (int k = 0; k < num_group; k++) {
      std::set<ui> s1;
      s1.insert(k);
      cases_balance.push_back(s1);
    }
    for (int j = 1; j < i; j++) {
      std::vector<std::set<ui>> cases_balance_new;
      for (int k = 0; k < num_group; k++) {
        for (auto &t : cases_balance) {
          if (t.find(static_cast<unsigned int>(k)) == t.end()) {
            std::set<ui> local_case;
            local_case = t;
            local_case.insert(k);
            if (std::find(cases_balance_new.begin(), cases_balance_new.end(), local_case) ==
                cases_balance_new.end()) {
              cases_balance_new.push_back(local_case);
            }
          }
        }
      }
      cases_balance = cases_balance_new;
    }
    for (auto &j : cases_balance) {
      all_balance.push_back(j);
    }
  }

  std::cout << "Before do" << std::endl;
  if (all_balance.size() == 0) {
    order_count_1 = query_count;
    order_count_2 = 0;
    for (ui j = 0; j < query_count; j++) {
      order_1[j] = order[j];
    }
    return;
  }

  for (uint64_t j = 0; j < all_balance.size(); j++) {
    // long  double value = 0;
    int64_t l_c = 0;
    int64_t l_cc = 0;
    int l_e = 0;
    std::cout << "Group->";
    std::queue<ui> q3;
    for (auto itr = all_balance[j].begin(); itr != all_balance[j].end(); itr++) {
      q3.push(tree[root].children[*itr]);
      l_cc *= cartesian[*itr];
      // std::cout <<"," << *itr;
      for (int i = 0; i < num_group; i++) {
        if (std::find(all_balance[j].begin(), all_balance[j].end(), i) == all_balance[j].end()) {
          l_e += e_g[*itr][i];
        }
      }
    }

    while (!q3.empty()) {
      // dequeue front node and print it
      ui v = q3.front();
      q3.pop();
      l_c += 1;
      // order_1[order_count_1++] = v;
      if (tree[v].children_count > 2) {
        for (ui m = 0; m < tree[v].children_count; m++) {
          q3.push(tree[v].children[m]);
        }
      }
    }

    std::cout << "total_cartesian" << total_cartesian << std::endl;

    l_c = static_cast<int64_t>((query_count) * (query_count) / 4) - (query_count - l_c) * (l_c);
    std::tuple<int64_t, int64_t, int64_t> tmp;
    std::get<0>(tmp) = l_c;
    std::get<1>(tmp) = l_e;
    std::get<2>(tmp) = j;
    l_c_all.push_back(tmp);

    // std::cout <<",l_c, l_e:" << l_c << "," << l_e << std::endl;

    // value = 0;
  }

  sort(l_c_all.begin(), l_c_all.end());

  // std::cout <<"l_c->" <<std::get<0>(l_c_all[0]) << std::endl;
  // std::cout <<"Check the first one,l_c:" <<std::get<0>(l_c_all[0]) << ",l_e:"
  // <<std::get<1>(l_c_all[0])
  // <<
  //  endl;

  std::cout << "End all prosibility:" << all_balance.size() << std::endl;

  // Group 1
  std::vector<ui> node;
  std::queue<ui> q1, q2;

  for (int i = 0; i < num_group; i++) {
    // node.push_back(*all_balance[i].begin());

    int t = std::get<2>(l_c_all[0]);
    if (std::find(all_balance[t].begin(), all_balance[t].end(), i) != all_balance[t].end()) {
      q1.push(tree[root].children[i]);
    } else {
      q2.push(tree[root].children[i]);
    }
  }

  order_count_1 = 0;
  order_count_2 = 0;
  // if()
  order_1[order_count_1++] = root;
  order_2[order_count_2++] = root;

  ui v;
  while (!q1.empty()) {
    // dequeue front node and print it
    v = q1.front();
    q1.pop();
    order_1[order_count_1++] = v;
    for (ui j = 0; j < tree[v].children_count; j++) {
      q1.push(tree[v].children[j]);
    }
  }
  // Group 2

  while (!q2.empty()) {
    // dequeue front node and print it
    v = q2.front();
    q2.pop();
    order_2[order_count_2++] = v;
    for (ui j = 0; j < tree[v].children_count; j++) {
      q2.push(tree[v].children[j]);
    }
  }

  if (order_count_1 > order_count_2) {
    for (ui k = 0; k < order_count_2; k++) {
      ui tmp = order_1[k];
      order_1[k] = order_2[k];
      order_2[k] = tmp;
    }
    for (ui k = order_count_2; k < order_count_1; k++) {
      order_2[k] = order_1[k];
    }
    ui tmp = order_count_1;
    order_count_1 = order_count_2;
    order_count_2 = tmp;
  }
}

void pruneCandidates(Graph *data_graph,
                     Graph *query_graph,
                     V_ID query_vertex,
                     V_ID *pivot_vertices,
                     ui pivot_vertices_count,
                     V_ID **candidates,
                     ui *candidates_count,
                     ui *flag,
                     ui *updated_flag) {
  L_ID query_vertex_label = query_graph->getVertexLabel(query_vertex);
  ui query_vertex_degree = query_graph->getVertexDegree(query_vertex);

  ui count = 0;
  ui updated_flag_count = 0;

  for (ui i = 0; i < pivot_vertices_count; ++i) {
    V_ID pivot_vertex = pivot_vertices[i];

    for (ui j = 0; j < candidates_count[pivot_vertex]; ++j) {
      V_ID v = candidates[pivot_vertex][j];

      if (v == INVALID_VERTEX_ID) continue;
      ui v_nbrs_count;
      V_ID *v_nbrs = data_graph->getVertexNeighbors(v, v_nbrs_count);

      for (ui k = 0; k < v_nbrs_count; ++k) {
        V_ID v_nbr = v_nbrs[k];
        L_ID v_nbr_label = data_graph->getVertexLabel(v_nbr);
        ui v_nbr_degree = data_graph->getVertexDegree(v_nbr);

        if (flag[v_nbr] == count && v_nbr_label == query_vertex_label &&
            v_nbr_degree >= query_vertex_degree) {
          flag[v_nbr] += 1;

          if (count == 0) {
            updated_flag[updated_flag_count++] = v_nbr;
          }
        }
      }
    }
    count += 1;
  }

  for (ui i = 0; i < candidates_count[query_vertex]; ++i) {
    ui v = candidates[query_vertex][i];
    if (v == INVALID_VERTEX_ID) continue;

    if (flag[v] != count) {
      candidates[query_vertex][i] = INVALID_VERTEX_ID;
    }
  }

  for (ui i = 0; i < updated_flag_count; ++i) {
    ui v = updated_flag[i];
    flag[v] = 0;
  }
}

void Index_To_Candidate(ui **&candidates,
                        ui *&candidates_count,
                        std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Candidates,
                        ui query_count,
                        ui data_count,
                        ui *order,
                        bool *visited) {
  for (ui i = 0; i < query_count; i++) {
    ui u = order[i];
    candidates_count[u] = 0;
    std::unordered_map<V_ID, std::vector<V_ID>> frontiers = P_Candidates[u];

    for (const auto &frontier : frontiers) {
      for (unsigned int j : frontier.second) {
        if (visited[j] == false) {
          visited[j] = true;
          candidates[u][candidates_count[u]++] = j;
        }
      }
    }
    std::fill(visited, visited + data_count, false);
  }
}

void compactCandidates(ui **&candidates, ui *&candidates_count, ui query_count) {
  for (ui i = 0; i < query_count; ++i) {
    V_ID query_vertex = i;
    ui next_position = 0;
    for (ui j = 0; j < candidates_count[query_vertex]; ++j) {
      V_ID data_vertex = candidates[query_vertex][j];

      if (data_vertex != INVALID_VERTEX_ID) {
        candidates[query_vertex][next_position++] = data_vertex;
      }
    }

    candidates_count[query_vertex] = next_position;
  }
}

void intersection(
    std::unordered_map<V_ID, std::vector<V_ID>> &query_provenance,
    std::unordered_map<V_ID, std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>>
        &data_provenance,
    V_ID &u,
    std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>> &node_provenance,
    V_ID &u_p,
    std::unordered_map<V_ID, std::vector<V_ID>> &frontiers_1) {
  for (const auto &it_1 : frontiers_1) {
    V_ID v_f = it_1.first;
    for (unsigned int v : it_1.second) {
      std::unordered_map<V_ID, std::vector<V_ID>> pro;
      bool v_existed = false;
      if (node_provenance.find(v) != node_provenance.end()) {
        v_existed = true;
      }
      if (v_existed == false) {
        for (uint64_t g = 0; g < query_provenance[u].size(); g++) {
          if (u == query_provenance[u][g]) {
            // add current node and v to the provenance information if this node is one of the
            // required provenance ID.
            std::vector<V_ID> pro_v;
            pro_v.push_back(v);
            pro.insert(std::pair<V_ID, std::vector<V_ID>>(query_provenance[u][g], pro_v));
            // std::cout <<"  !!!!!!!!!!!++++intersect  " << pro[query_provenance[u][g]][0] <<
            //  endl;
          } else if (data_provenance[u_p][v_f][query_provenance[u][g]].size() != 0) {
            pro.insert(std::pair<V_ID, std::vector<V_ID>>(
                query_provenance[u][g], data_provenance[u_p][v_f][query_provenance[u][g]]));
          }
        }
        if (pro.size() != 0) {
          node_provenance.insert(
              std::pair<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>(v, pro));
        }
      } else {
        for (uint64_t g = 0; g < query_provenance[u].size(); g++) {
          if (u != query_provenance[u][g] &&
              data_provenance[u_p][v_f][query_provenance[u][g]].size() != 0) {
            // add current node and v to the provenance information if this node is one of the
            // required provenance ID.
            for (uint64_t ll = 0; ll < data_provenance[u_p][v_f][query_provenance[u][g]].size();
                 ll++) {
              if (std::find(node_provenance[v][query_provenance[u][g]].begin(),
                            node_provenance[v][query_provenance[u][g]].end(),
                            data_provenance[u_p][v_f][query_provenance[u][g]][ll]) ==
                  node_provenance[v][query_provenance[u][g]].end()) {
                node_provenance[v][query_provenance[u][g]].push_back(
                    data_provenance[u_p][v_f][query_provenance[u][g]][ll]);
              }
            }
          }
        }
      }
    }
  }
}
void CheckRepartiton1(Graph *&query_graph,
                      ui *&order_1,
                      ui *&order_2,
                      ui &order_count_1,
                      ui &order_count_2,
                      bool &inside,
                      bool &outside,
                      ui &counnt) {
  while (inside == false && outside == false && counnt < 10) {
    outside = true;
    counnt++;
    for (ui i = 1; i < order_count_1; i++) {
      inside = false;
      ui nb_count;
      ui *nb = query_graph->getVertexNeighbors(order_1[i], nb_count);
      for (ui j = i + 1; j < order_count_1; j++) {
        if (std::find(nb, nb + nb_count, order_1[j]) != nb + nb_count) {
          inside = true;
          break;
        }
      }

      if (inside == false) {
        outside = false;
        for (ui j = 1; j < order_count_2; j++) {
          nb = query_graph->getVertexNeighbors(order_2[j], nb_count);
          if (std::find(nb, nb + nb_count, order_1[i]) != nb + nb_count) {
            inside = false;
            outside = true;
            order_2[order_count_2++] = order_1[i];
            for (ui k = i + 1; k < order_count_1; k++) {
              order_1[k - 1] = order_1[k];
            }
            order_count_1--;
            break;
          }
        }
      }

      if (outside == true && inside == false) {
        outside = false;
        break;
      }
    }
  }
}
void CheckRepartiton2(Graph *&query_graph,
                      ui *&order_1,
                      ui *&order_2,
                      ui &order_count_1,
                      ui &order_count_2,
                      bool &inside,
                      bool &outside,
                      ui &counnt) {
  while (inside == false && outside == false && counnt < 10) {
    outside = true;
    counnt++;
    for (ui i = 1; i < order_count_2; i++) {
      inside = false;
      ui nb_count;
      ui *nb = query_graph->getVertexNeighbors(order_2[i], nb_count);
      for (ui j = i + 1; j < order_count_1; j++) {
        if (std::find(nb, nb + nb_count, order_2[j]) != nb + nb_count) {
          inside = true;
          break;
        }
      }
      if (inside == false) {
        outside = false;
        for (ui j = 1; j < order_count_1; j++) {
          nb = query_graph->getVertexNeighbors(order_1[j], nb_count);
          if (std::find(nb, nb + nb_count, order_2[i]) != nb + nb_count) {
            inside = false;
            outside = true;
            order_1[order_count_1++] = order_2[i];
            for (ui k = i + 1; k < order_count_2; k++) {
              order_2[k - 1] = order_2[k];
            }
            order_count_2--;
            break;
          }
        }
      }
      if (outside == true && inside == false) {
        outside = false;
        break;
      }
    }
  }
}
bool CECIFunction(Graph *data_graph,
                  Graph *query_graph,
                  ui **&candidates,
                  ui *&candidates_count,
                  ui *&order,
                  ui *&provenance,
                  TreeNode *&tree,
                  std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Candidates,
                  std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> &P_Provenance) {
  // std::cout <<"Initialize function: " << std::endl;
  //   Initial the CECI Index.
  //   In this case,

  double timer_all_s = omp_get_wtime();

  // std::cout <<"a" << std::endl;
  V_ID start_vertex = InitialStartVertex(data_graph, query_graph);

  // start_vertex = 6;
  // std::cout <<"Start Vertex: " << start_vertex << std::endl;

  bfs(query_graph,
      start_vertex,
      tree,
      order,
      provenance);  // Build the tree structure and order from query graph

  // query_count is the number of the vertexs in the query graph.
  ui query_count = query_graph->getVerticesCount();
  std::vector<ui> order_index(query_count);

  // Build vertex to the order Reverse index;
  for (ui i = 0; i < query_count; ++i) {
    V_ID query_vertex = order[i];
    order_index[query_vertex] = i;
  }

  // In the query graph
  // every query tree, build their neighbour and parents information.
  // std::cout <<"Begin Building Neighbourhood Informations" << std::endl;

  for (ui i = 0; i < query_count; ++i) {
    V_ID u = order[i];
    // tree[u].under_level_count = 0;
    tree[u].bn_count = 0;
    tree[u].fn_count = 0;
    ui u_nbrs_count;

    V_ID *u_nbrs = query_graph->getVertexNeighbors(u, u_nbrs_count);
    for (ui j = 0; j < u_nbrs_count; ++j) {
      V_ID u_nbr = u_nbrs[j];
      if (order_index[u_nbr] < order_index[u]) {
        tree[u].bn[tree[u].bn_count++] = u_nbr;
      } else {
        tree[u].fn[tree[u].fn_count++] = u_nbr;
      }
    }
  }

  // Initialization
  ui candidates_max_count = data_graph->getGraphMaxLabelFrequency();

  ui data_count = data_graph->getVerticesCount();
  // ui query_count = query_graph->getVerticesCount();
  ui *updated_flag = new ui[data_graph->getVerticesCount()];
  ui *flag = new ui[data_graph->getVerticesCount()];
  std::fill(flag, flag + data_graph->getVerticesCount(), 0);

  candidates_count = new ui[query_count];
  memset(candidates_count, 0, sizeof(ui) * query_count);

  candidates = new ui *[query_count];

  ui **candidates_inter = new ui *[query_count];
  ui **candidates_index = new ui *[query_count];

  // ui* candidates_count_inter = new ui[query_count];
  ui *candidates_count_index = new ui[query_count];

  for (ui i = 0; i < query_count; ++i) {
    candidates[i] = new ui[candidates_max_count];
    candidates_inter[i] = new ui[candidates_max_count];
    candidates_index[i] = new ui[candidates_max_count];
  }

  for (ui i = 0; i < query_count; i++) {
    ZFComputeNLF(
        data_graph, query_graph, order[i], candidates_count[order[i]], candidates[order[i]]);
  }
  std::cout << "begin" << std::endl;

  // The number of refinement is k. According to the original paper, we set k as 3.
  for (ui k = 0; k < 3; ++k) {
    if (k % 2 == 0) {
      for (ui i = 1; i < query_count; ++i) {
        V_ID u = order[i];
        TreeNode &node = tree[u];
        pruneCandidates(data_graph,
                        query_graph,
                        u,
                        node.bn,
                        node.bn_count,
                        candidates,
                        candidates_count,
                        flag,
                        updated_flag);
      }
    } else {
      for (int i = query_count - 2; i >= 0; --i) {
        V_ID u = order[i];
        TreeNode &node = tree[u];
        pruneCandidates(data_graph,
                        query_graph,
                        u,
                        node.fn,
                        node.fn_count,
                        candidates,
                        candidates_count,
                        flag,
                        updated_flag);
      }
    }
  }

  compactCandidates(candidates, candidates_count, query_count);

  for (ui j = 0; j < query_count; j++) {
    std::cout << "u->" << order[j] << "Candidates_count->" << candidates_count[order[j]]
              << std::endl;
  }
  std::cout << "Done with Filter" << std::endl;

  candidates_count_index[order[0]] = candidates_count[order[0]];

  for (ui k = 0; k < candidates_count[order[0]]; k++) {
    candidates_index[order[0]][k] = candidates[order[0]][k];
  }

  // std::cout <<"print neighbourhood function" << std::endl;
  //  which pair of intersection we need
  std::unordered_map<V_ID, std::vector<std::pair<V_ID, V_ID>>> intersetion;
  // Size is the length of the query graph .
  std::unordered_map<V_ID, std::vector<V_ID>> query_provenance;
  // Size is .
  // std::vector<std::unordered_map<V_ID, V_ID>> data_provenance;
  // <U_ID, <V_ID, <U_ID, std::vector of <V_ID>>>> //
  // The last item in the data_provenance is corresponding to the different candidates with the same
  // provenance key in the query graph.
  //

  std::unordered_map<V_ID, std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>>
      data_provenance;
  // This is all
  // // 11
  //
  for (ui i = 0; i < query_count; ++i) {
    V_ID u = order[i];
    V_ID tmp_first = INVALID_VERTEX_ID;
    V_ID tmp_second = INVALID_VERTEX_ID;
    if (tree[u].bn_count > 1) {
      // std::cout <<"Node: (" << u << ")-> ";
      tmp_first = tree[u].parent;
      for (ui j = 0; j < tree[u].bn_count; j++) {
        // std::cout <<tree[u].bn[j] << ", ";
        tmp_second = tree[u].bn[j];
        if (tree[u].parent != tmp_second) {
          Insertion(u, tmp_first, tmp_second, intersetion);
        }
      }
    }
  }

  // int length = intersetion.size();

  std::unordered_map<V_ID, std::vector<std::pair<V_ID, V_ID>>>::iterator its = intersetion.begin();

  std::unordered_map<V_ID, std::vector<V_ID>> branching;

  // int i = 0;

  // all
  // Check the parent node for each intersection's pair.

  while (its != intersetion.end()) {
    std::vector<V_ID> single_branch;
    std::vector<std::pair<V_ID, V_ID>> inter = its->second;

    for (auto &k : inter) {
      V_ID r = ParentNode(k.first, k.second, tree);
      // All.
      add_provenance(query_provenance, k.first, r);
      add_provenance(query_provenance, k.second, r);
      single_branch.push_back(r);
    }

    if (single_branch.size() > 0) {
      branching.insert(std::pair<V_ID, std::vector<V_ID>>(its->first, single_branch));
    }
    its++;
  }

  // Reverse BFS order to record what the provenance their parents need.
  // bool need_add = query_count;
  for (ui i = query_count - 1; i > 0; --i) {
    V_ID u = order[i];
    V_ID u_p = tree[u].parent;

    if (query_provenance.find(u) != query_provenance.end()) {
      if (query_provenance.find(u_p) != query_provenance.end()) {
        for (uint64_t j = 0; j < query_provenance[u].size(); j++) {
          if (std::find(query_provenance[u_p].begin(),
                        query_provenance[u_p].end(),
                        query_provenance[u][j]) == query_provenance[u_p].end()) {
            query_provenance[u_p].push_back(query_provenance[u][j]);
          }
        }

      } else {  // If the map is empty with index of u_p.
        std::unordered_map<V_ID, std::vector<V_ID>>::iterator iter;
        iter = query_provenance.begin();
        query_provenance.insert(iter, std::pair<V_ID, std::vector<V_ID>>(u_p, query_provenance[u]));
      }
    }
  }

  // Print out the tree structure;

  // data_provenance;

  // Start doing for the data graph.
  //
  // Not a good one;

  // TE_Candidates construction and filtering.

  V_ID root = order[0];

  // std::cout <<"Root is:" << root << std::endl;

  // We use the ID 0 as their candidates.

  // Initialize the Buffer for the Candidate and Candidate Count:

  // std::cout <<"Initialize Max Count: " << candidates_max_count << std::endl;

  // ZFComputeNLF(data_graph, query_graph, root, candidates_count[root], candidates[root]);

  // std::cout <<"Number of Candidate: " << candidates_count[root] << std::endl;

  // No Candidates with the root
  if (candidates_count[root] == 0) {
    std::cout << "Build Candidate Fail" << std::endl;
    return false;
  }

  // Building the P_Candidates.
  std::vector<ui> data_visited(data_count);

  std::fill(data_visited.begin(), data_visited.end(), 0);

  std::vector<ui> data_visited_2(data_count);

  std::fill(data_visited_2.begin(), data_visited_2.end(), 0);

  std::vector<bool> visited_query(query_count);
  std::fill(visited_query.begin(), visited_query.end(), false);

  visited_query[root] = true;

  // Candidates and Provenances
  P_Candidates.resize(query_count);
  P_Provenance.resize(query_count);

  // Initialization of the root's Candidates
  //

  std::unordered_map<V_ID, std::vector<V_ID>> root_candidate;
  for (ui i = 0; i < candidates_count[root]; i++) {
    std::vector<V_ID> tmp;
    tmp.push_back(candidates[root][i]);
    root_candidate.insert(std::pair<V_ID, std::vector<V_ID>>(candidates[root][i], tmp));
  }

  P_Candidates[root] = root_candidate;

  // std::cout <<"Begin BFS and intersetion Function: " << std::endl;

  // Record the start provenance in the data graph.
  std::unordered_map<V_ID, V_ID> Provenance;
  std::unordered_map<V_ID, std::unordered_map<V_ID, V_ID>>
      Close_provenance;  // <first_node, <second_node, Provenance>>.

  // record the index and provenance for the root.
  //

  // This records according to order index.
  //

  if (query_provenance.find(root) != query_provenance.end()) {
    std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>> node_provenance;
    for (ui i = 0; i < candidates_count[root]; i++) {
      std::vector<V_ID> pro_v;
      pro_v.push_back(candidates[root][i]);
      std::unordered_map<V_ID, std::vector<V_ID>> pro;
      std::unordered_map<V_ID, std::vector<V_ID>>::iterator iter = pro.begin();
      pro.insert(iter, std::pair<V_ID, std::vector<V_ID>>(root, pro_v));

      node_provenance.insert(
          std::pair<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>(candidates[root][i], pro));
    }

    data_provenance.insert(
        std::pair<V_ID, std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>>(
            root, node_provenance));
  }

  // return 0;

  // This one
  //

  for (ui i = 1; i < query_count; ++i) {
    // std::cout <<"####check candidate 4->0 is->" << candidates[4][0] << std::endl;
    V_ID u = order[i];
    // TreeNode& u_node = tree[u];
    bool do_interset = false;
    if (intersetion.find(u) != intersetion.end()) {
      // std::cout <<"In the key of Intersetion." << std::endl;
      do_interset = true;
    }
    // sets of frontiers and count for parent node.

    ui u_l = query_graph->getVertexLabel(u);
    ui u_d = query_graph->getVertexDegree(u);
    ui u_f = tree[u].parent;

    // get NLF of current node.
    // std::unordered_map<L_ID, ui>* u_nlf = query_graph->getVertexNLF(u);
    candidates_count_index[u] = 0;
    // initial each candidate_count of each node to be 0.
    //    V_ID* frontiers = candidates[u];
    std::unordered_map<V_ID, std::vector<V_ID>> nb_interset;

    for (ui j = 0; j < candidates_count[u]; ++j) {
      V_ID v = candidates[u][j];
      data_visited_2[v] = 1;
    }

    if (do_interset && 1 == 2) {
      int length_inter = intersetion[u].size();
      for (int d = 0; d < length_inter; d++) {
        std::vector<V_ID> tt;
        if (visited_query[u] == true) {
          // ui u_1 = intersetion[u][d].first;
          ui u_2 = intersetion[u][d].second;
          ui u_2_f = tree[u_2].parent;

          // v_1 and u_1 will be the parent nodes.
          std::unordered_map<V_ID, std::vector<V_ID>> frontiers_2 = P_Candidates[u_2];
          ui frontiers_count_2 = frontiers_2.size();
          for (auto it = nb_interset.cbegin(); it != nb_interset.cend();) {
            ui v_1 = it->first;
            bool find_v_1 = false;
            std::unordered_map<V_ID, std::vector<V_ID>>::iterator item = frontiers_2.begin();
            std::vector<V_ID> tmp_inter;
            for (ui k = 0; k < frontiers_count_2; ++k) {
              ui v_2_f = item->first;  // get the parent ID fron the Candidate index.
              ui pro_node = branching[u][d];
              bool if_pro = false;
              for (ui k_1 = 0; k_1 < data_provenance[u_f][v_1][pro_node].size(); k_1++) {
                for (ui k_2 = 0; k_2 < data_provenance[u_2_f][v_2_f][pro_node].size(); k_2++) {
                  if (data_provenance[u_f][v_1][pro_node][k_1] ==
                      data_provenance[u_2_f][v_2_f][pro_node][k_2]) {
                    if_pro = true;
                  }
                }
              }
              if (if_pro == true) {
                for (unsigned int v_2 : item->second) {
                  ui data_nbrs_count_2;
                  V_ID *data_nbrs_2 = data_graph->getVertexNeighbors(v_2, data_nbrs_count_2);
                  for (unsigned int p : it->second) {
                    for (ui q = 0; q < data_nbrs_count_2; q++) {
                      if (p == data_nbrs_2[q] &&
                          find(tmp_inter.begin(), tmp_inter.end(), p) == tmp_inter.end()) {
                        tmp_inter.push_back(data_nbrs_2[q]);
                        find_v_1 = true;
                      }
                    }
                  }
                }
              }
              item++;
            }
            if (tmp_inter.size() > 0) {
              find_v_1 = true;
              nb_interset[v_1] = tmp_inter;
            }
            if (find_v_1 == false) {
              nb_interset.erase(it++);
            } else {
              ++it;
            }
          }
          // Else
        } else {
          visited_query[u] = true;
          ui u_1 = intersetion[u][d].first;
          ui u_2 = intersetion[u][d].second;
          ui u_1_f = u_1;
          ui pro_node = branching[u][d];
          ui u_2_f = tree[u_2].parent;
          if (u_1 != root) {
            u_1_f = tree[u_1].parent;
          }
          if (DEBUG == 1) {
            std::cout << u_1 << " " << u_2 << "Unvisited, Pro-->" << pro_node << std::endl;
          }
          std::unordered_map<V_ID, std::vector<V_ID>> frontiers_1 = P_Candidates[u_1];
          std::unordered_map<V_ID, std::vector<V_ID>> frontiers_2 = P_Candidates[u_2];
          // ui frontiers_count_1 = frontiers_1.size();
          // ui frontiers_count_2 = frontiers_2.size();
          if (u_1 == root || u_1 == pro_node) {
            u_1_f = u_1;
            for (const auto &it_1 : frontiers_1) {
              std::vector<V_ID> c_1 = it_1.second;
              for (unsigned int v_1 : c_1) {
                // std::vector<V_ID> tmp_inter;
                for (const auto &it_2 : frontiers_2) {
                  ui v_2_f = it_2.first;
                  bool if_pro = false;
                  for (unsigned int k_2 : data_provenance[u_2_f][v_2_f][pro_node]) {
                    if (v_1 == k_2) {
                      if_pro = true;
                    }
                  }
                  std::vector<V_ID> c_2 = it_2.second;
                  std::vector<V_ID> tmp_inter;
                  if (if_pro == true) {
                    ui data_nbrs_count_1;
                    V_ID *data_nbrs_1 = data_graph->getVertexNeighbors(v_1, data_nbrs_count_1);

                    for (unsigned int v_2 : c_2) {
                      ui data_nbrs_count_2;
                      V_ID *data_nbrs_2 = data_graph->getVertexNeighbors(v_2, data_nbrs_count_2);
                      // label filter and degree filter should be added here
                      for (ui p = 0; p < data_nbrs_count_1; p++) {
                        for (ui q = 0; q < data_nbrs_count_2; q++) {
                          ui t = data_nbrs_1[p];
                          // std::cout <<"Nbrs_1->" << t << "Nbrs_2->" << data_nbrs_2[q] <<
                          // std::endl;
                          if (t == data_nbrs_2[q] && data_graph->getVertexLabel(t) == u_l &&
                              data_graph->getVertexDegree(t) >= u_d) {
                            if (std::find(tmp_inter.begin(), tmp_inter.end(), t) ==
                                tmp_inter.end()) {
                              tmp_inter.push_back(t);
                            }
                          }
                        }
                      }
                    }
                  }

                  if (tmp_inter.size() > 0) {
                    nb_interset.insert(std::pair<V_ID, std::vector<V_ID>>(v_1, tmp_inter));
                  }
                }
              }
            }
          } else {
            // u_1 is neither root nor parent.
            for (const auto &it_1 : frontiers_1) {
              ui v_1_f = it_1.first;
              std::vector<V_ID> c_1 = it_1.second;
              for (unsigned int v_1 : c_1) {
                std::vector<V_ID> tmp_inter;
                for (const auto &it_2 : frontiers_2) {
                  ui v_2_f = it_2.first;
                  bool if_pro = false;
                  for (ui k_1 = 0; k_1 < data_provenance[u_1_f][v_1_f][pro_node].size(); k_1++) {
                    for (ui k_2 = 0; k_2 < data_provenance[u_2_f][v_2_f][pro_node].size(); k_2++) {
                      if (data_provenance[u_1_f][v_1_f][pro_node][k_1] ==
                          data_provenance[u_2_f][v_2_f][pro_node][k_2]) {
                        if_pro = true;
                        break;
                      }
                    }
                    if (if_pro) {
                      break;
                    }
                  }
                  if (if_pro) {
                    std::vector<V_ID> c_2 = it_2.second;
                    ui data_nbrs_count_1;
                    V_ID *data_nbrs_1 = data_graph->getVertexNeighbors(v_1, data_nbrs_count_1);
                    for (unsigned int v_2 : c_2) {
                      ui data_nbrs_count_2;
                      V_ID *data_nbrs_2 = data_graph->getVertexNeighbors(v_2, data_nbrs_count_2);
                      for (ui p = 0; p < data_nbrs_count_1; p++) {
                        for (ui q = 0; q < data_nbrs_count_2; q++) {
                          ui t = data_nbrs_1[p];
                          if (t == data_nbrs_2[q] && data_graph->getVertexLabel(t) == u_l &&
                              data_graph->getVertexDegree(t) >= u_d) {
                            if (std::find(tmp_inter.begin(), tmp_inter.end(), t) ==
                                tmp_inter.end()) {
                              tmp_inter.push_back(t);
                            }
                          }
                        }
                      }
                    }
                  }
                }
                if (tmp_inter.size() > 0) {
                  nb_interset.insert(std::pair<V_ID, std::vector<V_ID>>(v_1, tmp_inter));
                }
              }
            }
          }
        }
        // V_ID u_p = tree[u].parent;
      }

      bool find_u = false;

      std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>> node_provenance;

      if (query_provenance.find(u) != query_provenance.end()) {
        // All.
        find_u = true;
      }
      V_ID u_p = tree[u].parent;

      for (auto &itt : nb_interset) {
        V_ID v_f = itt.first;
        auto tmp = P_Candidates[u].emplace(v_f, std::vector<V_ID>());
        for (unsigned int v : itt.second) {
          if (data_visited_2[v] == 1) {
            tmp.first->second.push_back(v);
            if (data_visited[v] == 0) {
              data_visited[v] = 1;
              candidates_index[u][candidates_count_index[u]++] = v;
            }
          }
        }
        if (tmp.first->second.size() == 0) {
          P_Candidates[u].erase(v_f);

          // frontiers[j] = INVALID_VERTEX_ID;
          for (ui k = 0; k < tree[u_p].children_count; ++k) {
            V_ID u_c = tree[u_p].children[k];
            if (visited_query[u_c]) {
              P_Candidates[u_c].erase(v_f);
            }
          }
        }
      }
      if (find_u) {
        std::unordered_map<V_ID, std::vector<V_ID>> frontiers_1 = P_Candidates[u];

        intersection(query_provenance, data_provenance, u, node_provenance, u_p, frontiers_1);
      }
      if (node_provenance.size() != 0) {
        data_provenance.insert(
            std::pair<V_ID, std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>>(
                u, node_provenance));
      }
      // std::cout <<"tmp print it all EndEnd !!!" << std::endl;
      for (ui j = 0; j < candidates_count_index[u]; ++j) {
        V_ID v = candidates_index[u][j];
        data_visited[v] = 0;  // reset the data_visited for the data graph.
      }
    }
    if (visited_query[u] == false) {
      // BFS function
      // std::cout <<"" << std::endl;
      // std::cout <<"BFS node: { " << u << " }-> ";
      visited_query[u] = true;
      // All
      V_ID u_p = tree[u].parent;

      // Records u's label and degree.
      // std::cout <<"parent is->" << u_p << std::endl;
      V_ID *frontiers = candidates_index[u_p];
      ui frontiers_count = candidates_count_index[u_p];
      // std::cout <<"Frontier Candidate: ";
      if (DEBUG) {
        std::cout << u << " is BFS and parent node is-->" << u_p << std::endl;
        std::cout << "Frontier count is --> " << frontiers_count << std::endl;
      }

      // std::cout <<"parent is->" << u_p << ", frontier_count->" << frontiers_count << std::endl;
      bool find_u = false;
      if (query_provenance.find(u) != query_provenance.end()) {
        // All.
        find_u = true;
      }
      std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>> node_provenance;

      for (ui j = 0; j < frontiers_count; ++j) {
        V_ID v_f = frontiers[j];  // for all its frontiers nodes
        // std::cout <<"," << v_f ;
        //   If it's not a valid one for building the CECI idnex, skip
        if (v_f == INVALID_VERTEX_ID) {
          std::cout << " " << v_f << "is a invalid id";
          continue;
        }
        ui data_nbrs_count;
        V_ID *data_nbrs = data_graph->getVertexNeighbors(v_f, data_nbrs_count);
        // std::cout <<"V_f is->" << v_f << std::endl;
        //  This is for the vector of candidates.
        auto tmp = P_Candidates[u].emplace(v_f, std::vector<V_ID>());
        // std::cout <<"VF is ->" << v_f << " ";
        //  P_Candidates
        //  bool is_provenance = false;
        // std::cout <<"Neighbourhood is: ";
        for (ui k = 0; k < data_nbrs_count; ++k) {
          V_ID v = data_nbrs[k];
          // std::cout <<v << ", ";
          if (data_visited_2[v] == 1) {
            bool is_valid = true;
            // std::cout <<"###################check 1" << std::endl;
            if (is_valid) {
              // std::cout <<"->is valid" << std::endl;
              tmp.first->second.push_back(v);
              if (data_visited[v] == 0) {
                data_visited[v] = 1;
                candidates_index[u][candidates_count_index[u]++] = v;
              }

              if (find_u) {
                if (node_provenance.find(v) == node_provenance.end()) {
                  // All.
                  std::unordered_map<V_ID, std::vector<V_ID>> pro;
                  for (uint64_t g = 0; g < query_provenance[u].size(); g++) {
                    if (u == query_provenance[u][g]) {
                      // add current node and v to the provenance information if this node is one of
                      // the required provenance ID.
                      std::vector<V_ID> pro_v;
                      pro_v.push_back(v);
                      pro.insert(std::pair<V_ID, std::vector<V_ID>>(query_provenance[u][g], pro_v));
                      // std::cout <<"  !!!!!!!!!!!  " << pro[query_provenance[u][g]][0] <<
                      // std::endl;
                    } else if (data_provenance[u_p][v_f][query_provenance[u][g]].size() != 0) {
                      // pass the provenance information from the parent node to its child node.
                      //
                      //
                      if (pro.find(query_provenance[u][g]) == pro.end()) {
                        pro.insert(std::pair<V_ID, std::vector<V_ID>>(
                            query_provenance[u][g],
                            data_provenance[u_p][v_f][query_provenance[u][g]]));
                      } else {
                        std::vector<V_ID> tmp_v = data_provenance[u_p][v_f][query_provenance[u][g]];
                        for (unsigned int &m : tmp_v) {
                          std::vector<V_ID>::iterator q_t =
                              find(node_provenance[v][query_provenance[u][g]].begin(),
                                   node_provenance[v][query_provenance[u][g]].end(),
                                   m);
                          if (q_t == node_provenance[v][query_provenance[u][g]].end()) {
                            node_provenance[v][query_provenance[u][g]].push_back(m);
                          }
                        }
                      }
                    }
                  }
                  if (pro.size() != 0) {
                    node_provenance.insert(
                        std::pair<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>(v, pro));
                  }
                } else {
                  for (uint64_t g = 0; g < query_provenance[u].size(); g++) {
                    if (data_provenance[u_p][v_f][query_provenance[u][g]].size() != 0) {
                      // pass the provenance information from the parent node to its child node.
                      if (node_provenance[v].find(query_provenance[u][g]) ==
                          node_provenance[v].end()) {
                        node_provenance[v].insert(std::pair<V_ID, std::vector<V_ID>>(
                            query_provenance[u][g],
                            data_provenance[u_p][v_f][query_provenance[u][g]]));
                        // std::cout <<"*********" << pro[query_provenance[u][g]][0] << std::endl;
                      } else {
                        std::vector<V_ID> tmp_v = data_provenance[u_p][v_f][query_provenance[u][g]];
                        for (unsigned int &m : tmp_v) {
                          std::vector<V_ID>::iterator q_t =
                              find(node_provenance[v][query_provenance[u][g]].begin(),
                                   node_provenance[v][query_provenance[u][g]].end(),
                                   m);
                          if (q_t == node_provenance[v][query_provenance[u][g]].end()) {
                            node_provenance[v][query_provenance[u][g]].push_back(m);
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }

        if (tmp.first->second.empty()) {
          // set it as invalid
          // std::cout <<" Erase it. "<< std::endl;
          P_Candidates[u].erase(v_f);

          frontiers[j] = INVALID_VERTEX_ID;
          for (ui k = 0; k < tree[u_p].children_count; ++k) {
            V_ID u_c = tree[u_p].children[k];
            if (visited_query[u_c]) {
              // std::cout <<"u_c is " << u_c <<", v_f is " << v_f;
              P_Candidates[u_c].erase(v_f);
            }
          }
        }
      }

      if (find_u && node_provenance.size() != 0) {
        data_provenance.insert(
            std::pair<V_ID, std::unordered_map<V_ID, std::unordered_map<V_ID, std::vector<V_ID>>>>(
                u, node_provenance));
      }

      if (candidates_count_index[u] == 0) {
        std::cout << "Node: " << u << " Fail" << std::endl;
        // std::cout <<"Pro: " << std::endl;
        return false;
      }
      // flag reset.
      for (ui j = 0; j < candidates_count_index[u]; ++j) {
        V_ID v = candidates_index[u][j];
        data_visited[v] = 0;  // reset the data_visited for the data graph.
      }
    }

    for (ui j = 0; j < candidates_count[u]; ++j) {
      V_ID v = candidates[u][j];
      data_visited_2[v] = 0;
    }
  }
  // Temp print

  if (DEBUG) {
    std::cout << " ### Reverse Refinement" << std::endl;
  }

  std::cout << "Refinement->" << std::endl;

  for (int i = query_count - 1; i >= 0; i--) {
    // std::cout <<"Current u->";
    ui u = order[i];
    // std::cout <<u << std::endl;
    Refinement(intersetion, order, tree, u, P_Candidates, data_provenance, data_graph);
    std::cout << "After Refine ----> ######## P->" << P_Candidates[u].size() << std::endl;
  }

  int all_cc = 0;
  std::unordered_map<V_ID, std::vector<V_ID>> resss;
  local_optimization(resss,
                     query_count,
                     order_index,
                     intersetion,
                     order,
                     tree,
                     P_Candidates,
                     data_provenance,
                     data_graph,
                     query_graph);

  all_cc = resss.size();
  all_cc = all_cc + 1 - 1;
  // int ress = 0;

  // Enumeration
  // cuTS enumeration:
  // (1)

  // std::cout <<"#### all function -> ####" << std::endl << std::endl;
  ui **candidates_2 = new ui *[query_count];
  int64_t **parent_offset = new int64_t *[query_count];
  int64_t *candidates_l = new int64_t[query_count];
  ui *parent_l = new ui[query_count];
  // bool* visited_d = new bool[candidates_max_count];

  for (ui i = 0; i < query_count; ++i) {
    candidates_2[i] = new ui[Q_LIMIT];
    parent_offset[i] = new int64_t[Q_LIMIT];
    parent_l[i] = 0;
    candidates_l[i] = 0;
  }
  // Enumeration:
  //   (1) Process the query graph and find out all the pattern edges information.

  std::vector<std::vector<V_ID>> pattern_e;
  // std::vector<std::vector<std::vector<std::pair<V_ID,V_ID>>>> data_e;
  std::vector<int> pattern_candidates;
  for (ui i = 0; i < query_count - 1; ++i) {
    std::vector<V_ID> tmp_e;
    V_ID u = order[i];
    ui u_nbrs_count;
    int tmp_cc = candidates_count[u];
    /*unordered_map<V_ID, std::vector<V_ID>> frontiers_1 = P_Candidates[u];
std::cout <<i << std::endl;
    for (auto it_1 = frontiers_1.cbegin();  it_1 != frontiers_1.cend(); it_1 ++) {
        tmp_cc += it_1->second.size();
    }*/

    V_ID *u_nbrs = query_graph->getVertexNeighbors(u, u_nbrs_count);
    for (ui j = 0; j < u_nbrs_count; ++j) {
      V_ID u_nbr = u_nbrs[j];
      // If this node is not their parent nodes and has less index(which means haven't visited yet).
      if (order_index[u_nbr] > order_index[u]) {
        // Recordes the pattern edges information.
        tmp_e.push_back(u_nbr);
      }
    }
    pattern_e.push_back(tmp_e);
    pattern_candidates.push_back(tmp_cc);
    // std::cout <<"Node->" << u << ", Candidates:" << tmp_cc << ",order->" << i << std::endl;
  }

  V_ID u_2 = order[query_count - 1];
  // ui u_nbrs_count;
  ui tmp_cc = candidates_count[u_2];

  /*unordered_map<V_ID, std::vector<V_ID>> frontiers_1 = P_Candidates[u_2];
  for (auto it_1 = frontiers_1.cbegin();  it_1 != frontiers_1.cend(); it_1 ++) {
      tmp_cc += it_1->second.size();
  }*/

  pattern_candidates.push_back(tmp_cc);

  //   (2) Process the data graph to find out all the candidates for the data graph.

  // std::cout <<"end 1" << std::endl;

  //   (3)
  //   This is all.   Partition the two subgraph by calculating their cartesian && the candidates
  //   of edges.
  // Partition the query graph into two subgroup

  int cartesian[100];
  ui r = order[0];
  ui final_group;

  //   (4) Enumerate local result

  ui *order_1 = new ui[100];
  ui *order_2 = new ui[100];
  ui order_count_1 = 0;
  ui order_count_2 = 0;

  std::cout << "in" << std::endl;

  bool split = false;

  if (split == false) {
    cartesian_s(cartesian,
                order,
                order_index,
                tree,
                pattern_candidates,
                r,
                pattern_e,
                final_group,
                order_1,
                order_2,
                order_count_1,
                order_count_2,
                query_count);
  } else {
    std::string myText;

    // Read from the text file
    std::ifstream MyReadFile("fiedler");

    // Use a while loop together with the getline() function to read the file line by line
    // int oo = 0;
    getline(MyReadFile, myText);

    // char type;
    V_ID id;
    L_ID class_id;
    while (MyReadFile >> id) {
      MyReadFile >> class_id;

      std::cout << id << "," << class_id << std::endl;
    }

    // Close the file
    MyReadFile.close();
    return false;
    // Read the partition result.
  }

  // Here is the section for the coding.
  //

  bool inside = false;
  bool outside = false;
  ui counnt = 0;
  ui *order_1_d = new ui[query_count];
  ui *order_2_d = new ui[query_count];
  ui order_1_d_count = order_count_1;
  ui order_2_d_count = order_count_2;

  for (ui i = 0; i < order_count_1; i++) {
    order_1_d[i] = order_1[i];
  }

  for (ui i = 0; i < order_count_2; i++) {
    order_2_d[i] = order_2[i];
  }

  CheckRepartiton1(
      query_graph, order_1, order_2, order_count_1, order_count_2, inside, outside, counnt);

  // if (order_count_1 > 1 ) {
  counnt = 0;
  inside = false;
  outside = false;
  CheckRepartiton2(
      query_graph, order_1, order_2, order_count_1, order_count_2, inside, outside, counnt);
  // bool nodo = false;
  if (order_count_1 <= 1 || order_count_2 <= 1) {
    // nodo = true;
  }
  if (order_count_1 <= 2 || order_count_2 <= 2) {
    // nodo = true;
    std::cout << "Should not do it" << std::endl;
    order_count_1 = order_1_d_count;
    order_count_2 = order_2_d_count;
    for (ui i = 0; i < order_count_1; i++) {
      order_1[i] = order_1_d[i];
    }
    for (ui i = 0; i < order_count_2; i++) {
      order_2[i] = order_2_d[i];
    }
  }

  static V_ID *visit_local_2 = new V_ID[data_count];

  static bool *visited = new bool[data_count];
  std::fill(visited, visited + data_count, false);
  Index_To_Candidate(candidates_index,
                     candidates_count_index,
                     P_Candidates,
                     query_count,
                     data_count,
                     order,
                     visited);

  for (ui j = 0; j < query_count; j++) {
    std::cout << "u->" << order[j] << "Candidates_count->" << candidates_count_index[order[j]]
              << std::endl;
  }

  for (ui i = 1; i < order_count_1; i++) {
    for (ui j = i + 1; j < order_count_1; j++) {
      if (candidates_count_index[order_1[i]] > 1.5 * candidates_count_index[order_1[j]]) {
        ui tmp10 = order_1[i];
        order_1[i] = order_1[j];
        order_1[j] = tmp10;
      }
    }
  }

  for (ui i = 1; i < order_count_2; i++) {
    for (ui j = i + 1; j < order_count_2; j++) {
      if (candidates_count_index[order_2[i]] > 1.5 * candidates_count_index[order_2[j]]) {
        ui tmp10 = order_2[i];
        order_2[i] = order_2[j];
        order_2[j] = tmp10;
      }
    }
  }

  std::cout << "out" << std::endl;

  // std::cout <<"Test 2" << std::endl;
  bool *p_3_1 = new bool[query_count];

  bool *p_3_2 = new bool[query_count];

  for (ui i = 0; i < query_count; i++) {
    p_3_1[i] = false;
    p_3_2[i] = false;
  }

  // (5.1) Load edges information,std::cout <<"Load edge->" << std::endl;
  for (ui i = 1; i < order_count_1; i++) {
    for (ui j = 1; j < order_count_2; j++) {
      // std::cout <<"Tmp:" << tmp << std::endl;
      //  if (order_1[i] != order_2[j]) {
      if (order_index[order_2[j]] > order_index[order_1[i]]) {
        ui tmp = order_index[order_1[i]];
        if (std::find(pattern_e[tmp].begin(), pattern_e[tmp].end(), order_2[j]) !=
            pattern_e[tmp].end()) {
          p_3_1[order_1[i]] = true;
          p_3_2[order_2[j]] = true;
        }
      } else if (order_index[order_2[j]] < order_index[order_1[i]]) {
        ui tmp = order_index[order_2[j]];
        if (std::find(pattern_e[tmp].begin(), pattern_e[tmp].end(), order_1[i]) !=
            pattern_e[tmp].end()) {
          p_3_1[order_1[i]] = true;
          p_3_2[order_2[j]] = true;
        }
      }

      // if( order_1[i] != order_2[j] and query_graph->getVertexLabel(order_1[i]) ==
      // query_graph->getVertexLabel(order_2[j])) {
      //      p_3_1[order_1[i]] = true;
      //      p_3_2[order_2[j]] = true;
      // }
      //}
    }
  }

  // if (1 == 2) {
  //   ui move = 0;
  //   for (ui i = 1; i < order_count_1 - move; i++) {
  //     if (p_3_1[order_1[i]] == false) {
  //       bool doit = true;
  //       ui nb_count;
  //       ui *nb = query_graph->getVertexNeighbors(order_1[i], nb_count);

  //       for (ui j = 0; j < nb_count; j++) {
  //         bool inorder = false;
  //         for (ui k = 0; k < order_count_1; k++) {
  //           if (order_1[k] == nb[j]) {
  //             inorder = true;
  //           }
  //         }
  //         if (nodo == false) {
  //           if (candidates_count_index[order_1[i]] < 1.5 * candidates_count_index[nb[j]] &&
  //               inorder) {
  //             doit = false;
  //             p_3_1[order_1[i]] = true;
  //           }
  //         } else {
  //           if (candidates_count_index[order_1[i]] < 1.2 * candidates_count_index[nb[j]] &&
  //               inorder) {
  //             doit = false;
  //             p_3_1[order_1[i]] = true;
  //           }
  //         }
  //       }

  //       if (doit) {
  //         ui tmp2 = order_1[i];
  //         move += 1;
  //         for (ui j = i; j < order_count_1 - 1; j++) {
  //           ui tmp = order_1[j];
  //           order_1[j] = order_1[j + 1];
  //           order_1[j + 1] = tmp;
  //         }
  //         if (order_1[i] != tmp2) {
  //           i -= 1;
  //         }
  //       }
  //     }
  //   }

  //   move = 0;
  //   for (ui i = 1; i < order_count_2 - move; i++) {
  //     if (p_3_2[order_2[i]] == false) {
  //       bool doit = true;

  //       std::cout << std::endl;
  //       ui nb_count;
  //       ui *nb = query_graph->getVertexNeighbors(order_2[i], nb_count);

  //       for (ui j = 0; j < nb_count; j++) {
  //         bool inorder = false;
  //         for (ui k = 0; k < order_count_2; k++) {
  //           if (order_2[k] == nb[j]) {
  //             inorder = true;
  //           }
  //         }
  //         if (nodo == false) {
  //           if (candidates_count_index[order_2[i]] < 1.5 * candidates_count_index[nb[j]] &&
  //               inorder) {
  //             doit = false;
  //             p_3_2[order_2[i]] = true;
  //           }
  //         } else {
  //           if (candidates_count_index[order_2[i]] < 1.2 * candidates_count_index[nb[j]] &&
  //               inorder) {
  //             doit = false;
  //             p_3_2[order_2[i]] = true;
  //           }
  //         }
  //       }

  //       if (doit) {
  //         ui tmp2 = order_2[i];
  //         move += 1;
  //         for (ui j = i; j < order_count_2 - 1; j++) {
  //           ui tmp = order_2[j];
  //           order_2[j] = order_2[j + 1];
  //           order_2[j + 1] = tmp;
  //         }
  //         if (order_2[i] != tmp2) {
  //           i -= 1;
  //         }
  //       }
  //     }
  //   }
  // }

  static V_ID **res_1 = new V_ID *[query_count];
  static V_ID **res_2 = new V_ID *[query_count];
  static V_ID **res_1_n = new V_ID *[query_count];
  static V_ID **res_2_n = new V_ID *[query_count];

  static int64_t **parent_offset_1 = new int64_t *[query_count];
  static int64_t **children_offset_1 = new int64_t *[query_count];
  static int64_t **children_offset_2 = new int64_t *[query_count];
  static int64_t **parent_offset_2 = new int64_t *[query_count];

  static int64_t **parent_offset_1_n = new int64_t *[query_count];
  static int64_t **parent_offset_2_n = new int64_t *[query_count];

  int64_t *candidates_l_1 = new int64_t[query_count];
  int64_t *candidates_l_2 = new int64_t[query_count];

  int64_t *candidates_l_1_n = new int64_t[query_count];
  int64_t *candidates_l_2_n = new int64_t[query_count];

  for (ui i = 0; i < query_count; ++i) {
    res_1[i] = new V_ID[Q_LIMIT];
    res_2[i] = new V_ID[Q_LIMIT];
    res_1_n[i] = new V_ID[Q_LIMIT];
    res_2_n[i] = new V_ID[Q_LIMIT];

    parent_offset_1[i] = new int64_t[Q_LIMIT];
    parent_offset_2[i] = new int64_t[Q_LIMIT];

    parent_offset_1_n[i] = new int64_t[Q_LIMIT];
    parent_offset_2_n[i] = new int64_t[Q_LIMIT];

    children_offset_1[i] = new int64_t[Q_LIMIT];
    children_offset_2[i] = new int64_t[Q_LIMIT];

    candidates_l_1[i] = 0;
    candidates_l_2[i] = 0;

    candidates_l_1_n[i] = 0;
    candidates_l_2_n[i] = 0;
  }

  // int NTHREADS = 1, nthreads;
  int64_t total_result = 0;
  // double sum[NTHREADS];
  double timer_start1 = omp_get_wtime();
  // omp_set_num_threads(NTHREADS);

  std::vector<V_ID> order_index_1(query_count);
  std::vector<V_ID> order_index_2(query_count);

  // std::cout <<"Group original->";
  for (ui i = 0; i < query_count; i++) {
    ui query_vertex = order[i];
    std::cout << query_vertex << ",";
    order_index_1[query_vertex] = 99999;
    order_index_2[query_vertex] = 99999;
  }

  std::cout << "Group 1->" << std::endl;

  for (ui i = 0; i < order_count_1; i++) {
    ui query_vertex = order_1[i];
    std::cout << query_vertex << ",";
    order_index_1[query_vertex] = i;
  }

  std::cout << std::endl;

  std::cout << "Group 2->" << std::endl;
  for (ui i = 0; i < order_count_2; i++) {
    ui query_vertex = order_2[i];
    std::cout << query_vertex << ",";
    order_index_2[query_vertex] = i;
  }

  std::cout << std::endl;

  // static bool *visit_all = new bool[data_count*data_count];
  // std::fill(visit_all, visit_all+data_count*data_count, false);
  /*
     for(ui i=0; i < data_count; i++) {
            ui data_count_1;
            V_ID *data_nb = data_graph->getVertexNeighbors(i, data_count_1);
            for(ui j=0; j < data_count; j++) {
                  visit_all[i*data_count + j] = true;
            }
     }*/

  // Morphism of partition 1.
  static bool morphism_1[32];

  for (ui i = 0; i < order_count_1; i++) {
    morphism_1[i] = false;
    for (ui j = 0; j < order_count_1; j++) {
      if (i != j &&
          query_graph->getVertexLabel(order_1[i]) == query_graph->getVertexLabel(order_1[j])) {
        morphism_1[i] = true;
        break;
      }
    }
  }

  // Morphism of partition 2.
  /*
  static bool morphism_2[32];
  for (ui i = 0; i < order_count_2; i ++) {
      morphism_2[i] = false;
      for (ui j = 0; j < order_count_2; j ++) {
          if (i!=j and query_graph->getVertexLabel(order_2[i]) ==
  query_graph->getVertexLabel(order_2[j])) { morphism_2[i] = true; break;
          }
      }
  }
  */

  std::cout << "Middle->" << std::endl;
  // Connection and offset.
  // static V_ID *visit_local_2 = new V_ID[data_count];

  static V_ID *connection = new V_ID[query_count * query_count];

  static V_ID *offset = new V_ID[query_count];

  for (ui i = 0; i < query_count; ++i) {
    offset[i] = 0;
    if (i != 0) {
      offset[i] = offset[i - 1];
    }
    V_ID u = order[i];
    // V_ID u_p = tree[u].parent;
    ui u_nbrs_count;
    V_ID *u_nbrs = query_graph->getVertexNeighbors(u, u_nbrs_count);
    for (ui j = 0; j < u_nbrs_count; ++j) {
      V_ID u_nbr = u_nbrs[j];
      // If this node is not their parent nodes and has less index(which means haven't visited yet).
      if (order_index[u_nbr] < order_index[u]) {
        connection[offset[i]] = order_index[u_nbr];
        offset[i] += 1;
      }
      /*if (order_index[u_nbr] > order_index[u]) {
          connection[offset[i]] = order_index[u_nbr];
          offset[i] += 1;
      }*/
    }
  }

  static V_ID *connection_1 = new V_ID[query_count * query_count];
  static V_ID *offset_1 = new V_ID[query_count];
  for (ui i = 0; i < order_count_1; ++i) {
    offset_1[i] = 0;
    if (i != 0) {
      offset_1[i] = offset_1[i - 1];
    }

    V_ID u = order_1[i];
    // V_ID u_p = tree[u].parent;
    ui u_nbrs_count;
    V_ID *u_nbrs = query_graph->getVertexNeighbors(u, u_nbrs_count);
    for (ui j = 0; j < u_nbrs_count; ++j) {
      V_ID u_nbr = u_nbrs[j];
      // If this node is not their parent nodes and has less index(which means haven't visited yet).
      if (order_index_1[u_nbr] < order_index_1[u]) {
        connection_1[offset_1[i]] = order_index_1[u_nbr];
        offset_1[i] += 1;
      }
      /*if (order_index[u_nbr] > order_index[u]) {
          connection[offset[i]] = order_index[u_nbr];
          offset[i] += 1;
      }*/
    }
  }

  static V_ID *connection_2 = new V_ID[query_count * query_count];
  static V_ID *offset_2 = new V_ID[query_count];
  for (ui i = 0; i < order_count_2; ++i) {
    offset_2[i] = 0;
    if (i != 0) {
      offset_2[i] = offset_2[i - 1];
    }
    V_ID u = order_2[i];
    // V_ID u_p = tree[u].parent;
    ui u_nbrs_count;
    V_ID *u_nbrs = query_graph->getVertexNeighbors(u, u_nbrs_count);
    for (ui j = 0; j < u_nbrs_count; ++j) {
      V_ID u_nbr = u_nbrs[j];
      // If this node is not their parent nodes and has less index(which means haven't visited yet).
      if (order_index_2[u_nbr] < order_index_2[u]) {
        connection_2[offset_2[i]] = order_index_2[u_nbr];
        offset_2[i] += 1;
      }
    }
  }

  int64_t res_all_1 = 0;
  ui u = order_1[0];
  static V_ID res_r_1[32];

  std::cout << "Before" << std::endl;

  auto f_4 = std::chrono::steady_clock::now();

  // Local Enumeration 1
  u = order_1[0];
  // total_1 = 0;
  // int current_order = 0;
  std::vector<ui> local_4;
  // std::vector<>isll

  ui u_1_0 = order_1[0];
  for (ui i = 0; i < candidates_count[u_1_0]; i++) {
    res_1[0][i] = candidates[u_1_0][i];
    res_all_1 += 1;
  }
  candidates_l_1[0] = res_all_1;

  // (5.1) Load edges information,std::cout <<"Load edge->" << std::endl;
  // std::cout <<"Test 2" << std::endl;
  std::vector<std::pair<ui, ui>> p_e;

  for (ui i = 1; i < order_count_1; i++) {
    for (ui j = 1; j < order_count_2; j++) {
      // std::cout <<"Tmp:" << tmp << std::endl;
      //  if (order_1[i] != order_2[j]) {
      if (order_index[order_2[j]] > order_index[order_1[i]]) {
        ui tmp = order_index[order_1[i]];
        if (std::find(pattern_e[tmp].begin(), pattern_e[tmp].end(), order_2[j]) !=
            pattern_e[tmp].end()) {
          p_e.emplace_back(i, j);
        }
      } else if (order_index[order_2[j]] < order_index[order_1[i]]) {
        ui tmp = order_index[order_2[j]];
        if (std::find(pattern_e[tmp].begin(), pattern_e[tmp].end(), order_1[i]) !=
            pattern_e[tmp].end()) {
          p_e.emplace_back(i, j);
        }
      }
      //}
    }
  }
  // std::cout << "This" << p_e.size() << std::endl;

  // (5.2) Load isomorphism information.
  // . . .
  std::vector<std::pair<ui, ui>> i_e;

  for (ui i = 1; i < order_count_1; i++) {
    for (ui j = 1; j < order_count_2; j++) {
      if (order_1[i] != order_2[j] &&
          query_graph->getVertexLabel(order_1[i]) == query_graph->getVertexLabel(order_2[j])) {
        i_e.emplace_back(i, j);
      }
    }
  }

  L_ID *l_1 = new L_ID[order_count_1];
  L_ID *l_2 = new L_ID[order_count_2];
  bool *mor = new bool[order_count_2];

  V_ID *con_2 = new V_ID[order_count_2];

  V_ID *con_2_2 = new V_ID[order_count_1];

  for (ui i = 0; i < order_count_1; i++) {
    l_1[i] = query_graph->getVertexLabel(order_1[i]);
    con_2_2[i] = 0;
  }

  for (ui i = 0; i < order_count_2; i++) {
    con_2[i] = 0;
    mor[i] = false;
    l_2[i] = query_graph->getVertexLabel(order_2[i]);
    for (ui j = 0; j < i; j++) {
      if (l_2[i] == l_2[j]) {
        mor[i] = true;
        mor[j] = true;
      }
    }
  }

  std::vector<V_ID> q_all;
  ui p_1 = 0;
  ui q_1 = 0;
  for (auto &t : p_e) {
    ui p = t.first;
    ui q = t.second;
    con_2[q] += 1;
    con_2_2[p] += 1;

    if (p > p_1) {
      p_1 = p;
    }
    if (q > q_1) {
      q_1 = q;
    }
  }

  for (auto &t : i_e) {
    // std::cout <<"p_1 and i_e[t].first" << p_1 << " " << i_e[t].first << std::endl;
    if (t.first > p_1) {
      p_1 = t.first;
    }
    if (t.second > q_1) {
      q_1 = t.second;
    }
  }

  ui p_1_1 = p_1 + 1;
  ui q_1_1 = q_1 + 1;

  static bool morphism[32];

  for (ui i = 0; i < query_count; i++) {
    morphism[i] = false;
    for (ui j = 0; j < query_count; j++) {
      if (i != j &&
          query_graph->getVertexLabel(order[i]) == query_graph->getVertexLabel(order[j])) {
        morphism[i] = true;
        break;
      }
    }
  }

  auto f_5 = std::chrono::steady_clock::now();

  if (order_count_2 <= 1) {
    ui bfs_order_1 = 1;

    std::cout << "Enumerate 1" << std::endl;
    enumeration_bfs2(visited,
                     res_all_1,
                     candidates_index,
                     candidates_count_index,
                     order,
                     tree,
                     res_r_1,
                     bfs_order_1,
                     query_count,
                     query_graph,
                     data_graph,
                     order_index,
                     connection,
                     offset,
                     res_1,
                     parent_offset_1,
                     candidates_l_1,
                     visit_local_2,
                     candidates_max_count,
                     morphism);

    int64_t embedding = exploreGraphQLStyle(data_graph,
                                            query_graph,
                                            candidates_index,
                                            candidates_count_index,
                                            order,
                                            10000000,
                                            res_all_1);

    f_5 = std::chrono::steady_clock::now();

    std::cout << embedding << "," << 0 << ","
              << std::chrono::duration_cast<std::chrono::nanoseconds>(f_5 - f_4).count() /
                     1000000000.0
              << ","
              << std::chrono::duration_cast<std::chrono::nanoseconds>(f_5 - f_4).count() /
                     1000000000.0
              << "," << res_all_1 << std::endl;

    // freopen("output.txt", "a", stdout);
    // std::cout <<embedding << "," << 0 << ","
    // <<std::chrono::duration_cast<std::chrono::nanoseconds>(f_5 - f_4).count()/1000000000.0  <<
    // "," <<std::chrono::duration_cast<std::chrono::nanoseconds>(f_5 - f_4).count()/1000000000.0 <<
    // "," << res_all_1 << std::endl; fclose(stdout);
    return true;

  } else {
    ui bfs_order_1 = 1;

    std::cout << "Enumerate 1" << std::endl;
    enumeration_bfs2(visited,
                     res_all_1,
                     candidates_index,
                     candidates_count_index,
                     order_1,
                     tree,
                     res_r_1,
                     bfs_order_1,
                     order_count_1,
                     query_graph,
                     data_graph,
                     order_index_1,
                     connection_1,
                     offset_1,
                     res_1,
                     parent_offset_1,
                     candidates_l_1,
                     visit_local_2,
                     candidates_max_count,
                     morphism_1);

    f_5 = std::chrono::steady_clock::now();
  }
  std::cout << "Done with enumeration 1" << std::endl;

  auto f_6 = std::chrono::steady_clock::now();

  static V_ID *visit_local_4 = new V_ID[data_count * query_count];
  std::fill(visit_local_4, visit_local_4 + data_count * query_count, 0);

  //   (5) Parallel combination.
  //   In this section, we are using the parallel method.

  static int *res_s = new int[Q_LIMIT];

  static bool **valid_1 = new bool *[query_count];
  static bool **valid_2 = new bool *[query_count];

  for (ui i = 0; i < query_count; i++) {
    valid_1[i] = new bool[candidates_l_1[order_count_1 - 1]];
    valid_2[i] = new bool[candidates_l_2[order_count_2 - 1]];
    std::fill(valid_1[i], valid_1[i] + candidates_l_1[order_count_1 - 1], false);
    std::fill(valid_2[i], valid_2[i] + candidates_l_2[order_count_2 - 1], false);
  }

  std::cout << "Test" << std::endl;
  for (ui i = 0; i < candidates_l_1[order_count_1 - 1]; i++) {
    valid_1[order_count_1 - 1][i] = true;
  }

  for (ui i = 0; i < candidates_l_2[order_count_2 - 1]; i++) {
    valid_2[order_count_2 - 1][i] = true;
  }

  reverse_cuts(res_1,
               parent_offset_1,
               children_offset_1,
               candidates_l_1,
               order_count_1,
               res_1_n,
               parent_offset_1_n,
               candidates_l_1_n,
               valid_1);
  reverse_cuts(res_2,
               parent_offset_2,
               children_offset_2,
               candidates_l_2,
               order_count_2,
               res_2_n,
               parent_offset_2_n,
               candidates_l_2_n,
               valid_2);

  // Print Children information::
  for (ui i = 0; i < query_count; i++) {
    ui tmp_u = order[i];
    std::cout << tmp_u << ",children:[";
    for (ui j = 0; j < tree[tmp_u].children_count; j++) {
      std::cout << "," << tree[tmp_u].children[j];
    }
    ui u_nbrs_count_1;
    V_ID *u_nbrs_1 = query_graph->getVertexNeighbors(tmp_u, u_nbrs_count_1);

    std::cout << "]" << std::endl;
    std::cout << "Neighborhood:[";
    for (ui j = 0; j < u_nbrs_count_1; j++) {
      std::cout << "," << u_nbrs_1[j];
    }
    std::cout << "]" << std::endl;
  }

  V_ID **res_1_r = new V_ID *[candidates_l_1[order_count_1 - 1]];
  V_ID **res_2_r = new V_ID *[candidates_l_2[order_count_2 - 1]];

  for (int64_t i = 0; i < candidates_l_1[order_count_1 - 1]; i++) {
    res_1_r[i] = new V_ID[order_count_1];
    for (ui j = 0; j < order_count_1; j++) {
      res_1_r[i][j] = 0;
    }
  }

  std::cout << "Initial Test" << std::endl;

  for (int64_t i = 0; i < candidates_l_1[order_count_1 - 1]; i++) {
    res_1_r[i][order_count_1 - 1] = res_1[order_count_1 - 1][i];
    int64_t t = i;
    for (int j = order_count_1 - 2; j >= 0; j--) {
      t = parent_offset_1[j + 1][t];
      res_1_r[i][j] = res_1[j][t];
    }
  }
  std::cout << "End point 1:" << std::endl;
  for (int64_t i = 0; i < candidates_l_2[order_count_2 - 1]; i++) {
    res_2_r[i] = new V_ID[order_count_2];
  }
  for (int64_t i = 0; i < candidates_l_2[order_count_2 - 1]; i++) {
    res_2_r[i][order_count_2 - 1] = res_2[order_count_2 - 1][i];
    int64_t t = i;
    for (int j = order_count_2 - 2; j >= 0; j--) {
      t = parent_offset_2[j + 1][t];
      res_2_r[i][j] = res_2[j][t];
    }
  }
  std::cout << std::endl << "Test all" << std::endl;

  bool *visit_q_1 = new bool[order_count_1];

  std::fill(visit_q_1, visit_q_1 + order_count_1, false);

  p_1 = p_1_1 - 1;
  std::cout << order_count_1 - p_1 << std::endl;
  std::vector<ui> p_all;
  for (ui i = 0; i < order_count_2; i++) {
    if (con_2[i] > 0) {
      q_all.push_back(i);
    }
  }

  for (ui i = 0; i < order_count_1; i++) {
    if (con_2_2[i] > 0) {
      p_all.push_back(i);
    }
  }

  int64_t call_count = 0;

  for (int64_t d_1 = 0; d_1 < candidates_l_1[order_count_1 - 1]; d_1++) {
    res_s[d_1] = 0;
  }

  std::cout << "I->" << candidates_l_1[order_count_1 - 1] << std::endl;
  std::cout << "J->" << candidates_l_2[order_count_2 - 1] << std::endl;

  double timer_middle = omp_get_wtime();
  int64_t total_sum = 0;
  V_ID res_1_1[32];

  double timer_local_enumeration = omp_get_wtime();

  bool *visited_j = new bool[candidates_l_2[order_count_2 - 1]];
  for (int64_t k = 0; k < candidates_l_2[order_count_2 - 1]; k++) {
    visited_j[k] = false;
  }

  if (candidates_l_1[order_count_1 - 1] < candidates_l_2[order_count_2 - 1]) {
    for (int64_t d_1 = 0; d_1 < candidates_l_1[0]; d_1++) {
      int64_t start_1, end_1;
      ui order_s = 0;
      find_children_range(children_offset_1, candidates_l_1, p_1_1, order_s, start_1, end_1, d_1);
      // V_ID r_1,r_2;
      int64_t d_2;
      for (d_2 = 0; d_2 < candidates_l_2[0]; d_2++) {
        if (res_1[0][d_1] == res_2[0][d_2]) {
          break;
        }
      }
      // std::cout <<"d_1" << std::endl;
      for (int64_t i = start_1; i < end_1; i++) {
        res_1_1[p_1] = res_1[p_1][i];
        int64_t j2 = i;
        for (int ii = p_1; ii > 0; ii--) {
          j2 = parent_offset_1[ii][j2];
          res_1_1[ii - 1] = res_1[ii - 1][j2];
        }
        ui p2, q2;
        for (auto &t : i_e) {
          p2 = t.first;
          visited[res_1_1[p2]] = true;
        }
        for (auto &t : p_e) {
          p2 = t.first;
          q2 = t.second;
          int64_t tt = data_count * q2;
          std::unordered_map<L_ID, std::vector<ui>> *v_nl = data_graph->getVertexNL(res_1_1[p2]);
          std::vector<V_ID> &tmp = (*v_nl)[l_2[q2]];
          for (unsigned int m : tmp) {
            visit_local_4[tt + m] += 1;
          }
        }
        int64_t total = 0;
        bool all_exist = false;
        int64_t start_2[32], end_2[32];
        ui t, q, q_add;
        t = 0;
        int64_t call_count_2 = 0;
        if (t < q_all.size()) {
          q = q_all[t];
          q_add = q + 1;
          int64_t tmp = data_count * q;
          // Find the range of children
          find_children_range(
              children_offset_2, candidates_l_2, q_add, order_s, start_2[t], end_2[t], d_2);
          if (q_add == order_count_2) {
            std::cout << con_2[q] << "," << q << std::endl;
            for (int64_t j = start_2[t]; j < end_2[t]; j++) {
              V_ID *res_t_2 = res_2_r[j];
              call_count_2 += 1;
              if (visit_local_4[tmp + res_t_2[q]] >= con_2[q]) {
                all_exist = true;
                for (auto &t_2 : i_e) {
                  ui q_3 = t_2.second;
                  if (visited[res_t_2[q_3]]) {
                    all_exist = false;
                    break;
                  }
                }
                total += all_exist;
              }
            }
          } else {
            // std::cout << "t" << std::endl;
            for (int64_t j = start_2[t]; j < end_2[t]; j++) {
              call_count_2++;
              if (visit_local_4[tmp + res_2[q][j]] == con_2[q]) {
                total += edge_next(res_2_r,
                                   visited_j,
                                   visit_local_4,
                                   data_count,
                                   con_2,
                                   visited,
                                   children_offset_2,
                                   candidates_l_2,
                                   q_add,
                                   j,
                                   order_count_2,
                                   i_e,
                                   q_all,
                                   t,
                                   start_2,
                                   end_2,
                                   res_2,
                                   q_1,
                                   call_count_2);
              }
            }
          }
        } else {
          // bool all_exist;
          find_children_range(
              children_offset_2, candidates_l_2, order_count_2, order_s, start_2[t], end_2[t], d_2);
          for (int64_t j = start_2[t]; j < end_2[t]; j++) {
            V_ID *res_t_2 = res_2_r[j];
            call_count_2 += 1;
            all_exist = true;
            for (auto &t_2 : i_e) {
              call_count_2 += 1;
              ui q_3 = t_2.second;
              if (visited[res_t_2[q_3]]) {
                all_exist = false;
                break;
              }
            }
            total += all_exist;
          }
        }
        // #pragma omp end parallel for simd
        if (p_1_1 == order_count_1) {
          total_sum += total;
          call_count += call_count_2;
        } else {
          int64_t start_1_1;
          int64_t end_1_1;
          int64_t pp = i;
          find_children_range(
              children_offset_1, candidates_l_1, order_count_1, p_1, start_1_1, end_1_1, pp);
          // std::cout <<start_1_1 << std::endl;
          int64_t tt = end_1_1 - start_1_1;
          if (tt < 0) {
            tt = 0;
          }
          total_sum += total * tt;
          call_count += call_count_2 + 1;
          // std::cout <<"Middle ### -> " << tt << std::endl;
        }

        for (auto &t2 : p_e) {
          p2 = t2.first;
          q2 = t2.second;
          int64_t tt = data_count * q2;
          std::unordered_map<L_ID, std::vector<ui>> *v_nl = data_graph->getVertexNL(res_1_1[p2]);
          std::vector<V_ID> &tmp = (*v_nl)[l_2[q2]];
          for (unsigned int m : tmp) {
            visit_local_4[tt + m] = 0;
          }
        }

        for (auto &t2 : i_e) {
          p2 = t2.first;
          visited[res_1_1[p2]] = false;
        }
      }
    }
  } else {
    // Fix comment 1
    ui tmp_p_q = p_1_1;
    p_1_1 = q_1_1;
    q_1_1 = tmp_p_q;

    tmp_p_q = p_1;
    p_1 = q_1;
    q_1 = tmp_p_q;

    for (auto &t : i_e) {
      ui tmp11 = t.first;
      t.first = t.second;
      t.second = tmp11;
    }
    for (auto &t : p_e) {
      ui tmp11 = t.first;
      t.first = t.second;
      t.second = tmp11;
    }

    for (int64_t d_1 = 0; d_1 < candidates_l_2[0]; d_1++) {
      int64_t start_1, end_1;
      ui order_s = 0;
      find_children_range(children_offset_2, candidates_l_2, p_1_1, order_s, start_1, end_1, d_1);
      // V_ID r_1,r_2;
      int64_t d_2;
      for (d_2 = 0; d_2 < candidates_l_1[0]; d_2++) {
        if (res_2[0][d_1] == res_1[0][d_2]) {
          break;
        }
      }

      // std::cout <<"d_1" << std::endl;
      for (int64_t i = start_1; i < end_1; i++) {
        res_1_1[p_1] = res_2[p_1][i];

        for (int ii = p_1; ii > 0; ii--) {
          int64_t j = i;
          j = parent_offset_2[ii][j];
          res_1_1[ii - 1] = res_2[ii - 1][j];
        }
        ui p2, q2;
        for (auto &t : i_e) {
          p2 = t.first;
          visited[res_1_1[p2]] = true;
        }
        for (auto &t : p_e) {
          p2 = t.first;
          q2 = t.second;
          int64_t tt = data_count * q2;
          std::unordered_map<L_ID, std::vector<ui>> *v_nl = data_graph->getVertexNL(res_1_1[p2]);
          std::vector<V_ID> &tmp = (*v_nl)[l_1[q2]];
          for (unsigned int m : tmp) {
            visit_local_4[tt + m] += 1;
          }
        }
        // Fix comment 2
        int64_t total = 0;
        bool all_exist = false;
        int64_t start_2[32], end_2[32];
        ui t, q, q_add;
        t = 0;
        int64_t call_count_2 = 0;
        if (t < p_all.size()) {
          q = p_all[t];
          q_add = q + 1;
          int64_t tmp = data_count * q;
          find_children_range(
              children_offset_1, candidates_l_1, q_add, order_s, start_2[t], end_2[t], d_2);
          if (q_add == order_count_1) {
            std::cout << con_2_2[q] << "," << q << std::endl;
            for (int64_t j = start_2[t]; j < end_2[t]; j++) {
              V_ID *res_t_2 = res_1_r[j];
              call_count_2 += 1;
              if (visit_local_4[tmp + res_t_2[q]] >= con_2_2[q]) {
                all_exist = true;
                for (auto &t_2 : i_e) {
                  ui q_3 = t_2.second;
                  if (visited[res_t_2[q_3]]) {
                    all_exist = false;
                    break;
                  }
                }
                total += all_exist;
              }
            }
          } else {
            // std::cout <<"all-->" << std::endl;
            for (int64_t j = start_2[t]; j < end_2[t]; j++) {
              call_count_2++;
              if (visit_local_4[tmp + res_1[q][j]] == con_2_2[q]) {
                total += edge_next(res_1_r,
                                   visited_j,
                                   visit_local_4,
                                   data_count,
                                   con_2_2,
                                   visited,
                                   children_offset_1,
                                   candidates_l_1,
                                   q_add,
                                   j,
                                   order_count_1,
                                   i_e,
                                   p_all,
                                   t,
                                   start_2,
                                   end_2,
                                   res_1,
                                   q_1,
                                   call_count_2);
              }
            }
          }
        } else {
          //
          find_children_range(
              children_offset_1, candidates_l_1, order_count_1, order_s, start_2[t], end_2[t], d_2);
          for (int64_t j = start_2[t]; j < end_2[t]; j++) {
            V_ID *res_t_2 = res_1_r[j];
            call_count_2 += 1;
            all_exist = true;
            for (auto &t_2 : i_e) {
              call_count_2 += 1;
              ui q_3 = t_2.second;
              if (visited[res_t_2[q_3]]) {
                all_exist = false;
                break;
              }
            }
            total += all_exist;
          }
        }
        // #pragma omp end parallel for simd
        if (p_1_1 == order_count_2) {
          total_sum += total;
          call_count += call_count_2;
        } else {
          int64_t start_1_1;
          int64_t end_1_1;
          int64_t pp = i;
          find_children_range(
              children_offset_2, candidates_l_2, order_count_2, p_1, start_1_1, end_1_1, pp);
          // std::cout <<start_1_1 << std::endl;
          int64_t tt = end_1_1 - start_1_1;
          if (tt < 0) {
            tt = 0;
          }
          total_sum += total * tt;
          call_count += call_count_2 + 1;
          // std::cout <<"Middle ### -> " << tt << std::endl;
        }

        for (t = 0; t < p_e.size(); t++) {
          p2 = p_e[t].first;
          q2 = p_e[t].second;
          int64_t tt = data_count * q2;
          std::unordered_map<L_ID, std::vector<ui>> *v_nl = data_graph->getVertexNL(res_1_1[p2]);
          std::vector<V_ID> &tmp = (*v_nl)[l_1[q2]];
          for (unsigned int m : tmp) {
            visit_local_4[tt + m] = 0;
          }
        }

        for (t = 0; t < i_e.size(); t++) {
          p2 = i_e[t].first;
          visited[res_1_1[p2]] = false;
        }
      }
    }
  }
  // std::cout <<"Total 2:" << total_2 << std::endl;
  std::cout << "Data count" << data_count << std::endl;
  total_result = total_sum;

  double timer_end = omp_get_wtime();
  double timer_took =
      timer_end - timer_middle +
      std::chrono::duration_cast<std::chrono::nanoseconds>(f_5 - f_4).count() / 1000000000.0 +
      std::chrono::duration_cast<std::chrono::nanoseconds>(f_6 - f_5).count() /
          1000000000.0;  // timer_start1;
  // double timer_took3 = timer_took;

  std::cout << "call count->" << call_count << ", per call nanoseconds->"
            << timer_took * 1000000000.0 / call_count << std::endl;
  std::cout << "total results:" << total_result << ",Time:" << timer_took << ",local enumeration-->"
            << (timer_local_enumeration - timer_start1) * 100 / timer_took << std::endl;

  //   (6) Global node based-enumeration result comparasion
  // std::cout <<"Start enumeration" <<  endl;
  u = order[0];

  static V_ID **res = new V_ID *[query_count];
  static V_ID **cuTS = new V_ID *[query_count];

  for (ui i = 0; i < query_count; ++i) {
    res[i] = new V_ID[candidates_max_count];
    cuTS[i] = new V_ID[candidates_max_count];
  }

  int64_t res_all = 0;

  // int64_t total = 0;

  std::fill(visited, visited + data_count, false);

  double timer_start = omp_get_wtime();
  auto first = std::chrono::steady_clock::now();

  auto f_2 = first;
  auto f_3 = first;
  auto f_10 = first;
  // traditional enumeration:

  f_10 = std::chrono::steady_clock::now();

  if (tree[u].children_count == 0) {
    int length = P_Candidates[u].size();
    for (int i = 0; i < length; i++) {
      res_all += 1;
    }
  } else {
    // ui current_order = 0;
    ui bfs_order = 0;
    query_count -= 1;
    // int64_t single_res;
    std::cout << "Start-->" << std::endl;
    int64_t bfs_count = 0;

    for (const auto &it4 : P_Candidates[u]) {
      res_all += 1;
      // [current_order] = it4->first;
      candidates_2[bfs_order][bfs_count++] = it4.first;
      visited[it4.first] = true;
      visited[it4.first] = false;
    }

    f_2 = std::chrono::steady_clock::now();

    candidates_l[bfs_order++] = bfs_count;
    query_count += 1;
    f_3 = std::chrono::steady_clock::now();
  }

  // auto second =std::chrono::steady_clock::now();
  std::cout << "P_1 Time->"
            << std::chrono::duration_cast<std::chrono::nanoseconds>(f_5 - f_4).count() /
                   1000000000.0
            << std::endl;
  std::cout << "P_2 Time->"
            << std::chrono::duration_cast<std::chrono::nanoseconds>(f_6 - f_5).count() /
                   1000000000.0
            << std::endl;
  std::cout << "DFS Time->"
            << std::chrono::duration_cast<std::chrono::nanoseconds>(f_2 - first).count() /
                   1000000000.0
            << std::endl;

  timer_end = omp_get_wtime();
  std::cout << "BFS->"
            << std::chrono::duration_cast<std::chrono::nanoseconds>(f_3 - f_2).count() /
                   1000000000.0
            << std::endl;
  double last_r = timer_took;
  timer_took = timer_end - timer_start;
  std::cout << "Node total results:" << candidates_l[query_count - 1] - total_result
            << ",Time:" << timer_took << ", ratio->"
            << timer_took * 100 / (timer_took + timer_start1 - timer_all_s) << " %, Accelera rate->"
            << timer_took / last_r << std::endl;

  // freopen("output.txt", "a", stdout);

  // std::cout <<total_result << "," << candidates_l[query_count-1] - total_result << "," <<
  // timer_took3
  //  << "," <<std::chrono::duration_cast<std::chrono::nanoseconds>(f_3 - f_2).count()/1000000000.0
  //  << ","
  //  << call_count << std::endl;

  // fclose(stdout);

  return true;
  // NTE Tree:
}
//
//
//
// NLF data_graph, query_graph, ID of the vertex node. total count.
//

void ZFComputeNLF(Graph *data_graph, Graph *query_graph, V_ID i, ui &count, ui *tmp) {
  // std::cout <<"Test 0" << std::endl;
  L_ID label = query_graph->getVertexLabel(i);
  ui degree = query_graph->getVertexDegree(i);
  // std::cout <<"Test 00" << std::endl;
  std::unordered_map<L_ID, ui> *query_nlf = query_graph->getVertexNLF(i);

  // data vertex count;
  ui data_v_count;
  ui *data_v = data_graph->getVerticesByLabel(label, data_v_count);
  count = 0;
  // std::cout <<"Test 1" << std::endl;
  for (ui j = 0; j < data_v_count; ++j) {
    ui v = data_v[j];
    if (data_graph->getVertexDegree(v) >= degree) {
      // NFL check
      std::unordered_map<L_ID, ui> *data_nlf = data_graph->getVertexNLF(v);

      if (data_nlf->size() >= query_nlf->size()) {
        bool valid = true;

        // Label, count in the (nlf)
        for (auto item : *query_nlf) {
          auto element = data_nlf->find(item.first);
          if (element == data_nlf->end() || element->second < item.second) {
            valid = false;
            break;
          }
        }

        if (valid) {
          if (tmp != nullptr) {
            tmp[count] = v;  //
          }
          count += 1;  // Recored count of the number
        }
      }
    }
  }
}
