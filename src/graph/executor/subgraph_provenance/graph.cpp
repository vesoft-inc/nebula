// Copyright [2022] <nebula>
#include "graph.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <vector>

void Graph::printGraph() {
  std::cout << "|V|: " << v_count << ", |E|: " << e_count << ", |L|: " << l_count << std::endl;
  std::cout << "Max Degree: " << max_degree << ", Max Label Frequency: " << max_label_frequency
            << std::endl;
  printf("Neighbourhood: ");
  // for (int i = 0; i < v_count; i ++) {
  //     printf("%d, ", offsets[i]);
  //
  // }
  printf("\n");
  for (ui i = 0; i < v_count; i++) {
    printf("V_ID: %d, offsets: %d:", i, offsets[i]);
    for (ui j = offsets[i]; j < offsets[i + 1]; j++) {
      printf("%d, ", neighbors[j]);
      std::cout << " (" << getVertexLabel(neighbors[j]) << ") ";
    }
    printf("\n");
  }

  printf("\n");
}

void Graph::BuildReverseIndex() {
  reverse_index = new ui[v_count];
  reverse_index_offsets = new ui[l_count + 1];
  reverse_index_offsets[0] = 0;

  ui total = 0;
  for (ui i = 0; i < l_count; ++i) {
    reverse_index_offsets[i + 1] = total;
    total += labels_frequency[i];
  }

  for (ui i = 0; i < v_count; ++i) {
    L_ID label = labels[i];
    reverse_index[reverse_index_offsets[label + 1]++] = i;
  }
}

void Graph::BuildNLCF() {  // neighbors count
  // Map from [label_id] -> count of this label count value.
  nlf = new std::unordered_map<L_ID, ui>[v_count];
  nl = new std::unordered_map<L_ID, std::vector<ui>>[v_count];
  for (ui i = 0; i < v_count; ++i) {
    ui count;
    V_ID *neighbors_tmp = getVertexNeighbors(i, count);
    std::vector<ui> v_t;
    // count is the number of the offsets[id + 1] - offsets[id]; which is the number of neighbours.
    for (ui j = 0; j < count; ++j) {
      V_ID u = neighbors_tmp[j];
      L_ID label = getVertexLabel(u);
      // If not found, this should be 0;
      if (nlf[i].find(label) == nlf[i].end()) {
        nlf[i][label] = 0;
        nl[i].emplace(label, std::vector<ui>());
      }
      nlf[i][label] += 1;  // Add one after each count.
      nl[i][label].push_back(u);
    }
    for (auto &iter : nl[i]) {
      sort(iter.second.begin(), iter.second.end());
    }
  }
  return;
}

void Graph::loadGraphFromExecutor(unsigned int v_c,
                                  unsigned int l_c,
                                  unsigned int e_c,
                                  unsigned int *off,
                                  unsigned int *nei,
                                  unsigned int *lab) {
  this->v_count = v_c;
  this->l_count = l_c;
  this->e_count = e_c;
  this->offsets = new ui[v_c + 1];
  this->neighbors = new V_ID[e_c * 2];
  this->labels = new L_ID[v_c];

  for (ui i = 0; i < v_c + 1; ++i) {
    this->offsets[i] = off[i];
  }

  for (ui i = 0; i < e_c; ++i) {
    this->neighbors[i] = nei[i];
  }

  L_ID max_label_id = 0;
  for (ui i = 0; i < v_count; ++i) {
    this->labels[i] = lab[i];
    if (labels_frequency.find(lab[i]) == labels_frequency.end()) {
      labels_frequency[lab[i]] = 0;
      if (lab[i] > max_label_id) {
        max_label_id = lab[i];
      }
    }
  }
  // Initialize label count;
  if (labels_frequency.size() > max_label_id + 1) {
    l_count = labels_frequency.size();
  } else {
    l_count = max_label_id + 1;
  }

  BuildReverseIndex();
  BuildNLCF();
  return;
}

void Graph::loadGraph(const std::string &file_path) {
  std::ifstream graphFile(file_path);

  if (!graphFile.is_open()) {
    std::cout << "Error opening " << file_path << " ." << std::endl;
    exit(-1);
  }

  char type;
  graphFile >> type >> v_count >> e_count;
  offsets = new ui[v_count + 1];
  offsets[0] = 0;

  neighbors = new V_ID[e_count * 2];
  labels = new L_ID[v_count];
  l_count = 0;
  max_degree = 0;

  L_ID max_label_id = 0;
  std::vector<ui> neighbors_offsets(v_count, 0);
  while (graphFile >> type) {
    if (type == 'v') {  // Read Vertex, build index of id->label, records its degree.
      V_ID id;
      L_ID label;
      ui degree;
      graphFile >> id >> label >> degree;

      labels[id] = label;
      offsets[id + 1] = offsets[id] + degree;
      if (degree > max_degree) {
        max_degree = degree;
      }

      if (labels_frequency.find(label) == labels_frequency.end()) {
        labels_frequency[label] = 0;
        if (label > max_label_id) {
          max_label_id = label;
        }
      }
      labels_frequency[label] += 1;
    } else if (type == 'e') {  // Read edge.
      V_ID src;
      V_ID dst;
      graphFile >> src >> dst;

      ui offset_tmp = offsets[src] + neighbors_offsets[src];
      neighbors[offset_tmp] = dst;

      offset_tmp = offsets[dst] + neighbors_offsets[dst];
      neighbors[offset_tmp] = src;

      neighbors_offsets[src] += 1;
      neighbors_offsets[dst] += 1;
    }
  }

  graphFile.close();

  // Initialize label count;
  if (labels_frequency.size() > max_label_id + 1) {
    l_count = labels_frequency.size();
  } else {
    l_count = max_label_id + 1;
  }

  // Recheck the max_label_id;
  // std::cout"Test All: " << std::endl;

  for (auto item : labels_frequency) {
    // std::coutitem.second << " ";
    if (item.second > max_label_frequency) {
      max_label_frequency = item.second;
    }
  }

  BuildReverseIndex();
  BuildNLCF();
}
