// Copyright [2022] <nebula>
#include <fstream>
#include <unordered_map>
#include <vector>

#include "graph.h"
#include "subgraph.h"
// TODO:
//   (1) Build from the CSR compressed files.
//   (2) Build Reverse Refinement
//   (3) Print_out the Tree.
//

int ceci() {
  std::string input_query_graph_file;  // = argv[1];
  std::string input_data_graph_file;   // = argv[2];

  Graph* query_graph = new Graph();
  query_graph->loadGraph(input_query_graph_file);

  Graph* data_graph = new Graph();
  data_graph->loadGraph(input_data_graph_file);

  // std::cout"-----" << std::endl:endl;
  // std::cout"Query Graph Meta Information" << std::endl:endl;
  query_graph->printGraph();

  // std::cout"-----" << std::endl:endl;
  //   data_graph->printGraph();

  // std::cout"--------------------------------------------------------------------" <<
  // std::endl:endl;

  /**
   * Start queries.
   */

  // std::cout"Start queries..." << std::endl:endl;
  // std::cout"-----" << std::endl:endl;

  ui** candidates = nullptr;
  ui* candidates_count = nullptr;

  TreeNode* ceci_tree = nullptr;
  ui* ceci_order = nullptr;
  ui* provenance = nullptr;

  std::vector<std::unordered_map<V_ID, std::vector<V_ID>>>
      P_Candidates;  //  Parent, first branch, second branch.
  std::vector<std::unordered_map<V_ID, std::vector<V_ID>>> P_Provenance;
  // std::cout"Provenance Function: " << std::endl:endl;

  bool result = CECIFunction(data_graph,
                             query_graph,
                             candidates,
                             candidates_count,
                             ceci_order,
                             provenance,
                             ceci_tree,
                             P_Candidates,
                             P_Provenance);
  // std::cout"Function End: " << std::endl:endl;
  // std::vector<std::unordered_map<V_ID,std::vector<V_ID>>> TE_Candidates; //  <v_p, {set of v_c}>
  // std::vector<>

  return result;
}
