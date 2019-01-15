/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package nebula.graph.client;

public class GraphServerNode {

  // For load balancing
  private int hostPort;
  private String nodeId;
  private String hostAddress;

  public GraphServerNode(String nodeId, String hostAddress, int hostPort) {
    this.nodeId = nodeId;
    this.hostAddress = hostAddress;
    this.hostPort = hostPort;
  }

  public GraphServerNode(String hostAddress, int hostPort) {
    this.hostAddress = hostAddress;
    this.hostPort = hostPort;
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public int getHostPort() {
    return hostPort;
  }


  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }


  public void setHostAddress(String hostAddress) {
    this.hostAddress = hostAddress;
  }

  public void setHostPort(int hostPort) {
    this.hostPort = hostPort;
  }

}
