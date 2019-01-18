/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package nebula.graph.client;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;
import com.facebook.thrift.transport.TTransportException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import nebula.graph.AuthResponse;
import nebula.graph.ErrorCode;
import nebula.graph.ExecutionResponse;
import nebula.graph.GraphService;
import nebula.graph.client.network.GraphInetAddresses;
import nebula.graph.client.network.GraphTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GraphClient implements GraphClientIface {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphClient.class.getName());

    private int currentNodeIndex;
    private long sessionId_;
    private TTransport transport = null;
    private GraphService.Client syncClient;
    private volatile List<GraphTransportAddress> nodes = Collections.emptyList();

    public void addTransportAddress(String hostAddress, int hostPort) {
        if (!GraphInetAddresses.isInetAddress(hostAddress)) {
            LOGGER.error("This is not a valid ip address : " + hostAddress);
        }
        try {
            nodes.add(new GraphTransportAddress(InetAddress.getByName(hostAddress), hostPort));
        } catch (UnknownHostException e) {
            LOGGER.error(e.getMessage());
        }
    }

    public int getNodesSize() {
        return nodes.size();
    }

    public GraphTransportAddress getActiveTransportAddress() {
        if (currentNodeIndex == 0) {
            return null;
        }
        return nodes.get(currentNodeIndex--);
    }

    @Override
    public int connect(String username, String password) {

        GraphTransportAddress activeNodeAddr = null;
        currentNodeIndex = getNodesSize();

        while (true) {
            try {
                activeNodeAddr = getActiveTransportAddress();

                if (activeNodeAddr == null) {
                    LOGGER.error("No transport address are available");
                    return ErrorCode.E_FAIL_TO_CONNECT;
                }

                transport = new TSocket(activeNodeAddr.getAddress(),
                        activeNodeAddr.getPort(), conn_timeout_ms);

                TProtocol protocol = new TBinaryProtocol(transport);

                syncClient = new GraphService.Client(protocol);

                transport.open();

                AuthResponse result = syncClient.authenticate(username, password);

                if (result.getError_code() != ErrorCode.SUCCEEDED) {
                    LOGGER.error("Host : "
                            + activeNodeAddr.getAddress() + "error : "
                            + result.getError_msg());
                } else {
                    sessionId_ = result.getSession_id();
                    return ErrorCode.SUCCEEDED;
                }
            } catch (TTransportException tte) {
                LOGGER.error("Connect failed: " + tte.getMessage());
            } catch (TException te) {
                LOGGER.error("Connect failed: " + te.getMessage());
            } catch (Exception e) {
                LOGGER.error("Connect failed: " + e.getMessage());
            }
        }
    }

    @Override
    public void disconnect() {

        if (transport == null || !transport.isOpen()) {
            return;
        }

        try {
            syncClient.signout(sessionId_);
        } catch (TException e) {
            LOGGER.error("Disconnect error: " + e.getMessage());
        } finally {
            transport.close();
        }
    }

    @Override
    public int execute(String stmt) {

        ExecutionResponse executionResponse = null;

        if (transport == null || !transport.isOpen()) {
            return ErrorCode.E_DISCONNECTED;
        }

        try {
            executionResponse = syncClient.execute(sessionId_, stmt);
            if (executionResponse.getError_code() != ErrorCode.SUCCEEDED) {
                LOGGER.error("execute error: " + executionResponse.getError_msg());
                return executionResponse.getError_code();
            }

        } catch (TException e) {
            LOGGER.error("Thrift rpc call failed: " + e.getMessage());
            return ErrorCode.E_RPC_FAILURE;
        }

        return ErrorCode.SUCCEEDED;
    }

    @Override
    public int executeUpdate(String stmt) {
        return execute(stmt);
    }

    @Override
    public ResultSet executeQuery(String stmt) {
        ExecutionResponse executionResponse = internalExecuteQuery(stmt);
        return new ResultSet(executionResponse.getColumn_names(),
                executionResponse.getRows());
    }

    public ExecutionResponse internalExecuteQuery(String stmt) {
        ExecutionResponse executionResponse = null;

        if (transport == null || !transport.isOpen()) {
            LOGGER.error("Thrift rpc call failed");
            return null;
        }

        try {
            executionResponse = syncClient.execute(sessionId_, stmt);
            if (executionResponse.getError_code() != ErrorCode.SUCCEEDED) {
                LOGGER.error("execute error: " + executionResponse.getError_msg());
                return executionResponse;
            }
        } catch (TException e) {
            LOGGER.error("Thrift rpc call failed: " + e.getMessage());
        }
        return null;
    }
}
