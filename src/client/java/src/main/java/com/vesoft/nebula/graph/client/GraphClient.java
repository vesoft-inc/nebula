/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.graph.client;

import static com.google.common.base.Preconditions.checkArgument;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;
import com.facebook.thrift.transport.TTransportException;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.vesoft.nebula.graph.AuthResponse;
import com.vesoft.nebula.graph.ErrorCode;
import com.vesoft.nebula.graph.ExecutionResponse;
import com.vesoft.nebula.graph.GraphService;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphClient implements GraphClientIface {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphClient.class.getName());

    private int retry;
    private final int timeout;
    private long sessionId_;
    private TTransport transport = null;
    private GraphService.Client syncClient;
    private List<InetSocketAddress> nodes = Lists.newLinkedList();

    public GraphClient(Map<String, Integer> addresses, int timeout, int retry) {
        checkArgument(addresses != null && addresses.size() != 0);
        checkArgument(timeout > 0);
        checkArgument(retry > 0);

        for (Map.Entry<String, Integer> entry : addresses.entrySet()) {
            String host = entry.getKey();
            int port = entry.getValue();
            if (!InetAddresses.isInetAddress(host) || (port <= 0 || port >= 65535)) {
                throw new IllegalArgumentException(String.format("%s:%d is not a valid address", host, port));
            } else {
                nodes.add(new InetSocketAddress(host, port));
            }
        }
        this.timeout = timeout;
        this.retry = retry;
    }

    public GraphClient(Map<String, Integer> addresses) {
        this(addresses, DEFAULT_TIMEOUT_MS, DEFAULT_CONNECTION_RETRY_SIZE);
    }

    @Override
    public int connect(String username, String password) {
        while (retry != 0) {
            retry -= 1;

            Random random = new Random(System.currentTimeMillis());
            int index = random.nextInt(nodes.size());
            InetSocketAddress activeNode = nodes.get(index);

            String activeHost;
            InetAddress address = activeNode.getAddress();
            if (address instanceof Inet6Address) {
                activeHost = InetAddresses.toUriString(address);
            } else {
                activeHost = InetAddresses.toAddrString(address);
            }

            transport = new TSocket(activeHost,
                    activeNode.getPort(), timeout);
            TProtocol protocol = new TBinaryProtocol(transport);

            try {
                transport.open();
                syncClient = new GraphService.Client(protocol);
                AuthResponse result = syncClient.authenticate(username, password);
                if (result.getError_code() == ErrorCode.E_BAD_USERNAME_PASSWORD) {
                    LOGGER.error("User name or password error");
                    return ErrorCode.E_BAD_USERNAME_PASSWORD;
                }

                if (result.getError_code() != ErrorCode.SUCCEEDED) {
                    LOGGER.error(String.format("Host : %s error : %s",
                            activeHost, result.getError_msg()));
                } else {
                    sessionId_ = result.getSession_id();
                    return ErrorCode.SUCCEEDED;
                }
            } catch (TTransportException tte) {
                LOGGER.error("Connect failed: " + tte.getMessage());
            } catch (TException te) {
                LOGGER.error("Connect failed: " + te.getMessage());
            }
        }
        return ErrorCode.E_FAIL_TO_CONNECT;
    }

    @Override
    public void disconnect() {
        if (!checkTransportOpened(transport)) {
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
        if (!checkTransportOpened(transport)) {
            return ErrorCode.E_DISCONNECTED;
        }

        try {
            ExecutionResponse executionResponse = syncClient.execute(sessionId_, stmt);
            if (executionResponse.getError_code() != ErrorCode.SUCCEEDED) {
                LOGGER.error("execute error: " + executionResponse.getError_msg());
            }
            return executionResponse.getError_code();
        } catch (TException e) {
            LOGGER.error("Thrift rpc call failed: " + e.getMessage());
            return ErrorCode.E_RPC_FAILURE;
        }
    }

    @Override
    public int executeUpdate(String stmt) {
        return execute(stmt);
    }

    @Override
    public ResultSet executeQuery(String stmt) {
        ExecutionResponse executionResponse = internalExecuteQuery(stmt);
        if (Objects.isNull(executionResponse)) {
            return new ResultSet();
        } else {
            return new ResultSet(executionResponse.getColumn_names(),
                    executionResponse.getRows());
        }
    }

    private boolean checkTransportOpened(TTransport transport) {
        return !Objects.isNull(transport) && transport.isOpen();
    }

    private ExecutionResponse internalExecuteQuery(String stmt) {
        if (!checkTransportOpened(transport)) {
            LOGGER.error("Thrift rpc call failed");
            return null;
        }

        try {
            ExecutionResponse executionResponse = syncClient.execute(sessionId_, stmt);
            if (executionResponse.getError_code() == ErrorCode.SUCCEEDED) {
                return executionResponse;
            } else {
                LOGGER.error("execute error: " + executionResponse.getError_msg());
            }
        } catch (TException e) {
            LOGGER.error("Thrift rpc call failed: " + e.getMessage());
        }
        return null;
    }
}
