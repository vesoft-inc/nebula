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

/**
 * The Java thrift client wrapper.
 */
public class GraphClientImpl implements GraphClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphClientImpl.class.getName());

    private String host;
    private int port;
    private int retry;
    private final int timeout;
    private long sessionId_;
    private TTransport transport = null;
    private GraphService.Client client;

    /**
     * The Constructor of GraphClient.
     *
     * @param host      The host of graph services.
     * @param port      The port of graph services.
     * @param timeout   The timeout of RPC request.
     * @param retry     The number of retries when connection failure.
     */
    public GraphClientImpl(String host, int port, int timeout, int retry) {
        checkArgument(timeout > 0);
        checkArgument(retry > 0);
        if (!InetAddresses.isInetAddress(host) || (port <= 0 || port >= 65535)) {
            throw new IllegalArgumentException(String.format("%s:%d is not a valid address", host, port));
        }

        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.retry = retry;
    }

    /**
     * The Constructor of GraphClient.
     *
     * @param host      The host of graph services.
     */
    public GraphClientImpl(String host) {
        this(host, DEFAULT_PORT, DEFAULT_TIMEOUT_MS, DEFAULT_CONNECTION_RETRY_SIZE);
    }

    /**
     * Connect to the Graph Services.
     *
     * @param username The user's name, default is user.
     * @param password The user's password, default is password.
     * @return         The ErrorCode of status, 0 is succeeded.
     */
    @Override
    public int connect(String username, String password) {
        while (retry != 0) {
            retry -= 1;

            transport = new TSocket(host, port, timeout);
            TProtocol protocol = new TBinaryProtocol(transport);

            try {
                transport.open();
                client = new GraphService.Client(protocol);
                AuthResponse result = client.authenticate(username, password);
                if (result.getError_code() == ErrorCode.E_BAD_USERNAME_PASSWORD) {
                    LOGGER.error("User name or password error");
                    return ErrorCode.E_BAD_USERNAME_PASSWORD;
                }

                if (result.getError_code() != ErrorCode.SUCCEEDED) {
                    LOGGER.error(String.format("Connect host %s failed : %s",
                            host, result.getError_msg()));
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

    /**
     * Sign out from Graph Services.
     */
    @Override
    public void disconnect() {
        if (!checkTransportOpened(transport)) {
            return;
        }

        try {
            client.signout(sessionId_);
        } catch (TException e) {
            LOGGER.error("Disconnect error: " + e.getMessage());
        } finally {
            transport.close();
        }
    }

    /**
     * Execute the query sentence.
     *
     * @param stmt The query sentence.
     * @return     The ErrorCode of status, 0 is succeeded.
     */
    @Override
    public int execute(String stmt) {
        if (!checkTransportOpened(transport)) {
            return ErrorCode.E_DISCONNECTED;
        }

        try {
            ExecutionResponse executionResponse = client.execute(sessionId_, stmt);
            if (executionResponse.getError_code() != ErrorCode.SUCCEEDED) {
                LOGGER.error("execute error: " + executionResponse.getError_msg());
            }
            return executionResponse.getError_code();
        } catch (TException e) {
            LOGGER.error("Thrift rpc call failed: " + e.getMessage());
            return ErrorCode.E_RPC_FAILURE;
        }
    }

    /**
     * Execute the query sentence.
     *
     * @param stmt The query sentence.
     * @return     The ErrorCode of status, 0 is succeeded.
     */
    @Override
    public int executeUpdate(String stmt) {
        return execute(stmt);
    }

    /**
     * Execute the query sentence which will return a ResultSet.
     *
     * @param stmt The query sentence.
     * @return     The ErrorCode of status, 0 is succeeded.
     */
    @Override
    public ResultSet executeQuery(String stmt) throws ConnectionException, NGQLException, TException {
        if (!checkTransportOpened(transport)) {
            LOGGER.error("Thrift rpc call failed");
            throw new ConnectionException();
        }

        ExecutionResponse executionResponse = client.execute(sessionId_, stmt);
        if (executionResponse.getError_code() == ErrorCode.SUCCEEDED) {
            return new ResultSet(executionResponse.getColumn_names(),
                                 executionResponse.getRows());
        } else {
            LOGGER.error("Execute error: " + executionResponse.getError_msg());
            throw new NGQLException();
        }
    }

    private boolean checkTransportOpened(TTransport transport) {
        return !Objects.isNull(transport) && transport.isOpen();
    }
}
