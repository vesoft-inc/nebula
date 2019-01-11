/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
package nebula.graph.client;

import com.facebook.thrift.TApplicationException;
import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransport;
import com.facebook.thrift.transport.TTransportException;
import nebula.graph.AuthResponse;
import nebula.graph.ErrorCode;
import nebula.graph.ExecutionResponse;
import nebula.graph.GraphService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphClient implements GraphClientIface {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphClient.class.getName());

    private String addr_;
    private short port_;
    private long sessionId_;
    private GraphService.Client syncClient;
    private TTransport transport = null;

    public GraphClient(String addr, short port) {
        this.addr_ = addr;
        this.port_ = port;
    }

    @Override
    public int connect(String username, String password) {

        try {
            transport = new TSocket(addr_, port_, conn_timeout_ms);

            TProtocol protocol = new TBinaryProtocol(transport);

            syncClient = new GraphService.Client(protocol);

            transport.open();

            AuthResponse result = syncClient.authenticate(username, password);

            if(result.getError_code() != ErrorCode.SUCCEEDED){
                LOGGER.debug(result.getError_msg());
                return result.getError_code();
            }else {
                sessionId_ = result.getSession_id();
            }
        }catch (TTransportException tte) {

            LOGGER.debug("Connect failed: " + tte.getMessage());
            return ErrorCode.E_FAIL_TO_CONNECT;

        } catch (TException e) {
            if (e instanceof TApplicationException

                    && ((TApplicationException) e).getType() ==

                    TApplicationException.MISSING_RESULT) {

                LOGGER.debug("The sessionId is NULL: " + e.getMessage());

            }else {
                LOGGER.debug("Connect failed: " + e.getMessage());
            }
            return ErrorCode.E_FAIL_TO_CONNECT;
        }

        return ErrorCode.SUCCEEDED;
    }

    @Override
    public void disconnect() {
        if(transport == null || !transport.isOpen()){
            return;
        }

        try {
            syncClient.signout(sessionId_);
        } catch (TException e) {
            LOGGER.debug("Disconnect error: " + e.getMessage());
        }finally {
            transport.close();
        }
    }

    @Override
    public int execute(String stmt) {
        ExecutionResponse executionResponse = null;

        if(transport == null || !transport.isOpen()){
            return ErrorCode.E_DISCONNECTED;
        }

        try {
            executionResponse = syncClient.execute(sessionId_, stmt);
            if(executionResponse.getError_code() != ErrorCode.SUCCEEDED){
                LOGGER.debug("execute error: " + executionResponse.getError_msg());
                return executionResponse.getError_code();
            }

        } catch (TException e) {
            LOGGER.debug("Thrift rpc call failed: " + e.getMessage());
            return ErrorCode.E_RPC_FAILURE;
        }


        return ErrorCode.SUCCEEDED;
    }
}
