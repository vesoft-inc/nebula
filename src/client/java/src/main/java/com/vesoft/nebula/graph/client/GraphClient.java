/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.graph.client;

import com.facebook.thrift.TException;

public interface GraphClient {

    public static final int DEFAULT_TIMEOUT_MS = 1000;
    public static final int DEFAULT_CONNECTION_RETRY_SIZE = 3;
    public static final int DEFAULT_EXECUTION_RETRY_SIZE = 3;

    public int connect(String username, String password);

    public void disconnect();

    public int execute(String stmt);

    public int executeUpdate(String stmt);

    public ResultSet executeQuery(String stmt)
            throws ConnectionException, NGQLException, TException;
}
