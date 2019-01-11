/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
package nebula.graph.client;

public interface GraphClientIface {

//    This interface will be inherited by blocking and non-blocking clients..

    public int conn_timeout_ms = 1000;

    public int connect(String username, String password);

    public void disconnect();

    public int execute(String stmt);
}
