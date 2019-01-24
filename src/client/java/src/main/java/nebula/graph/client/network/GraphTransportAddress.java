/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package nebula.graph.client.network;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class GraphTransportAddress {

    private final InetSocketAddress address;

    public GraphTransportAddress(InetAddress address, int port) {
        this(new InetSocketAddress(address, port));
    }

    public GraphTransportAddress(InetSocketAddress address) {
        if (address == null) {
            throw new IllegalArgumentException("InetSocketAddress must not be null");
        }
        if (address.getAddress() == null) {
            throw new IllegalArgumentException(
                    "Address must be resolved but returned null"
            );
        }
        this.address = address;
    }

    public String getAddress() {
        return GraphNetworkAddress.format(address.getAddress());
    }

    public String getHostName() {
        return address.getHostName();
    }

    @Override
    public String toString() {
        return address.toString();
    }

    public int getPort() {
        return address.getPort();
    }

}
