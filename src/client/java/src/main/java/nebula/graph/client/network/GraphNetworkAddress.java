/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package nebula.graph.client.network;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class GraphNetworkAddress {

    private GraphNetworkAddress() {}

    public static String format(InetSocketAddress address) {
        if (address.getAddress() == null) {
            throw new IllegalArgumentException("InetSocketAddress must not be null or ip format invalid");
        }
        return format(address.getAddress(), address.getPort());
    }

    public static String format(InetAddress address) {
        return format(address, -1);
    }

    static String format(InetAddress address, int port) {
        Objects.requireNonNull(address);

        StringBuilder builder = new StringBuilder();

        if (port != -1 && address instanceof Inet6Address) {
            builder.append(GraphInetAddresses.toUriString(address));
        } else {
            builder.append(GraphInetAddresses.toAddrString(address));
        }

        if (port != -1) {
            builder.append(':');
            builder.append(port);
        }

        return builder.toString();
    }
}
