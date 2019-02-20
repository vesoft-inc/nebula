/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package nebula.graph.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import nebula.graph.client.network.GraphTransportAddress;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class GraphTransportAddressTest extends GraphClientBase {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void graphTransportAddressTest1() throws Exception {

        Pair<String, Integer> addr = Pair.of("127.0.0.1", 1123);

        GraphTransportAddress graphTransportAddress =
                new GraphTransportAddress(InetAddress.getByName(addr.getKey()), addr.getValue());

        Assert.assertEquals("127.0.0.1", graphTransportAddress.getAddress());
        Assert.assertEquals("/127.0.0.1:1123", graphTransportAddress.toString());
        Assert.assertEquals(1123, graphTransportAddress.getPort());
        Assert.assertEquals("localhost", graphTransportAddress.getHostName());
    }

    @Test
    public void graphTransportAddressTest2() throws Exception {
        Pair<String, Integer> addr = Pair.of("255.255.255.255", 1123);

        GraphTransportAddress graphTransportAddress =
                new GraphTransportAddress(InetAddress.getByName(addr.getKey()), addr.getValue());

        Assert.assertEquals("255.255.255.255", graphTransportAddress.getAddress());
        Assert.assertEquals("/255.255.255.255:1123", graphTransportAddress.toString());
        Assert.assertEquals(1123, graphTransportAddress.getPort());
        Assert.assertEquals("255.255.255.255", graphTransportAddress.getHostName());
    }

    @Test
    public void graphTransportAddressTest3() throws Exception {
        Pair<String, Integer> addr = Pair.of("255.255.255.256", 1123);

        expectedEx.expect(java.lang.IllegalArgumentException.class);
        expectedEx.expectMessage("Address must be resolved but returned null");

        GraphTransportAddress graphTransportAddress =
                new GraphTransportAddress(InetSocketAddress.createUnresolved(addr.getKey(), addr.getValue()));

    }

    @Test
    public void graphTransportAddressTest4() throws Exception {
        Pair<String, Integer> addr = Pair.of("0:0:0:0:0:0:0:1", 1123);

        GraphTransportAddress graphTransportAddress =
                new GraphTransportAddress(InetAddress.getByName(addr.getKey()), addr.getValue());

        Assert.assertEquals("::1", graphTransportAddress.getAddress());
        Assert.assertEquals("/0:0:0:0:0:0:0:1:1123", graphTransportAddress.toString());
        Assert.assertEquals(1123, graphTransportAddress.getPort());
        Assert.assertEquals("localhost", graphTransportAddress.getHostName());
    }

}
