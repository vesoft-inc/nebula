/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package nebula.graph.client;

import java.net.InetSocketAddress;
import nebula.graph.client.network.GraphNetworkAddress;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;



public class GraphNetworkAddressTest extends GraphClientBase {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void inetSocketAddressFormatTest1() {

        Assert.assertEquals("127.0.0.11:1123",
                GraphNetworkAddress.format(new InetSocketAddress("127.0.0.011",1123)));
        Assert.assertEquals("127.0.0.11:1123",
                GraphNetworkAddress.format(new InetSocketAddress("127.000.000.011",1123)));
    }

    @Test
    public void inetSocketAddressFormatTest2() {

        expectedEx.expect(java.lang.IllegalArgumentException.class);
        expectedEx.expectMessage("InetSocketAddress must not be null or ip format invalid");

        Assert.assertEquals("256.0.0.11:1123",
                GraphNetworkAddress.format(InetSocketAddress.createUnresolved("256.0.0.011",1123)));
    }
}
