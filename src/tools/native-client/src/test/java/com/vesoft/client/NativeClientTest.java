/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

package com.vesoft.client;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NativeClientTest {
    @Test
    public void testEncoded() {
        Object[] values = {
          false,
          7,
          1024L,
          3.14F,
          0.618,
          "Hello".getBytes(),
        };
        byte[] result = NativeClient.encoded(values);
        assertEquals(20, result.length);
    }
}

