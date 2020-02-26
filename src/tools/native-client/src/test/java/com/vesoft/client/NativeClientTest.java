/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.vesoft.client.NativeClient.Pair;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Map;
import org.junit.Test;

public class NativeClientTest {

    @Test
    public void testDecoded() {
        Object[] values = {
            false,
            7,
            3.14F,
            0.618,
            "Hello".getBytes()
        };
        byte[] result = NativeClient.encode(values);

        NativeClient.Pair[] pairs = new NativeClient.Pair[]{
            new NativeClient.Pair("b_field", Boolean.class.getName()),
            new NativeClient.Pair("i_field", Integer.class.getName()),
            new NativeClient.Pair("f_field", Float.class.getName()),
            new NativeClient.Pair("d_field", Double.class.getName()),
            new NativeClient.Pair("s_field", byte[].class.getName())
        };

        Map<String, byte[]> decodedResult = NativeClient.decode(result, pairs);

        byte byteValue = decodedResult.get("b_field")[0];
        boolean boolValue = (byteValue == 0x00) ? false : true;
        assertFalse(boolValue);

        assertEquals(4, decodedResult.get("i_field").length);
        assertEquals(4, decodedResult.get("f_field").length);
        assertEquals(8, decodedResult.get("d_field").length);
        assertEquals(5, decodedResult.get("s_field").length);

        ByteBuffer integerByteBuffer = ByteBuffer.wrap(decodedResult.get("i_field"));
        integerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        IntBuffer intBuffer = integerByteBuffer.asIntBuffer();
        assertEquals(7, intBuffer.get());


        ByteBuffer floatByteBuffer = ByteBuffer.wrap(decodedResult.get("f_field"));
        floatByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        FloatBuffer floatBuffer = floatByteBuffer.asFloatBuffer();
        assertEquals("Float Value ", 3.14F, floatBuffer.get(), 0.0001);

        ByteBuffer doubleByteBuffer = ByteBuffer.wrap(decodedResult.get("d_field"));
        doubleByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        DoubleBuffer doubleBuffer = doubleByteBuffer.asDoubleBuffer();
        assertEquals("Double Value ", 0.618, doubleBuffer.get(), 0.0001);

        ByteBuffer stringByteBuffer = ByteBuffer.wrap(decodedResult.get("s_field"));
        assertEquals("Hello", new String(stringByteBuffer.array()));
    }
}

