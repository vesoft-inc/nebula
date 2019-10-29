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
import java.util.List;
import java.util.Map;

import org.junit.Assert;
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

    @Test
    public void testGeo() {
        List<Long> result = NativeClient.getS2IndexCells(30.28522, 120.01338, 10, 18);
        List<Long> expect = Arrays.asList(
                3768216563899957248l, 3768216838777864192l,
                3768216632619433984l, 3768216684159041536l,
                3768216671274139648l, 3768216670200397824l,
                3768216669931962368l, 3768216669730635776l, 3768216669680304128l);
        Assert.assertEquals(expect, result);
    }

    @Test
    public void testCreateEdgeKey() {
        int partitionId = 1;
        long srcId = 100;
        int edgeType = 15;
        long edgeRank = 0;
        long dstId = 101l;
        long version = 1;
        byte[] result = NativeClient.createEdgeKey(partitionId, srcId, edgeType, edgeRank, dstId, version);

        byte[] key = {0x01, 0x01, 0x00, 0x00,
                0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x0f, 0x00, 0x00, 0x40,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

        Assert.assertEquals(key.length, result.length);
        for (int i = 0; i < key.length; ++i) {
            Assert.assertEquals(key[i], result[i]);
        }
    }

    @Test
    public void testGeoKey() {
        List<byte[]> keys = NativeClient.createGeoKey(1,
                30.28522, 120.01338, 10, 10,
                1, 0, 100l, 1);

        byte[] expect = NativeClient.createEdgeKey(
                1,3768216563899957248l,1,0,100l,1);

        Assert.assertEquals(1, keys.size());
        byte[] result = keys.get(0);
        for (int i = 0; i < expect.length; ++i) {
            Assert.assertEquals(expect[i], result[i]);
        }
    }
}

