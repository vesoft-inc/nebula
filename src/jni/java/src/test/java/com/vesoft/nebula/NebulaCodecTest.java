/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.vesoft.nebula.NebulaCodec.Pair;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class NebulaCodecTest {

    @Test
    public void testDecoded() {
        Object[] values = {
            false,
            7,
            3.14F,
            0.618,
            "Hello".getBytes()
        };
        byte[] result = NebulaCodec.encode(values);

        NebulaCodec.Pair[] pairs = new NebulaCodec.Pair[]{
            new NebulaCodec.Pair("b_field", Boolean.class.getName()),
            new NebulaCodec.Pair("i_field", Integer.class.getName()),
            new NebulaCodec.Pair("f_field", Float.class.getName()),
            new NebulaCodec.Pair("d_field", Double.class.getName()),
            new NebulaCodec.Pair("s_field", byte[].class.getName())
        };

        List<byte[]> decodedResult = NebulaCodec.decode(result, pairs, 0);

        byte[] byteValue = decodedResult.get(0);
        boolean boolValue = (byteValue[0] == 0x00) ? false : true;
        assertFalse(boolValue);

        ByteBuffer integerByteBuffer = ByteBuffer.wrap(decodedResult.get(1));
        integerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        IntBuffer intBuffer = integerByteBuffer.asIntBuffer();
        assertEquals(7, intBuffer.get());


        ByteBuffer floatByteBuffer = ByteBuffer.wrap(decodedResult.get(2));
        floatByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        FloatBuffer floatBuffer = floatByteBuffer.asFloatBuffer();
        assertEquals("Float Value ", 3.14F, floatBuffer.get(), 0.0001);

        ByteBuffer doubleByteBuffer = ByteBuffer.wrap(decodedResult.get(3));
        doubleByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        DoubleBuffer doubleBuffer = doubleByteBuffer.asDoubleBuffer();
        assertEquals("Double Value ", 0.618, doubleBuffer.get(), 0.0001);

        ByteBuffer stringByteBuffer = ByteBuffer.wrap(decodedResult.get(4));
        assertEquals("Hello", new String(stringByteBuffer.array()));
    }
}

