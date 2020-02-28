/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeClientResourceLoader {
    private static final String NEBULA_LIB_NAME = "/libnebula_native_client.so";
    private static final Logger LOGGER = LoggerFactory.getLogger(NativeClient.class.getName());

    public static void resourceLoader() {
        InputStream stream = ClassLoader.class.getResourceAsStream(NEBULA_LIB_NAME);
        if (stream == null) {
            throw new RuntimeException(NEBULA_LIB_NAME + " was not found in JAR");
        }

        File file = null;
        try {
            file = File.createTempFile("lib", ".so");
        } catch (IOException e) {
            LOGGER.error("libnebula_native_client Create Failed {}", e.getMessage());
        }

        if (!file.exists()) {
            throw new RuntimeException(file.getAbsolutePath() + " does not exist");
        }
        file.deleteOnExit();

        try {
            Files.copy(stream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOGGER.error("libnebula_native_client Copy Failed {}", e.getMessage());
        }
        System.load(file.getAbsolutePath());
    }
}
