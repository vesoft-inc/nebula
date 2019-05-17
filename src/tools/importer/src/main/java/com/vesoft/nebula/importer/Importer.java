/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.importer;

import static java.lang.Integer.valueOf;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import com.vesoft.nebula.graph.client.GraphClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Importer {

    private Importer() {
    }

    private class MissingOptionException extends RuntimeException {
        public MissingOptionException(String message) {
            super(message);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Importer.class.getName());
    private static final Importer INSTANCE = new Importer();
    private static final String EMPTY = "";

    private static final int DEFAULT_INSERT_BATCH_SIZE = 16;
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 3 * 1000;
    private static final int DEFAULT_CONNECTION_RETRY = 3;

    private String address;
    private int batchSize;
    private String column;
    private String file;
    private int timeout;
    private int retry;
    private boolean statistics = true;
    private String type;
    private String user;
    private String pswd;

    private static final String INSERT_VERTEX_VALUE_TEMPLATE = "(%d: %s)";
    private static final String INSERT_EDGE_VALUE_TEMPLATE = "(%d->%d: %s)";
    private static final String BATCH_INSERT_TEMPLATE = "INSERT %s %s(%s) values%s";

    private Options buildOptions() {
        Options options = new Options();
        options.addOption(new Option("h", "help", false, "help message"));
        options.addOption(new Option("f", "file", true, "data file path."));
        options.addOption(new Option("b", "batch", true, "batch insert size."));
        options.addOption(new Option("t", "type", true, "data type. vertex | edge"));
        options.addOption(new Option("c", "column", true, "vertex and edge column."));
        options.addOption(new Option("s", "stat", false, "print statistics info."));
        options.addOption(new Option("a", "address", false, "thrift service address."));
        options.addOption(new Option("r", "retry", true, "thrift connection retry number."));
        options.addOption(new Option("o", "timeout", true, "thrift connection timeout."));
        options.addOption(new Option("u", "user", true, "thrift service username."));
        options.addOption(new Option("p", "pswd", true, "thrift service password."));
        return options;
    }

    private void parseOptions(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        if (Objects.isNull(args) || args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Importer Options", options);
            System.exit(-1);
        }

        CommandLine commandLine = parser.parse(options, args);

        address = fetchOptionValue(commandLine, 'a', "address", EMPTY);
        batchSize = fetchOptionValue(commandLine, 'b', "batch", DEFAULT_INSERT_BATCH_SIZE);
        column = fetchOptionValue(commandLine, 'c', "column", EMPTY);
        file = fetchOptionValue(commandLine, 'f', "file", EMPTY);
        timeout = fetchOptionValue(commandLine, 'o', "timeout", DEFAULT_CONNECTION_TIMEOUT_MS);
        retry = fetchOptionValue(commandLine, 'r', "retry", DEFAULT_CONNECTION_RETRY);
        user = fetchOptionValue(commandLine, 'u', "user", EMPTY);
        pswd = fetchOptionValue(commandLine, 'p', "pswd", EMPTY);

        if (commandLine.hasOption('s')) {
            statistics = Boolean.valueOf(commandLine.getOptionValue('s', "false"));
        } else if (commandLine.hasOption("stat")) {
            statistics = Boolean.valueOf(commandLine.getOptionValue("stat", "false"));
        }
        type = fetchOptionValue(commandLine, 's', "stat", EMPTY);
    }

    private String fetchOptionValue(CommandLine cmd, Character opt, String longOpt, String defaultValue) {
        if (cmd.hasOption(opt)) {
            return cmd.getOptionValue(opt);
        } else if (cmd.hasOption(longOpt)) {
            return cmd.getOptionValue(longOpt);
        } else if (!Strings.isNullOrEmpty(defaultValue)) {
            return defaultValue;
        } else {
            throw new MissingOptionException(opt + " or " + longOpt + " should be setting");
        }
    }

    private int fetchOptionValue(CommandLine cmd, Character opt, String longOpt, int defaultValue) {
        if (cmd.hasOption(opt)) {
            return valueOf(cmd.getOptionValue(opt));
        } else if (cmd.hasOption(longOpt)) {
            return valueOf(cmd.getOptionValue(longOpt));
        } else if (defaultValue != 0) {
            return defaultValue;
        } else {
            throw new MissingOptionException(opt + " or " + longOpt + " should be setting");
        }
    }

    private Stream<String> readContent(String file) throws IOException {
        if (file.toLowerCase().startsWith("hdfs://")) {
            return dfsProcessor(file);
        } else {
            return localProcessor(file);
        }
    }

    private void run() {
        long rowCounter = 0;
        long startTime = System.currentTimeMillis();
        try (Stream<String> lines = readContent(file)) {
            lines.filter(line -> line.trim().length() == 0);

            if (statistics) {
                rowCounter = lines.count();
            }

            Map<String, Integer> hosts = StreamSupport.stream(
                    Splitter.on(":").split(address).spliterator(), false)
                    .map(token -> token.split(","))
                    .filter(pair -> pair.length == 2)
                    .collect(Collectors.toMap(pair -> pair[0], pair -> valueOf(pair[1])));
            LOGGER.info("Hosts : {}", hosts);

            GraphClient client = new GraphClient(hosts, timeout, retry);
            int code = client.connect(user, pswd);

            if (code == 0) {
                LOGGER.debug("{} connect to thrift service", user);
            } else {
                LOGGER.error("Connection or Authenticate Failed");
                return;
            }

            try {
                Iterator<List<String>> portion = Iterables.partition(() -> lines.iterator(), batchSize).iterator();
                while (portion.hasNext()) {
                    List<String> segment = portion.next();
                    LOGGER.debug("partly tokens {}", segment.toString());

                    List<String> values = new ArrayList<>(segment.size());
                    for (String line : segment) {
                        Iterable<String> tokens = Splitter.on(",").split(line);

                        if (Iterables.isEmpty(tokens)
                                || (type.equals("vertex") && Iterables.size(tokens) < 2)
                                || (type.equals("edge") && Iterables.size(tokens) < 4)) {
                            LOGGER.warn("Skip : {}", line);
                            continue;
                        }

                        List<String> tokenList = StreamSupport
                                .stream(tokens.spliterator(), false)
                                .map(String::toString)
                                .collect(Collectors.toList());

                        switch (type.toLowerCase()) {
                          case "vertex":
                              long id = Long.parseLong(tokenList.get(0));
                              String vertexValue = Joiner.on(", ").join(tokenList.subList(1, tokenList.size()));
                              LOGGER.debug("vertex id {}, value {}", id, vertexValue);
                              values.add(String.format(INSERT_VERTEX_VALUE_TEMPLATE, id, vertexValue));
                              break;
                          case "edge":
                              long source = Long.parseLong(tokenList.get(0));
                              long target = Long.parseLong(tokenList.get(1));
                              String edgeValue = Joiner.on(", ").join(tokenList.subList(2, tokenList.size()));
                              LOGGER.debug("edge source {}, target {}, value : {}", source, target, edgeValue);
                              values.add(String.format(INSERT_EDGE_VALUE_TEMPLATE, source, target, edgeValue));
                              break;
                          default:
                              LOGGER.error("type should be vertex | edge");
                              break;
                        }
                    }

                    String exec = String.format(BATCH_INSERT_TEMPLATE, type, column, Joiner.on(", ").join(values));
                    LOGGER.debug("execute {}", exec);
                    int result = client.execute(exec);
                    if (result != 0) {
                        LOGGER.error("Graph Client Execution Failed !");
                    } else {
                        LOGGER.error("Graph Client Execution SUCCEED !");
                    }
                }
            } finally {
                client.disconnect();
            }
        } catch (IOException e) {
            LOGGER.error("IOException {}", e.getMessage());
        }

        long interval = System.currentTimeMillis() - startTime;
        if (statistics) {
            LOGGER.info("Row Counter   : ", rowCounter);
            LOGGER.info("Time Interval : ", interval);
        }
    }

    private Stream<String> localProcessor(String file) throws IOException {
        Path filePath = Paths.get(file);
        if (!Files.exists(filePath) || !Files.isReadable(filePath)) {
            throw new IllegalArgumentException(file + " is not exist or not readable");
        }
        LOGGER.info("Reading Local FileSystem : {}", file);
        return Files.lines(filePath, Charsets.UTF_8);
    }

    private Stream<String> dfsProcessor(String filePath /*Map<String, String> config*/) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(filePath), configuration);
        LOGGER.info("Reading HDFS : {}", file);
        try (InputStream stream = fileSystem.open(new org.apache.hadoop.fs.Path(file));
             Reader reader = new InputStreamReader(stream);
             BufferedReader buffered = new BufferedReader(reader)) {
            return buffered.lines();
        }
    }

    public static void main(String[] args) {
        Options options = INSTANCE.buildOptions();
        try {
            INSTANCE.parseOptions(options, args);
        } catch (ParseException e) {
            LOGGER.error("Command Line Parse Failed : {}", e.getMessage());
        }
        INSTANCE.run();
    }
}

