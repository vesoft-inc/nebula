/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.importer;

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
import org.apache.log4j.Logger;

public class Importer {

    private Importer() {
    }

    private class MissingOptionException extends RuntimeException {
        public MissingOptionException(String message) {
            super(message);
        }
    }

    private static final Logger LOGGER = Logger.getLogger(Importer.class.getClass());
    private static final Importer INSTANCE = new Importer();
    private static final String EMPTY = "";

    private static final int DEFAULT_INSERT_BATCH_SIZE = 16;
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 3 * 1000;
    private static final int DEFAULT_CONNECTION_RETRY = 3;

    private String address;
    private int batchSize;
    private String spaceName;
    private String schemaName;
    private String columns;
    private String file;
    private int timeout;
    private int retry;
    private boolean statistics;
    private String type;
    private String user;
    private String pswd;
    private boolean hasRanking;

    private static final String USE_TEMPLATE = "USE %s";
    private static final String INSERT_VERTEX_VALUE_TEMPLATE = "%d: (%s)";
    private static final String INSERT_EDGE_VALUE_WITHOUT_RANKING_TEMPLATE = "%d->%d: (%s)";
    private static final String INSERT_EDGE_VALUE_TEMPLATE = "%d->%d@%d: (%s)";
    private static final String BATCH_INSERT_TEMPLATE = "INSERT %s %s(%s) values %s";

    private Options buildOptions() {
        Options options = new Options();
        options.addOption(new Option("h", "help", false, "help message"));
        options.addOption(new Option("f", "file", true, "data file path."));
        options.addOption(new Option("b", "batch", true, "batch insert size."));
        options.addOption(new Option("t", "type", true, "data type. vertex or edge."));
        options.addOption(new Option("n", "name", true, "specify the space name."));
        options.addOption(new Option("m", "schema", true, "specify the schema name."));
        options.addOption(new Option("c", "column", true, "vertex and edge's column."));
        options.addOption(new Option("s", "stat", false, "print statistics info."));
        options.addOption(new Option("a", "address", true, "thrift service address."));
        options.addOption(new Option("r", "retry", true, "thrift connection retry number."));
        options.addOption(new Option("o", "timeout", true, "thrift connection timeout."));
        options.addOption(new Option("u", "user", true, "thrift service username."));
        options.addOption(new Option("p", "pswd", true, "thrift service password."));
        options.addOption(new Option("k", "ranking", false, "the edge have ranking data."));
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
        spaceName = fetchOptionValue(commandLine, 'n', "name", EMPTY);
        schemaName = fetchOptionValue(commandLine, 'm', "schema", EMPTY);
        columns = fetchOptionValue(commandLine, 'c', "column", EMPTY);
        file = fetchOptionValue(commandLine, 'f', "file", EMPTY);
        timeout = fetchOptionValue(commandLine, 'o', "timeout", DEFAULT_CONNECTION_TIMEOUT_MS);
        retry = fetchOptionValue(commandLine, 'r', "retry", DEFAULT_CONNECTION_RETRY);
        user = fetchOptionValue(commandLine, 'u', "user", EMPTY);
        pswd = fetchOptionValue(commandLine, 'p', "pswd", EMPTY);
        type = fetchOptionValue(commandLine, 't', "type", EMPTY);
        statistics = fetchOptionValue(commandLine, 's', "stat", true);
        hasRanking = fetchOptionValue(commandLine, 'k', "ranking", false);
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
            return Integer.valueOf(cmd.getOptionValue(opt));
        } else if (cmd.hasOption(longOpt)) {
            return Integer.valueOf(cmd.getOptionValue(longOpt));
        } else {
            return defaultValue;
        }
    }

    private boolean fetchOptionValue(CommandLine cmd, Character opt, String longOpt, boolean defaultValue) {
        if (cmd.hasOption(opt) || cmd.hasOption(longOpt)) {
            return true;
        } else {
            return defaultValue;
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

            Map<String, Integer> hosts = StreamSupport.stream(
                    Splitter.on(",").split(address).spliterator(), false)
                    .map(token -> token.split(":"))
                    .filter(pair -> pair.length == 2)
                    .collect(Collectors.toMap(pair -> pair[0], pair -> Integer.valueOf(pair[1])));

            GraphClient client = new GraphClient(hosts, timeout, retry);
            int code = client.connect(user, pswd);

            if (code == 0) {
                LOGGER.debug(String.format("%s connect to thrift service", user));
            } else {
                LOGGER.error("Connection or Authenticate Failed");
                return;
            }

            code = client.execute(String.format(USE_TEMPLATE, spaceName));
            if (code == 0) {
                LOGGER.info(String.format("Switch Space to %s", spaceName));
            } else {
                LOGGER.error(String.format("USE %s Failed", spaceName));
                return;
            }

            try {
                Iterator<List<String>> portion = Iterables.partition(() -> lines.iterator(), batchSize).iterator();
                while (portion.hasNext()) {
                    List<String> segment = portion.next();
                    if (statistics) {
                        rowCounter += segment.size();
                    }

                    List<String> values = new ArrayList<>(segment.size());
                    for (String line : segment) {
                        Iterable<String> tokens = Splitter.on(",").split(line);

                        if (Iterables.isEmpty(tokens)
                                || (type.equals("vertex") && Iterables.size(tokens) < 2)
                                || (type.equals("edge") && Iterables.size(tokens) < 3)) {
                            LOGGER.warn(String.format("Skip : %s", line));
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
                              LOGGER.trace(String.format("vertex id: %d, value: %s", id, vertexValue));
                              values.add(String.format(INSERT_VERTEX_VALUE_TEMPLATE, id, vertexValue));
                              break;
                          case "edge":
                              long source = Long.parseLong(tokenList.get(0));
                              long target = Long.parseLong(tokenList.get(1));
                              if (hasRanking) {
                                  long ranking = Long.parseLong(tokenList.get(2));
                                  String edgeValue = Joiner.on(", ").join(tokenList.subList(3, tokenList.size()));
                                  LOGGER.trace(String.format("edge source: %d, target: %d, ranking: %d, value: %s", source, target,
                                                             ranking, edgeValue));
                                  values.add(String.format(INSERT_EDGE_VALUE_TEMPLATE, source, target,ranking, edgeValue));
                              } else {
                                  String edgeValue = Joiner.on(", ").join(tokenList.subList(2, tokenList.size()));
                                  LOGGER.trace(String.format("edge source: %d, target: %d, value: %s", source, target, edgeValue));
                                  values.add(String.format(INSERT_EDGE_VALUE_WITHOUT_RANKING_TEMPLATE, source,
                                                           target, edgeValue));
                              }
                              break;
                          default:
                              LOGGER.error("Type should be vertex or edge");
                              break;
                        }
                    }

                    String exec = String.format(BATCH_INSERT_TEMPLATE, type, schemaName,  columns, Joiner.on(", ").join(values));
                    LOGGER.debug(String.format("Execute: %s", exec));
                    int result = client.execute(exec);
                    if (result != 0) {
                        LOGGER.error("Graph Client Execution Failed !");
                    }
                }
            } finally {
                client.disconnect();
            }
        } catch (IOException e) {
            LOGGER.error("IOException: ", e);
        }

        long interval = System.currentTimeMillis() - startTime;
        if (statistics) {
            LOGGER.info(String.format("Row Counter   : %d", rowCounter));
            LOGGER.info(String.format("Time Interval : %d", interval));
        }
    }

    private Stream<String> localProcessor(String file) throws IOException {
        Path filePath = Paths.get(file);
        if (!Files.exists(filePath) || !Files.isReadable(filePath)) {
            throw new IllegalArgumentException(file + " is not exist or not readable");
        }
        LOGGER.info(String.format("Reading Local FileSystem: %s", file));
        return Files.lines(filePath, Charsets.UTF_8);
    }

    private Stream<String> dfsProcessor(String filePath /*Map<String, String> config*/) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(filePath), configuration);
        LOGGER.info(String.format("Reading HDFS: %s", file));
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
            LOGGER.error("Command Line Parse Failed: ", e);
        }
        INSTANCE.run();
    }
}

