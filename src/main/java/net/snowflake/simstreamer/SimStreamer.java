/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.simstreamer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.apache.commons.cli.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Example on how to use the Streaming Ingest client APIs.
 *
 * <p>Please read the README.md file for detailed steps
 */
public class SimStreamer {
    // Please follow the example in profile_streaming.json.example to see the required properties, or
    // if you have already set up profile.json with Snowpipe before, all you need is to add the "role"
    // property.

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final ObjectMapper mapperS = new ObjectMapper();

    public static <ParseException> void main(String[] args) throws Exception {

        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

        Option connection_parms = new Option("c", "connection_parms", true, "Path to connection.json file");
        options.addOption(connection_parms);

        Option simulation_parms = new Option("s", "simulation_parms", true, "Path to simulation.json file");
        options.addOption(simulation_parms);

        Option usecase_parms = new Option("u", "use-case", true, "Use Cases supported: tpch");
        options.addOption(usecase_parms);

        // define parser
        CommandLine cmd;
        CommandLineParser parser = new DefaultParser();
        HelpFormatter helper = new HelpFormatter();

        String connection_path = "./connection.json";
        String simulation_path = "./simulation.json";
        String usecase = "";

        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("c")) {
                connection_path = cmd.getOptionValue(connection_parms);
            }

            if (cmd.hasOption("s")) {
                simulation_path = cmd.getOptionValue(simulation_parms);
            }

            if (cmd.hasOption("u")) {
                usecase = cmd.getOptionValue(usecase_parms);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            helper.printHelp("Usage:", options);
            System.exit(0);
        }
        if (!usecase.isBlank()) {

            Properties props = new Properties();

            Iterator<Map.Entry<String, JsonNode>> propIt =
                    mapper.readTree(new String(Files.readAllBytes(Paths.get(connection_path)))).fields();
            while (propIt.hasNext()) {
                Map.Entry<String, JsonNode> prop = propIt.next();
                props.put(prop.getKey(), prop.getValue().asText());
            }

            Properties props_sim = new Properties();

            Iterator<Map.Entry<String, JsonNode>> propIt2 =
                    mapperS.readTree(new String(Files.readAllBytes(Paths.get(simulation_path)))).fields();
            while (propIt2.hasNext()) {
                Map.Entry<String, JsonNode> prop = propIt2.next();
                props_sim.put(prop.getKey(), prop.getValue().asText());
            }

            long l_orderkey_start = Long.parseLong(props_sim.getProperty("l_orderkey_start", "60000001"));
            int totalRowsInTable = Integer.parseInt(props_sim.getProperty("totalRowsinTable", "1000"));
            int num_channels = Integer.parseInt(props_sim.getProperty("num_channels", "1"));

            int batchsize = totalRowsInTable / num_channels;

            // Create a streaming ingest client
            // try (
            SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.builder("ICESTREAM").setProperties(props).build();

            int i = 0;

            for (i = 0; i < num_channels; i++) {

                long start_offset = l_orderkey_start + ((long) batchsize * i);
                Thread t = new Thread(new RunnableChannel(client, usecase, start_offset, batchsize, props, props_sim));
                t.start();
            }


            // System.out.println("SUCCESSFULLY inserted " + num_rows_inserted + " rows");

        } else System.out.println("Use case not provided");
    }

}
