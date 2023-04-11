package net.snowflake.simstreamer;

import com.github.javafaker.Faker;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

//import static net.snowflake.simstreamer.SimStreamer.ProcessChannelRowset;

public class RunnableChannel implements Runnable {

    private final SnowflakeStreamingIngestClient client;
    private final String usecase;
    private final long startkey;
    private final long batchsize;
    private final Properties props_con;
    private final Properties props_sim;

    public RunnableChannel(SnowflakeStreamingIngestClient client, String usecase, long startkeyvalue,
                           long numRowsinBatch, Properties props_connection, Properties props_simulation) {
        /*this.client = client;*/
        this.client = client;
        this.usecase = usecase;
        this.startkey = startkeyvalue;
        this.batchsize = numRowsinBatch;
        this.props_con = props_connection;
        this.props_sim = props_simulation;
    }

    @Override
    public void run() {
        long num_rows_inserted = 0;
        try {
            num_rows_inserted = ProcessChannelRowset(client, startkey, batchsize, props_con, props_sim);
            if (num_rows_inserted < 0) {
                System.out.println("Something went wrong. Check if the use case was valid.");
            } else {
                System.out.println("Number of rows inserted:" + num_rows_inserted);
            }

        } catch (ParseException | ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private long ProcessChannelRowset(SnowflakeStreamingIngestClient client, long startkeyvalue,
                                      long numRowsinBatch, Properties props_connection, Properties props_simulation) throws ParseException, ExecutionException, InterruptedException {

        Faker faker = new Faker();
        String channel_name = faker.space().constellation() + " " + faker.lorem().word();
        int sleep_ms = Integer.parseInt(props_simulation.getProperty("sleep_ms", "0"));

        String tablename = "";

        if (usecase.equals("tpch")) {

            tablename = "LINEITEM";

            OpenChannelRequest openChannelRequest =
                    OpenChannelRequest.builder(channel_name)
                            .setDBName(props_connection.getProperty("database"))
                            .setSchemaName(props_connection.getProperty("schema"))
                            .setTableName(tablename)
                            .setOnErrorOption(
                                    OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
                            .build();

            // Open a streaming ingest channel from the given client
            SnowflakeStreamingIngestChannel channel = client.openChannel(openChannelRequest);

            // Insert rows into the channel (Using insertRows API)
            long l_orderkey = startkeyvalue;
            long val = 0;

            for (val = 0; val < numRowsinBatch + 1; val++) {

                int num_lines = faker.number().randomDigitNotZero();

                for (int line = 1; line < num_lines + 1; line++) {

                    Map<String, Object> row = BuildRowTCPH(l_orderkey, line, props_simulation);
                    InsertValidationResponse response = channel.insertRow(row, String.valueOf(val));

                    if (response.hasErrors()) {
                        // Simply throw if there is an exception, or you can do whatever you want with the
                        // erroneous row
                        throw response.getInsertErrors().get(0).getException();
                    }

                    val++;

                    if (sleep_ms > 0) {
                        try {
                            Thread.sleep(sleep_ms);
                        } catch (InterruptedException e) {
                            System.out.println(e);
                        }
                    }
                }
                val--;
                l_orderkey++;
            }

            // If needed, you can check the offset_token registered in Snowflake to make sure everything
            // is committed
            final long expectedOffsetTokenInSnowflake = numRowsinBatch; // 0 based offset_token
            final int maxRetries = 3;
            int retryCount = 0;

            do {

                String offsetTokenFromSnowflake = channel.getLatestCommittedOffsetToken();
                if (offsetTokenFromSnowflake != null
                        && offsetTokenFromSnowflake.equals(String.valueOf(expectedOffsetTokenInSnowflake))) {

                    return numRowsinBatch;
                }
                retryCount++;
            } while (retryCount < maxRetries);

            // Close the channel, the function internally will make sure everything is committed (or throw
            // an exception if there is any issue)
            channel.close().get();

            return val;
        }

        return -1;
    }


    private static Map<String, Object> BuildRowTCPH(long keyvalue, int line, Properties props) throws ParseException {
        Map<String, Object> row = new HashMap<>();
        Faker rowfaker = new Faker();

        //Get Ranges of data
        long l_orderkey_start = Long.parseLong(props.getProperty("l_orderkey_start", "60000001"));
        long l_partkey_max = Long.parseLong(props.getProperty("l_partkey_max", "200000"));
        long l_suppkey_max = Long.parseLong(props.getProperty("l_suppkey_max", "10000"));

        long l_partkey_min = Long.parseLong(props.getProperty("l_partkey_min", "1"));
        long l_suppkey_min = Long.parseLong(props.getProperty("l_suppkey_min", "1"));

        String l_shipdate_min = props.getProperty("l_shipdate_min", "1992-01-02");
        String l_shipdate_max = props.getProperty("l_shipdate_max", "1998-12-01");

        //Declare required formats
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
        DecimalFormat decfor = new DecimalFormat("0.00");

        //Define column values
        long l_partkey = rowfaker.number().numberBetween(l_partkey_min, l_partkey_max);
        long l_suppkey = rowfaker.number().numberBetween(l_suppkey_min, l_suppkey_max);
        int l_quantity = rowfaker.number().numberBetween(1, 50);
        double l_extendedprice = rowfaker.number().randomDouble(2, 500, 100000);
        double l_discount = rowfaker.number().numberBetween(0, 20);
        double l_tax = rowfaker.number().numberBetween(4, 15);
        String l_returnflag = rowfaker.options().option("R", "N", "A");
        String l_linestatus = rowfaker.options().option("O", "F");
        Date minDate = sdf.parse(l_shipdate_min);
        Date maxDate = sdf.parse(l_shipdate_max);
        Date l_shipdate = rowfaker.date().between(minDate, maxDate);
        Date l_commitdate = rowfaker.date().past(7, TimeUnit.DAYS, l_shipdate);
        Date l_receiptdate = rowfaker.date().future(30, TimeUnit.DAYS, l_shipdate);
        String l_shipinstruct = rowfaker.options().option("COLLECT COD", "NONE", "DELIVER IN PERSON", "TAKE BACK RETURN");
        String l_shipmode = rowfaker.options().option("TRUCK", "AIR", "RAIL", "SHIP", "MAIL", "FOB", "REG AIR");
        String comment = rowfaker.lorem().sentence(10);

        if (comment.length() > 43) {
            comment = comment.substring(0, 43);
        }

        //Build row
        row.put("l_orderkey", keyvalue);
        row.put("l_partkey", l_partkey);
        row.put("l_suppkey", l_suppkey);
        row.put("l_linenumber", line);
        row.put("l_quantity", l_quantity);
        row.put("l_extendedprice", l_extendedprice);
        row.put("l_discount", decfor.format(l_discount / 100));
        row.put("l_tax", decfor.format(l_tax / 100));
        row.put("l_returnflag", l_returnflag);
        row.put("l_linestatus", l_linestatus);
        row.put("l_shipdate", sdf.format(l_shipdate));
        row.put("l_commitdate", sdf.format(l_commitdate));
        row.put("l_receiptdate", sdf.format(l_receiptdate));
        row.put("l_shipinstruct", l_shipinstruct);
        row.put("l_shipmode", l_shipmode);
        row.put("l_comment", comment);

        return row;

    }
}
