package com.nr.testapp;

import com.newrelic.api.agent.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.List;

import static com.nr.testapp.Config.V2_CREDENTIALS_PROVIDER;
import static com.nr.testapp.Config.REGION;
import static com.nr.testapp.Config.STREAM_NAME;

public class KinesisSync {
    public static final Logger log = LoggerFactory.getLogger(KinesisSync.class);


    @Trace(dispatcher = true)
    public static void runKinesisSync() {
        Region region = Region.of(REGION);
        try(KinesisClient kinesisClient = KinesisClient.builder().credentialsProvider(V2_CREDENTIALS_PROVIDER).region(region).build()) {
//            createStream(kinesisClient, STREAM_NAME);
//            Thread.sleep(5000);
            validateStream(kinesisClient, STREAM_NAME);
            setStockData(kinesisClient, STREAM_NAME);
            getStockTrades(kinesisClient, STREAM_NAME);
//            deleteStream(kinesisClient, STREAM_NAME);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public static void createStream(KinesisClient kinesisClient, String streamName) {
        final int shardCount = 1;
        try {
            CreateStreamRequest streamReq = CreateStreamRequest.builder()
                    .streamName(streamName)
                    .shardCount(shardCount)
                    .build();

            kinesisClient.createStream(streamReq);
            log.info("Created stream {} with shard {}", streamName, shardCount);
        } catch (Exception e) {
            log.error("Failed to create stream {} with shard {}", streamName, shardCount, e);
        }
    }

    public static void deleteStream(KinesisClient kinesisClient, String streamName) {
        try {
            DeleteStreamRequest delStream = DeleteStreamRequest.builder()
                    .streamName(streamName)
                    .build();
            kinesisClient.deleteStream(delStream);

        } catch (KinesisException e) {
            log.error("Failed to create stream {}", streamName, e);
            throw e;
        }
    }

    public static void getStockTrades(KinesisClient kinesisClient, String streamName) {
        String shardIterator;
        String lastShardId = null;
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamName)
                .build();

        List<Shard> shards = new ArrayList<>();
        DescribeStreamResponse streamRes;
        do {
            streamRes = kinesisClient.describeStream(describeStreamRequest);
            shards.addAll(streamRes.streamDescription().shards());

            if (shards.size() > 0) {
                lastShardId = shards.get(shards.size() - 1).shardId();
            }
        } while (streamRes.streamDescription().hasMoreShards());

        GetShardIteratorRequest itReq = GetShardIteratorRequest.builder()
                .streamName(streamName)
                .shardIteratorType("TRIM_HORIZON")
                .shardId(lastShardId)
                .build();

        GetShardIteratorResponse shardIteratorResult = kinesisClient.getShardIterator(itReq);
        shardIterator = shardIteratorResult.shardIterator();

        // Continuously read data records from shard.
        List<Record> records;

        // Create new GetRecordsRequest with existing shardIterator.
        // Set maximum records to return to 1000.
        GetRecordsRequest recordsRequest = GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(1000)
                .build();

        GetRecordsResponse result = kinesisClient.getRecords(recordsRequest);

        // Put result into record list. Result may be empty.
        records = result.records();

        // Print records
        for (Record record : records) {
            SdkBytes byteBuffer = record.data();
            log.info("Seq No: {} - {}", record.sequenceNumber(), new String(byteBuffer.asByteArray()));
        }
    }

    public static void setStockData(KinesisClient kinesisClient, String streamName) {
        try {
            // Repeatedly send stock trades with a 100 milliseconds wait in between.
            StockTradeGenerator stockTradeGenerator = new StockTradeGenerator();

            // Put in 50 Records for this example.
            int index = 50;
            for (int x = 0; x < index; x++) {
                StockTrade trade = stockTradeGenerator.getRandomTrade();
                sendStockTrade(trade, kinesisClient, streamName);
                Thread.sleep(100);
            }

        } catch (KinesisException | InterruptedException e) {
            log.error(e.getMessage());
            return;
        }
        System.out.println("Done");
    }

    private static void sendStockTrade(StockTrade trade, KinesisClient kinesisClient,
            String streamName) {
        byte[] bytes = trade.toJsonAsBytes();

        // The bytes could be null if there is an issue with the JSON serialization by
        // the Jackson JSON library.
        if (bytes == null) {
            log.warn("Could not get JSON bytes for stock trade");
            return;
        }

        log.info("Putting trade: " + trade);
        PutRecordRequest request = PutRecordRequest.builder()
                .partitionKey(trade.getTickerSymbol()) // We use the ticker symbol as the partition key, explained in
                // the Supplemental Information section below.
                .streamName(streamName)
                .data(SdkBytes.fromByteArray(bytes))
                .build();

        try {
            kinesisClient.putRecord(request);
        } catch (KinesisException e) {
            log.error(e.getMessage());
        }
    }

    private static void validateStream(KinesisClient kinesisClient, String streamName) {
        try {
            DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                    .streamName(streamName)
                    .build();

            DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

            if (!describeStreamResponse.streamDescription().streamStatus().toString().equals("ACTIVE")) {
                log.error("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }

        } catch (KinesisException e) {
            log.error("Error found while describing the stream " + streamName, e);
            throw e;
        }
    }
}