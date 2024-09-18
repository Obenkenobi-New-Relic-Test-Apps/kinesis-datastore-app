package com.nr.testapp;

import com.newrelic.api.agent.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.nr.testapp.Config.CREDENTIALS_PROVIDER;
import static com.nr.testapp.Config.REGION;
import static com.nr.testapp.Config.STREAM_NAME;

public class KinesisAsync {
    public static final Logger log = LoggerFactory.getLogger(KinesisAsync.class);

    @Trace(dispatcher = true)
    public static void runKinesisAsync() {
        Region region = Region.of(REGION);
        try(KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder().credentialsProvider(CREDENTIALS_PROVIDER).region(region).build()) {
//            createStream(kinesisClient, STREAM_NAME).get();
//            Thread.sleep(5000);
            validateStream(kinesisClient, STREAM_NAME).get();
            setStockData(kinesisClient, STREAM_NAME).get();
            getStockTrades(kinesisClient, STREAM_NAME).get();
//            deleteStream(kinesisClient, STREAM_NAME).get();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public static CompletableFuture<Boolean> createStream(KinesisAsyncClient kinesisClient, String streamName) {
        final int shardCount = 1;
        CreateStreamRequest streamReq = CreateStreamRequest.builder()
                .streamName(streamName)
                .shardCount(shardCount)
                .build();

        CompletableFuture<CreateStreamResponse> response = kinesisClient.createStream(streamReq);
        return response.thenApply(res -> {
            log.info("Created stream {} with shard {}", streamName, shardCount);
            log.info("Stream creation res {}", res);
            return true;
        }).exceptionally(e -> {
            log.warn("Failed to create stream {} with shard {}", streamName, shardCount, e);
            return false;
        });
    }

    private static CompletableFuture<Boolean> validateStream(KinesisAsyncClient kinesisClient, String streamName) {
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamName)
                .build();

        CompletableFuture<DescribeStreamResponse> descStreamRes = kinesisClient.describeStream(describeStreamRequest);
        return descStreamRes.thenApply(describeStreamResponse -> {
            boolean isActive = describeStreamResponse.streamDescription().streamStatus().toString().equals("ACTIVE");
            if (!isActive) {
                RuntimeException exception = new RuntimeException("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                log.error(exception.getMessage());
                throw exception;
            }
            return true;
        }).exceptionally(e -> {
            log.error("Error found while describing the stream " + streamName, e);
            throw new RuntimeException(e);
        });

    }

    public static CompletableFuture<Boolean> setStockData(KinesisAsyncClient kinesisClient, String streamName) {
        // Repeatedly send stock trades with a 100 milliseconds wait in between.
        StockTradeGenerator stockTradeGenerator = new StockTradeGenerator();

        // Put in 50 Records for this example.
        int recordCount = 50;
        CompletableFuture<?>[] trades = new CompletableFuture[recordCount];
        for (int x = 0; x < recordCount; x++) {
            StockTrade trade = stockTradeGenerator.getRandomTrade();
            sendStockTrade(trade, kinesisClient, streamName);
            trades[x] = sendStockTrade(trade, kinesisClient, streamName);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        CompletableFuture<Void> completedTrades = CompletableFuture.allOf(trades);
        return completedTrades.thenApply(v -> {
            log.info("Finished trades");
            return true;
        });
    }

    private static CompletableFuture<Boolean> sendStockTrade(StockTrade trade, KinesisAsyncClient kinesisClient,
            String streamName) {
        byte[] bytes = trade.toJsonAsBytes();

        // The bytes could be null if there is an issue with the JSON serialization by
        // the Jackson JSON library.
        if (bytes == null) {
            log.warn("Could not get JSON bytes for stock trade");
            return CompletableFuture.completedFuture(false);
        }

        log.info("Putting trade: " + trade);
        PutRecordRequest request = PutRecordRequest.builder()
                .partitionKey(trade.getTickerSymbol()) // We use the ticker symbol as the partition key, explained in
                // the Supplemental Information section below.
                .streamName(streamName)
                .data(SdkBytes.fromByteArray(bytes))
                .build();

        return kinesisClient.putRecord(request).thenApply(res -> true).exceptionally(e -> {
            log.error(e.getMessage());
            return false;
        });
    }

    public static CompletableFuture<GetShardIteratorResponse> getShardsIterator(KinesisAsyncClient kinesisClient, String streamName, DescribeStreamRequest describeStreamRequest,
            List<Shard> initShards, String initLastShardId) {

        CompletableFuture<DescribeStreamResponse> streamResFuture = kinesisClient.describeStream(describeStreamRequest);
        CompletableFuture<List<Shard>> shardsFuture = streamResFuture.thenApply(streamRes -> {
            List<Shard> newShards = streamRes.streamDescription().shards();
            List<Shard> shardsList = new ArrayList<>(initShards.size() + newShards.size());
            shardsList.addAll(initShards);
            shardsList.addAll(newShards);
            return shardsList;
        });

        CompletableFuture<String> lastShardIdFuture = shardsFuture.thenApply(shardsList -> {
            if (shardsList.size() > 0) {
                return shardsList.get(shardsList.size() - 1).shardId();
            }
            return initLastShardId;
        });

        return lastShardIdFuture.thenCompose(lastShardId -> shardsFuture.thenCompose(shards -> streamResFuture.thenCompose(streamRes -> {
            if (streamRes.streamDescription().hasMoreShards()) {
                return getShardsIterator(kinesisClient, streamName, describeStreamRequest, shards, lastShardId);
            }
            GetShardIteratorRequest itReq = GetShardIteratorRequest.builder()
                    .streamName(streamName)
                    .shardIteratorType("TRIM_HORIZON")
                    .shardId(lastShardId)
                    .build();
            return kinesisClient.getShardIterator(itReq);
        })));
    }

    public static CompletableFuture<Boolean> getStockTrades(KinesisAsyncClient kinesisClient, String streamName) {
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamName)
                .build();
        CompletableFuture<GetShardIteratorResponse> shardsIteratorFuture = getShardsIterator(kinesisClient,
                streamName, describeStreamRequest, new ArrayList<>(), null);

        CompletableFuture<String> shardIteratorFuture = shardsIteratorFuture.thenApply(GetShardIteratorResponse::shardIterator);

        CompletableFuture<GetRecordsResponse> resFuture = shardIteratorFuture.thenCompose(shardIterator -> {
            GetRecordsRequest recordsRequest = GetRecordsRequest.builder()
                    .shardIterator(shardIterator)
                    .limit(1000)
                    .build();
            return kinesisClient.getRecords(recordsRequest);
        });

        return resFuture.thenApply(result -> {
            log.info("Begin reading data");
            List<Record> records = result.records();
            for (Record record : records) {
                SdkBytes byteBuffer = record.data();
                log.info("Seq No: {} - {}", record.sequenceNumber(), new String(byteBuffer.asByteArray()));
            }
            log.info("Finished reading data");
            return true;
        });
    }

    public static CompletableFuture<?> deleteStream(KinesisAsyncClient kinesisClient, String streamName) {
        DeleteStreamRequest delStream = DeleteStreamRequest.builder()
                .streamName(streamName)
                .build();

        return kinesisClient.deleteStream(delStream).thenApply(res -> {
            log.info("Deleted stream {}", streamName);
            return res;
        }).exceptionally(e -> {
            log.error("Failed to create stream {}", streamName, e);
            throw new RuntimeException(e);
        });
    }

}
