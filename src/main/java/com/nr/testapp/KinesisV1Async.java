package com.nr.testapp;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.newrelic.api.agent.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.nr.testapp.Config.REGION;
import static com.nr.testapp.Config.STREAM_NAME;
import static com.nr.testapp.Config.V1_CREDENTIALS_PROVIDER;

public class KinesisV1Async {

    public static final Logger log = LoggerFactory.getLogger(KinesisV1Async.class);

    @Trace(dispatcher = true)
    public static void runKinesisAsync() {
        AmazonKinesisAsync kinesisClient = AmazonKinesisAsyncClientBuilder.standard()
                .withCredentials(V1_CREDENTIALS_PROVIDER)
                .withRegion(REGION)
                .build();
        try {
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


    public static void createStream(AmazonKinesisAsync kinesisClient, String streamName) throws ExecutionException, InterruptedException {
        final int shardCount = 1;
        CreateStreamRequest streamReq = new CreateStreamRequest();
        streamReq.setStreamName(streamName);
        streamReq.setShardCount(shardCount);

        try {
            kinesisClient.createStreamAsync(streamReq,
                    new CustomAsyncHandler<>("Created stream {} with shard {}", streamName, shardCount)).get();
        } catch (Exception e) {
            log.error("Exception creating stream {}", streamName, e);
        }

    }

    public static void deleteStream(AmazonKinesisAsync kinesisClient, String streamName) throws ExecutionException, InterruptedException {
        DeleteStreamRequest delStream = new DeleteStreamRequest();
        delStream.setStreamName(streamName);
        try {
            kinesisClient.deleteStreamAsync(delStream, new CustomAsyncHandler<>("Deleted stream {}", streamName)).get();
        } catch (Exception e) {
            log.error("Exception deleting stream {}", streamName, e);
        }
    }

    public static void getStockTrades(AmazonKinesisAsync kinesisClient, String streamName) throws ExecutionException, InterruptedException {
        String shardIterator;
        String lastShardId = null;
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);

        List<Shard> shards = new ArrayList<>();
        DescribeStreamResult streamRes;
        do {
            streamRes = kinesisClient.describeStreamAsync(describeStreamRequest, new CustomAsyncHandler<>("Described stream {}", streamName)).get();
            shards.addAll(streamRes.getStreamDescription().getShards());

            if (!shards.isEmpty()) {
                lastShardId = shards.get(shards.size() - 1).getShardId();
            }
        } while (streamRes.getStreamDescription().getHasMoreShards());

        GetShardIteratorRequest itReq = new GetShardIteratorRequest();
        itReq.setStreamName(streamName);
        itReq.setShardIteratorType("TRIM_HORIZON");
        itReq.setShardId(lastShardId);

        GetShardIteratorResult shardIteratorResult = kinesisClient.getShardIterator(itReq);
        shardIterator = shardIteratorResult.getShardIterator();

        // Continuously read data records from shard.
        List<com.amazonaws.services.kinesis.model.Record> records;

        // Create new GetRecordsRequest with existing shardIterator.
        // Set maximum records to return to 1000.
        GetRecordsRequest recordsRequest = new GetRecordsRequest();
        recordsRequest.setShardIterator(shardIterator);
        recordsRequest.setLimit(1000);

        GetRecordsResult result = kinesisClient.getRecordsAsync(recordsRequest, new CustomAsyncHandler<>("Retrieved records")).get();

        // Put result into record list. Result may be empty.
        records = result.getRecords();
        log.info("Getting records");

        // Print records
        for (com.amazonaws.services.kinesis.model.Record record : records) {
            ByteBuffer byteBuffer = record.getData();
            log.info("Seq No: {} - {}", record.getSequenceNumber(), new String(byteBuffer.array()));
        }
        log.info("Finished getting records");
    }

    public static void setStockData(AmazonKinesisAsync kinesisClient, String streamName) {
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

        } catch (InterruptedException e) {
            log.error(e.getMessage());
            return;
        }
        System.out.println("Done");
    }

    private static void sendStockTrade(StockTrade trade, AmazonKinesisAsync kinesisClient, String streamName) {
        byte[] bytes = trade.toJsonAsBytes();

        // The bytes could be null if there is an issue with the JSON serialization by
        // the Jackson JSON library.
        if (bytes == null) {
            log.warn("Could not get JSON bytes for stock trade");
            return;
        }

        log.info("Putting trade: " + trade);
        PutRecordRequest request = new PutRecordRequest();
        request.setStreamName(streamName);
        request.setPartitionKey(trade.getTickerSymbol());
        request.setData(ByteBuffer.wrap(bytes));
        kinesisClient.putRecordAsync(request, new CustomAsyncHandler<>("Put record {}", trade));
    }

    private static void validateStream(AmazonKinesisAsync kinesisClient, String streamName) throws ExecutionException, InterruptedException {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);

        DescribeStreamResult describeStreamResponse = kinesisClient.describeStreamAsync(describeStreamRequest).get();

        if (!"ACTIVE".equals(describeStreamResponse.getStreamDescription().getStreamStatus())) {
            log.error("Stream " + streamName + " is not active. Please wait a few moments and try again.");
            System.exit(1);
        }
    }

    public static class CustomAsyncHandler<T0 extends AmazonWebServiceRequest, T1> implements com.amazonaws.handlers.AsyncHandler<T0, T1> {
        private final String logMsg;
        private final Object[] args;

        CustomAsyncHandler(String logMsg, Object ...args) {
            this.logMsg = logMsg;
            this.args = args;
        }

        @Override
        public void onError(Exception e) {
            log.error("Error occurred", e);
        }

        @Override
        public void onSuccess(T0 request, T1 t1) {
            log.info(logMsg, args);
        }
    }
}
