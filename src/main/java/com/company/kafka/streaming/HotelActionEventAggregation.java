package com.company.kafka.streaming;

import com.company.kafka.config.ConfigCreator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class HotelActionEventAggregation {

    private static final Logger LOGGER = LoggerFactory.getLogger("HotelActionEventAggregation");

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> kStream = builder.stream("data");
        KStream<String, Long> kStreamFormatted = kStream.selectKey((key, value) -> value).map((key, value) ->
                new KeyValue<>(key.replace(",", "-"), 1L));

        //group stream and operate windowed aggregation
        KTable<Windowed<String>, Long> timeWindowedAggregatedStream = kStreamFormatted
                .groupByKey(Grouped.<String, Long>as(null)
                        .withValueSerde(Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(30)))
                .aggregate(
                        () -> 0L, /* initializer */
                        (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
                        Materialized
                                .<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
                                .withValueSerde(Serdes.Long()));

        timeWindowedAggregatedStream.toStream().to("windowedAggregation-output-topic",
                Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class),
                        Serdes.Long()));

        Topology topology = builder.build();
        System.out.println(topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, ConfigCreator.getConfig());

        //gracefully shutdown
        CountDownLatch countDownLatch = new CountDownLatch(1);

        try {
            kafkaStreams.start();
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Error occured when countdownlatch await", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                countDownLatch.countDown();
            }
        });
    }
}
