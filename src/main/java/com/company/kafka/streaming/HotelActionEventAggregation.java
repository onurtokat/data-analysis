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

        //main kstream
        KStream<String, String> kStream = builder.stream("data");

        //click filtered kstream
        KStream<String, Long> clickFormatted = kStream.filter((key, value) -> value.contains("click")).
                selectKey((key, value) -> value).map((key, value) ->
                new KeyValue<>(key.split(",")[0], 1L));

        //view filtered kstream
        KStream<String, Long> viewFormatted = kStream.filter((key, value) -> value.contains("view")).
                selectKey((key, value) -> value).map((key, value) ->
                new KeyValue<>(key.split(",")[0], 1L));

        //group stream and operate windowed aggregation for click
        KTable<Windowed<String>, Long> timeWindowedClick = clickFormatted
                .groupByKey(Grouped.<String, Long>as(null)
                        .withValueSerde(Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(30)))
                .aggregate(
                        () -> 0L, /* initializer */
                        (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
                        Materialized
                                .<String, Long, WindowStore<Bytes, byte[]>>as("timeWindowedClick-aggregated-stream-store")
                                .withValueSerde(Serdes.Long()));

        //group stream and operate windowed aggregation for view
        KTable<Windowed<String>, Long> timeWindowedView = viewFormatted
                .groupByKey(Grouped.<String, Long>as(null)
                        .withValueSerde(Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(30)))
                .aggregate(
                        () -> 0L, /* initializer */
                        (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
                        Materialized
                                .<String, Long, WindowStore<Bytes, byte[]>>as("timeWindowedView-aggregated-stream-store")
                                .withValueSerde(Serdes.Long()));

        //joined ktable
        KTable<Windowed<String>, String> joined = timeWindowedClick.join(timeWindowedView,
                (clickValue, viewValue) -> "click=" + clickValue + ", view=" + viewValue /* ValueJoiner */);

        //to result topic
        joined.toStream().to("windowedAggregation-output-topic",
                Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class),
                        Serdes.String()));

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
