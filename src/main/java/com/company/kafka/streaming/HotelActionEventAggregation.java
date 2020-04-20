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

/**
 * HotelActionEventAggregation class calculate click and view events in a windowed frame
 * @author Onur Tokat
 */
public class HotelActionEventAggregation {

    private static final Logger LOGGER = LoggerFactory.getLogger("HotelActionEventAggregation");

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        //main kstream
        KStream<String, String> kStream = builder.stream("data");

        //click filtered kstream
        KStream<String, Long> clickFormatted = filteredKStream(kStream, "click");

        //view filtered kstream
        KStream<String, Long> viewFormatted = filteredKStream(kStream, "view");

        //group stream and operate windowed aggregation for click
        KTable<Windowed<String>, Long> timeWindowedClick = timeWindowedKTable(clickFormatted, "click");

        //group stream and operate windowed aggregation for view
        KTable<Windowed<String>, Long> timeWindowedView = timeWindowedKTable(viewFormatted, "view");

        //joined ktable
        KTable<Windowed<String>, String> joined = timeWindowedClick.join(timeWindowedView,
                (clickValue, viewValue) -> "click=" + clickValue + ", view=" + viewValue /* ValueJoiner */);

        //to result topic
        joined.toStream().to("windowedAggregation-output-topic",
                Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class),
                        Serdes.String()));

        Topology topology = builder.build();
        System.out.println(topology.describe());//for debugging
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

    /**
     * filteredKStream method provides filtered stream for click or view events
     * @param kStream main stream
     * @param filterName click or view
     * @return filtered KStream
     */
    public static KStream<String, Long> filteredKStream(KStream<String, String> kStream, String filterName) {
        return kStream.filter((key, value) -> value.contains(filterName)).
                selectKey((key, value) -> value).map((key, value) ->
                new KeyValue<>(key.split(",")[0], 1L));
    }

    /**
     * timeWindowedKTable provided windowed aggregated click or view events
     * @param kStream
     * @param filterName click or view
     * @return windowed and aggregated KTable
     */
    public static KTable<Windowed<String>, Long> timeWindowedKTable(KStream<String, Long> kStream, String filterName) {
        return kStream
                .groupByKey(Grouped.<String, Long>as(null)
                        .withValueSerde(Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(30)))
                .aggregate(
                        () -> 0L, /* initializer */
                        (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
                        Materialized
                                .<String, Long, WindowStore<Bytes, byte[]>>as(filterName + "-aggregated-stream-store")
                                .withValueSerde(Serdes.Long()));
    }
}
