package com.africane.oms.TradeExecutionAggregationService.service;

import com.africane.oms.events.EquityOrderExecutedQuantityEvent;
import com.africane.oms.events.TradeExecutionAggregationEvent;
import com.africane.oms.events.TradeExecutionEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class TradeExecutionAggregationService {

    private static final Logger log = LoggerFactory.getLogger(TradeExecutionAggregationService.class);

    private final Serde<Long> longSerde = Serdes.Long();
    private final Serde<UUID> uuidSerde = Serdes.UUID();
    Serde<TradeExecutionEvent> tradeExecutionEventJsonSerde = new JsonSerde<>( TradeExecutionEvent.class );
    Serde<EquityOrderExecutedQuantityEvent> equityOrderExecutedQuantityEventJsonSerde = new JsonSerde<>( EquityOrderExecutedQuantityEvent.class );
    Serde<TradeExecutionAggregationEvent> tradeExecutionAggregationEventJsonSerde = new JsonSerde<>( TradeExecutionAggregationEvent.class );

    private final Duration windowDuration = Duration.ofMillis(200);
    private static final String WINDOWED_QUANTITY_STORE_NAME = "windowed_executed_quantity_store";
    private static final String WINDOWED_QUANTITY_SUPPRESS_NODE_NAME = "windowed_executed_quantity_suppress";

    Produced<UUID, EquityOrderExecutedQuantityEvent> equityOrderExecutedQuantityEventProduced = Produced.with(uuidSerde, equityOrderExecutedQuantityEventJsonSerde);
    Produced<UUID, TradeExecutionAggregationEvent> tradeExecutionAggregationEventProduced = Produced.with(uuidSerde, tradeExecutionAggregationEventJsonSerde);

    private static final String EXECUTIONS_BY_ORDER_ID_REKEY_TOPIC = "executions_by_order_id_rekey";

    @Bean
    public StreamsBuilderFactoryBean streamBuilder() {
        return new StreamsBuilderFactoryBean(streamsConfig());
    }

    public KafkaStreamsConfiguration streamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "TradeExecutionAggregationService");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$UUIDSerde");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonSerde");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/");
        return new KafkaStreamsConfiguration(props);
    }

    //@Bean
    public KStream<UUID, EquityOrderExecutedQuantityEvent> tradeExecutionAggregation(StreamsBuilder builder) {
        final KGroupedStream<UUID, TradeExecutionEvent> tradeExecutionEventKGroupedStream = builder
                .stream("oms.equity.trade.executions", Consumed.with(Serdes.UUID(), tradeExecutionEventJsonSerde))
                .selectKey((key, executionEvent) -> executionEvent.getId())
                .repartition(Repartitioned.<UUID, TradeExecutionEvent>as(EXECUTIONS_BY_ORDER_ID_REKEY_TOPIC)
                        .withKeySerde(uuidSerde)
                        .withValueSerde(tradeExecutionEventJsonSerde))
                .groupByKey();

        /**
         * We aggregate the executions in a time window and also the same time we make sure we suppress
         * so that all records suppressed til window closes.
         * Out incoming records are fixed-size window.
         * Once we aggregate the records we use a materialized view that relies on a windowstore
         * rather than kv store
         */
        KStream<UUID, EquityOrderExecutedQuantityEvent> kStream = tradeExecutionEventKGroupedStream
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowDuration))
                .aggregate(this::initialize,
                        this::aggregateQuantity,
                        materializedAsWindowStore(WINDOWED_QUANTITY_STORE_NAME, uuidSerde, longSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName(WINDOWED_QUANTITY_SUPPRESS_NODE_NAME))
                .toStream()
                .map((key, aggregatedQuantity)-> KeyValue.pair(key.key(), new EquityOrderExecutedQuantityEvent(key.key(), aggregatedQuantity)));


        kStream.peek((k, o) -> log.info("Aggregated Execution: {} {}", k, o)).to("oms.equity.order.aggregated-executed-quantity", equityOrderExecutedQuantityEventProduced);

        return kStream;
    }

    private Long initialize() {
        return 0L;
    }
    private Long aggregateQuantity(UUID key, TradeExecutionEvent executionEvent, Long aggregatedExecutedQuantity) {
        return aggregatedExecutedQuantity + executionEvent.getExecutedQuantity();
    }

    private <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materializedAsWindowStore(
            String storeName,
            Serde<K> keySerde,
            Serde<V> valueSerde
    ) {
        return Materialized.<K, V>as(Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }


    @Bean
    public KStream<UUID, TradeExecutionAggregationEvent> aggregateExecutions(StreamsBuilder builder) {
        final KGroupedStream<UUID, TradeExecutionEvent> tradeExecutionEventKGroupedStream = builder.stream("oms.equity.trade.executions", Consumed.with(Serdes.UUID(), tradeExecutionEventJsonSerde)).groupByKey();

        KStream<UUID, TradeExecutionAggregationEvent> tradeExecutionAggregationEventKStream =
                tradeExecutionEventKGroupedStream.cogroup(AGGREGATOR)
                        .aggregate(TradeExecutionAggregationEvent::new,
                                Materialized.with(Serdes.UUID(), tradeExecutionAggregationEventJsonSerde)
                        )
                        .toStream();
                        // Finally we check that our order is completed with validations before we send it to the output topic.
                        //.filter((key, order) -> order.isComplete(NUMBER_OF_RULES));

        tradeExecutionAggregationEventKStream.peek((k, o) -> log.info("Aggregated Executions: {} {}", k, o)).to("oms.equity.order.aggregated-executed-quantity", tradeExecutionAggregationEventProduced);

        return tradeExecutionAggregationEventKStream;
    }

    private static final Aggregator<UUID, TradeExecutionEvent, TradeExecutionAggregationEvent> AGGREGATOR = (key, value, aggregate) -> {
        aggregate.getTradeExecutionEvents().add(value);
        aggregate.setId(key);
        return aggregate;
    };
}
