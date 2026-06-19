package com.africane.oms.tradeexecutionaggregationservice.service;

import com.africane.oms.events.*;
import com.africane.oms.tradeexecutionaggregationservice.serde.OmsJsonSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class TradeExecutionAggregationService {

    private static final Logger log = LoggerFactory.getLogger(TradeExecutionAggregationService.class);
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;
    @Value("${spring.kafka.streams.state-dir}")
    private String streamStateDirectory;
    @Value("${streamGroupInstance}")
    private String streamGroupInstance;
    private final Serde<Long> longSerde = Serdes.Long();
    private final Serde<UUID> uuidSerde = Serdes.UUID();
    Serde<TradeExecutionWithFeesEvent> tradeExecutionWithFeesEventJsonSerde = new OmsJsonSerde<>(TradeExecutionWithFeesEvent.class);
    Serde<EquityOrderExecutedQuantityEvent> equityOrderExecutedQuantityEventJsonSerde = new OmsJsonSerde<>(EquityOrderExecutedQuantityEvent.class);
    Serde<TradeExecutionAggregationEvent> tradeExecutionAggregationEventJsonSerde = new OmsJsonSerde<>(TradeExecutionAggregationEvent.class);

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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$UUIDSerde");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$ByteArraySerde");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, streamStateDirectory);
        props.put("topology.optimization", "all");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG), streamGroupInstance);

        return new KafkaStreamsConfiguration(props);
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
        final KGroupedStream<UUID, TradeExecutionWithFeesEvent> tradeExecutionEventKGroupedStream = builder.stream("oms.equity.trade.executionsfees", Consumed.with(Serdes.UUID(), tradeExecutionWithFeesEventJsonSerde)).groupByKey();

        KStream<UUID, TradeExecutionAggregationEvent> tradeExecutionAggregationEventKStream =
                tradeExecutionEventKGroupedStream.cogroup(AGGREGATOR)
                        .aggregate(TradeExecutionAggregationEvent::new,
                                Materialized.with(Serdes.UUID(), tradeExecutionAggregationEventJsonSerde)
                        )
                        .toStream();

        tradeExecutionAggregationEventKStream.peek((k, o) -> log.info("Aggregated Executions: {} {}", k, o)).to("oms.equity.order.aggregated-executed-quantity", tradeExecutionAggregationEventProduced);

        return tradeExecutionAggregationEventKStream;
    }

    private static final Aggregator<UUID, TradeExecutionWithFeesEvent, TradeExecutionAggregationEvent> AGGREGATOR = (key, value, aggregate) -> {
        aggregate.getTradeExecutionEvents().add(getTradeExecutionEvent(value));
        aggregate.setId(key);
        return aggregate;
    };

    private static TradeExecutionEvent getTradeExecutionEvent(TradeExecutionWithFeesEvent tradeExecutionWithFeesEvent) {
        TradeExecutionEvent newTradeExecutionEvent = new TradeExecutionEvent();
        newTradeExecutionEvent.setTradeType(tradeExecutionWithFeesEvent.getTradeType());
        newTradeExecutionEvent.setExecutedPrice(tradeExecutionWithFeesEvent.getExecutedPrice());
        newTradeExecutionEvent.setExecutedQuantity(tradeExecutionWithFeesEvent.getExecutedQuantity());
        newTradeExecutionEvent.setExecutionDate(tradeExecutionWithFeesEvent.getExecutionDate());
        newTradeExecutionEvent.setTradeId(tradeExecutionWithFeesEvent.getTradeId());
        newTradeExecutionEvent.setCounterPartyTradingNo(tradeExecutionWithFeesEvent.getCounterPartyTradingNo());
        newTradeExecutionEvent.setCustomerId(tradeExecutionWithFeesEvent.getCustomerId());
        newTradeExecutionEvent.setMarketCenterOrderId(tradeExecutionWithFeesEvent.getMarketCenterOrderId());
        newTradeExecutionEvent.setSide(tradeExecutionWithFeesEvent.getSide());
        newTradeExecutionEvent.setId(tradeExecutionWithFeesEvent.getId());
        newTradeExecutionEvent.setTicker(tradeExecutionWithFeesEvent.getTicker());
        newTradeExecutionEvent.setStatus(tradeExecutionWithFeesEvent.getStatus());
        newTradeExecutionEvent.setTradeDate(tradeExecutionWithFeesEvent.getTradeDate());
        newTradeExecutionEvent.setTradingNo(tradeExecutionWithFeesEvent.getTradingNo());
        newTradeExecutionEvent.setTransactionDescription(tradeExecutionWithFeesEvent.getTransactionDescription());
        return newTradeExecutionEvent;
    }
}
