package com.africane.oms.tradeexecutionaggregationservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.kafka.streams.aot.KafkaStreamsBinderRuntimeHints;
import org.springframework.context.annotation.ImportRuntimeHints;

@SpringBootApplication
public class TradeExecutionAggregationServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(TradeExecutionAggregationServiceApplication.class, args);
	}

}
