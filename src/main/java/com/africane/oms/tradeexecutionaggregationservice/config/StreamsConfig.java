package com.africane.oms.tradeexecutionaggregationservice.config;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class StreamsConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamsConfig.class);

    @Bean
    @Primary
    public StreamsBuilderFactoryBeanCustomizer streamsBuilderFactoryBeanCustomizer() {
        return factoryBean -> {
            factoryBean.setStreamsUncaughtExceptionHandler(getStreamsUncaughtExceptionHandler());
//            factoryBean.setKafkaStreamsCustomizer(kafkaStreams ->
//                    kafkaStreams.setUncaughtExceptionHandler(getStreamsUncaughtExceptionHandler()));
        };
    }

    private StreamsUncaughtExceptionHandler getStreamsUncaughtExceptionHandler() {
        return exception -> {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception) {
                log.error(cause.getMessage());
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            }
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        };
    }
}
