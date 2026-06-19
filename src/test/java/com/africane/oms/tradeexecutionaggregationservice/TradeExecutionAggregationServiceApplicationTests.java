package com.africane.oms.tradeexecutionaggregationservice;

import com.africane.oms.tradeexecutionaggregationservice.service.TradeExecutionAggregationService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class TradeExecutionAggregationServiceApplicationTests {

    // Mock the Kafka Streams service so it doesn't try to connect to a real broker
    @MockitoBean
    TradeExecutionAggregationService tradeExecutionAggregationService;

    @Test
    void contextLoads() {
    }

}
