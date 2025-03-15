FROM ubuntu:plucky
EXPOSE 8080
COPY target/TradeExecutionAggregationService app
ENTRYPOINT ["/app"]
