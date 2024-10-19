FROM gcr.io/distroless/base
EXPOSE 8074
COPY TradeExecutionAggregationService-0.0.1-SNAPSHOT app
ENTRYPOINT ["/app"]

