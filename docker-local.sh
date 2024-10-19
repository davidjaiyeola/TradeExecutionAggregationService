#!/bin/bash
source ~/.bashrc

echo "Building"

mvnd clean install  -DskipTests

servicename="TradeExecutionAggregationService"
#get timestamp for the tag
timestamp=$(date +%Y%m%d%H%M%S)

export DOCKER_GIT_CREDENTIALS="$(cat ~/.git-credentials)"

#docker build --build-arg DOCKER_GIT_CREDENTIALS -t greenmedics.com:5001/greenmedics/$servicename:1.0-SNAPSHOT . -f Dockerfile
#docker tag $servicename:1.0-SNAPSHOT greenmedics.com:5001/greenmedics/$servicename:1.0-SNAPSHOT
#docker push greenmedics.com:5001/greenmedics/$servicename:1.0-SNAPSHOT

docker build -t tradeexecutionaggregationservice .  -f Dockerfile.local