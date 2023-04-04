#!/usr/bin/env bash
docker rm -f rabbit || true
docker run --rm -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:management
