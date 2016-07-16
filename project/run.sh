#!/usr/bin/env bash
mvn package && spark-submit --class "ResponseClassifierFeaturesMain" \
--master local[4] target/slack-email-1.0-SNAPSHOT.jar \
--conf "spark.executor.extraJavaOptions=-Xmx:6g" myApp.jar

