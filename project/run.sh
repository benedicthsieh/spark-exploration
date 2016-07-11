mvn package && spark-submit --class "DirectAndBroadcastMain" --master local[4] target/slack-email-1.0-SNAPSHOT.jar
