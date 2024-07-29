package com.example.bigdata.connectors;

import com.example.bigdata.model.ScoreEvent;
import com.example.bigdata.tools.ScoreEventSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class ScoreEventKafkaSource {

    static public KafkaSource<ScoreEvent> create(ParameterTool properties) {
        String bootstrapServers = properties.getRequired("kafka.bootstrap-servers");
        String topics = properties.getRequired("kafka.input-topic");

        return KafkaSource.<ScoreEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setTopics(topics)
                .setGroupId("state-proc-app")
                .setStartingOffsets(OffsetsInitializer.
                        committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new ScoreEventSchema())
                .build();
    }
}

