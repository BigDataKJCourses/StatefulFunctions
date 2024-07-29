package com.example.bigdata;

import com.example.bigdata.connectors.ScoreEventKafkaSource;
import com.example.bigdata.model.ScoreEvent;
import com.example.bigdata.tools.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class HouseStatsAnalysisCheckpoint {
    public static void main(String[] args) throws Exception {

        ParameterTool properties = Properties.get(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(properties.getInt("checkpoint.interval",1000));
        env.getCheckpointConfig().setCheckpointStorage(properties.get("checkpoint.path"));
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStream<ScoreEvent> scoreEventDS = env
                    .fromSource(ScoreEventKafkaSource.create(properties),
                            WatermarkStrategy.forBoundedOutOfOrderness(
                                    Duration.ofMillis(Long.parseLong(properties.get("data.input.delay")))),
                            "Kafka Source");

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> houseStatsDS =
                scoreEventDS
                        .map(se -> Tuple3.of(se.getHouse(), se.getScore(), 1))
                        .returns(Types.TUPLE(Types.STRING,Types.INT,Types.INT))
                        .keyBy(t -> t.f0)
                        .reduce((t1, t2) -> Tuple3.of(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2));

        houseStatsDS.print();
        //houseStatsDS.addSink(MySQLStats3Sink.create(properties, MySQLStats3Sink.UPSERT_COMMAND));

        env.execute("House Stats Analysis - With Checkpoint");
    }
}
