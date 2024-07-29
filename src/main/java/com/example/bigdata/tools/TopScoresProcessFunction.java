package com.example.bigdata.tools;

import com.example.bigdata.model.ScoreEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopScoresProcessFunction extends ProcessWindowFunction<ScoreEvent,
        Tuple2<String, List<Tuple2<String, Integer>>>, String, TimeWindow> {

    DateTimeFormatter outputFormatter;

    @Override
    public void open(Configuration parameters) {
        outputFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    }

    @Override
    public void process(String key, Context context,
                        Iterable<ScoreEvent> input,
                        Collector<Tuple2<String, List<Tuple2<String, Integer>>>> out) {
        List<Tuple2<String, Integer>> tmpScoreEvents = new ArrayList<>();

        for (ScoreEvent se : input) {
            tmpScoreEvents.add(Tuple2.of(se.getCharacter(),se.getScore()));
        }
        tmpScoreEvents.sort(Comparator.comparingInt(t -> - t.f1));
        if (tmpScoreEvents.size() > 3) {
            tmpScoreEvents.subList(3, tmpScoreEvents.size()).clear();
        }

        Instant instant = Instant.ofEpochMilli(context.window().getStart());
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        String formattedDate = dateTime.format(outputFormatter);

        out.collect(new Tuple2<>(formattedDate + ":" + key, tmpScoreEvents));
    }
}




