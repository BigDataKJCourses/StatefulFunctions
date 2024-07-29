package com.example.bigdata.tools;

import com.example.bigdata.model.ScoreEvent;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ScoreEventSchema implements DeserializationSchema<ScoreEvent>, SerializationSchema<ScoreEvent> {
    private static final long serialVersionUID = 1L;
    private transient Charset charset;

    public ScoreEventSchema() {
        this(StandardCharsets.UTF_8);
    }

    public ScoreEventSchema(Charset charset) {
        this.charset = charset;
    }

    public Charset getCharset() {
        return this.charset;
    }

    @Override
    public ScoreEvent deserialize(byte[] message) {
        String jsonString = new String(message, this.charset);
        JsonObject jsonObject = JsonParser.parseString(jsonString).getAsJsonObject();

        long timestampInMillis = parseStrTimeToMillis(jsonObject.get("ts").getAsString());

        return new ScoreEvent(
                jsonObject.get("house").getAsString(),
                jsonObject.get("character").getAsString(),
                jsonObject.get("score").getAsInt(),
                timestampInMillis
        );
    }

    @Override
    public boolean isEndOfStream(ScoreEvent nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(ScoreEvent element) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("house", element.getHouse());
        jsonObject.addProperty("character", element.getCharacter());
        jsonObject.addProperty("score", element.getScore());

        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        Instant instant = Instant.ofEpochMilli(element.getTs());
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);

        String formattedDate = dateTime.format(outputFormatter);
        jsonObject.addProperty("ts", formattedDate);

        String jsonString = jsonObject.toString();
        return jsonString.getBytes(this.charset);
    }

    @Override
    public TypeInformation<ScoreEvent> getProducedType() {
        return TypeExtractor.getForClass(ScoreEvent.class);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(this.charset.name());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }

    private long parseStrTimeToMillis(String strTime) {

        Pattern pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})(\\.\\d+)?");
        Matcher matcher = pattern.matcher(strTime);

        if (matcher.matches()) {
            String dateTimePart = matcher.group(1);
            String millisecondsPart = matcher.group(2);

            LocalDateTime dateTime = LocalDateTime.parse(dateTimePart, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            long timestampInMillis = dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();

            if (millisecondsPart != null) {
                millisecondsPart = millisecondsPart.substring(1); // Remove the dot at the beginning
                int milliseconds = Integer.parseInt(millisecondsPart);
                timestampInMillis += milliseconds;
            }

            return timestampInMillis;
        }
        return 0;
    }
}

