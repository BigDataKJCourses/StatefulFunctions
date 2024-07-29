package com.example.bigdata.connectors;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLStats3Sink {
    /* DROP TABLE house_stats_sink;

       CREATE TABLE house_stats_sink (
        house VARCHAR(200) PRIMARY KEY,
        how_many BIGINT,
        sum_score BIGINT);
     */
    public static final String INSERT_COMMAND = "insert into house_stats_sink " +
            "(how_many, sum_score, house) \n" +
            "values (?, ?, ?)";
    public static final String UPDATE_COMMAND = "update house_stats_sink " +
            "set how_many = ?, sum_score = ? \n" +
            "where house = ? ";

    public static final String UPSERT_COMMAND = "insert into house_stats_sink " +
            "(how_many, sum_score, house) \n" +
            "values (?, ?, ?) \n" +
            "ON DUPLICATE KEY UPDATE\n" +
            "  how_many = ?, " +
            "  sum_score = ? ";

    public static SinkFunction<Tuple3<String, Integer, Integer>> create(ParameterTool properties, String command) {
        String url = properties.getRequired("mysql.url");
        String username = properties.getRequired("mysql.username");
        String password = properties.getRequired("mysql.password");

        JdbcStatementBuilder<Tuple3<String, Integer, Integer>> statementBuilder =
                new JdbcStatementBuilder<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void accept(PreparedStatement ps, Tuple3<String, Integer, Integer> data) throws SQLException {
                        ps.setLong(1, data.f1);
                        ps.setLong(2, data.f2);
                        ps.setString(3, data.f0);
                        if (command.equals(UPSERT_COMMAND)) {
                            ps.setLong(4, data.f1);
                            ps.setLong(5, data.f2);
                        }
                    }
                };

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        SinkFunction<Tuple3<String, Integer, Integer>> jdbcSink =
                JdbcSink.sink(command,
                        statementBuilder,
                        executionOptions,
                        connectionOptions);

        return jdbcSink;
    }
}
