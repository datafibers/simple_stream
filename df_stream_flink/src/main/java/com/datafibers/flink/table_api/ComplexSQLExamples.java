package com.datafibers.flink.table_api;

import com.datafibers.flink.table_api.util.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

public class ComplexSQLExamples {

    public static void doStreamWindowing(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv)
    {
        // Stream windowing
        // Assume a source of page views that reports the country from which the page view is coming and time stamp of the view
        // In this example, we first register the stream of page views as a table with two columns nation and rowtime, to refer to the timestamp
        DataStream<Tuple2<String, Long>> pageViews = env.addSource(new PageViewsSource(1000)).assignTimestampsAndWatermarks(new WatermarkAssigner());

        tableEnv.registerDataStream("PageViews", pageViews, "nation, rowtime.rowtime");
        // We apply a hopping (sliding) window over the table
        // It is like an aggregate query where to group by nation column and the hopping window specification of 1 minute window that updates every second
        Table resultHop = tableEnv.sqlQuery("SELECT nation , COUNT(*) FROM PageViews GROUP BY HOP( rowtime , INTERVAL '1' MINUTE, INTERVAL '1' SECOND) , nation");
        // Here, we apply a session window on the same original stream (table) of page views. The session timeout is 1 second
        // We use the Session_start function to project the session start time stamp as a column in the results
        Table resultSession = tableEnv.sqlQuery("SELECT nation , COUNT(*), SESSION_START(rowtime , INTERVAL '1' SECOND), SESSION_ROWTIME(rowtime , INTERVAL '1' SECOND)  FROM PageViews GROUP BY SESSION( rowtime , INTERVAL '1' SECOND) , nation");

        // We need to transform the tuples in the tables carrying query results back to streams.
        //In CQL R2S (Relation to stream) transformation.
        // In order to do so, we need to let the stream know about the schema of the tuples
        TupleTypeInfo<Tuple2<String, Long>> tupleTypeHop = new TupleTypeInfo<>(
                Types.STRING(),
                Types.LONG());

        TupleTypeInfo<Tuple4<String, Long, Long, Long>> tupleTypeSession = new TupleTypeInfo<>(
                Types.STRING(),
                Types.LONG(),
                Types.SQL_TIMESTAMP(),
                Types.SQL_TIMESTAMP());

        // Write results from the table back to an append-only stream
        DataStream<Tuple2<String, Long>> queryResultHop = tableEnv.toAppendStream(resultHop, tupleTypeHop);
        // Prints a tuple of 2 of <String, Long> which is the country name and the count of views in the respective window instance.
        queryResultHop.print();


        DataStream<Tuple4<String, Long, Long, Long>> queryResultSession = tableEnv.toAppendStream(resultSession, tupleTypeSession);
        // Prints a tuple of 4 <String, Long, DateTime, DateTime> which corresponds to nation, number of views, session window start,
        //inclusive session window end
        queryResultSession.print();
    }
    public static void doStreamTableJoin(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv)
    {
        // Stream to table joins
        // Assume readings from a sensor that report the sensor id, line ID, reading value and the time stamp
        DataStream<Tuple4<String, String, Double, Long>> sensorReadings = env.addSource(new SensorSource());
        // Register as a table in order to run SQL queries against it.
        tableEnv.registerDataStream("SensorReadings", sensorReadings, "sensorID, lineID, readingValue, rowtime.rowtime");

        // This is a finite list of items which has an item ID an a line ID.
        CsvTableSource itemsSource = new CsvTableSource.Builder().path("data.csv").ignoreParseErrors().field("ItemID", Types.STRING())
                .field("LineID", Types.STRING()).build();

       // Similarly, register this list of items as a table.
        tableEnv.registerTableSource("Items", itemsSource);

        // Apply a join query and store the results in a dynamic (stream table) table
        Table streamTableJoinResult = tableEnv.sqlQuery("SELECT S.sensorID , S.readingValue , I.ItemID FROM SensorReadings S LEFT JOIN Items I ON S.lineID = I.LineID");

        // Transform the results again to a stream so that we can print it or use it elsewhere.
        DataStream<Tuple2<Boolean, Row>> joinQueryResult = tableEnv.toRetractStream(streamTableJoinResult, Row.class );


        joinQueryResult.print();

    }

    public static void doStreamToStreamJoin(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv)
    {
        // Stream to Stream joins

        // Assume a source of impressions which reports an id and value(int) and a time stamp
        // we assume two sources of the same schema, impressions and clicks to simulate if the user clicks on a link that he has an impression of
        DataStream<Tuple2<String, Long>> impressions = env.addSource(new ImpressionSource(500));
        DataStream<Tuple2<String, Long>> clicks = env.addSource(new ImpressionSource(100));
        // Register streams as tables, we explicitly specify fields so that we can given them names of our choice.
        tableEnv.registerDataStream("Impressions", impressions, "impressionID, rowtime.rowtime");
        tableEnv.registerDataStream("Clicks", clicks, "clickID, rowtime.rowtime");

        // Apply a stream-to-stream join using implicit windows of choosing the time stamp of one stream to be within a range of the time stamp
        // of the other stream
        Table streamStreamJoinResult = tableEnv.sqlQuery("SELECT i.impressionID, i.rowtime as impresstionTime, c.clickID, cast (c.rowtime as TIMESTAMP) as clickTime FROM Impressions i , Clicks c WHERE i.impressionID = c.clickID AND c.rowtime BETWEEN i.rowtime - INTERVAL '1' SECOND AND i.rowtime");
        // define a schema to use to write the data from the result table back to a stream.
        TupleTypeInfo<Tuple4<String, TimeIndicatorTypeInfo, String, TimeIndicatorTypeInfo>> streamStreamJoinResultTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.SQL_TIMESTAMP(), Types.STRING(), Types.SQL_TIMESTAMP());

        // The output is on the form of Tuple <String, Datetime, String, DateTime> which represent the impression id, its time stamp and the click id and its time stamp
        DataStream<Tuple4<String, TimeIndicatorTypeInfo, String, TimeIndicatorTypeInfo>> streamStreamJoinResultAsStream = tableEnv.toAppendStream(streamStreamJoinResult,streamStreamJoinResultTypeInfo);

        streamStreamJoinResultAsStream.print();
    }
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        doStreamWindowing(env, tableEnv);
//
        doStreamTableJoin(env, tableEnv);

        doStreamToStreamJoin(env, tableEnv);


        env.execute("Example SQL");
    }
}
