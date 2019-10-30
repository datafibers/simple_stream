package com.datafibers.flink.table_api.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;


public class ItemsSource implements StreamTableSource<Row> {
    @Override
    public TypeInformation<Row> getReturnType() {


        String[] names = new String[] {"ItemID" , "LineID"};
        TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING()};
        return Types.ROW(names, types);


    }

    @Override
    public TableSchema getTableSchema() {

        TypeInformation[] typeInfo = new TypeInformation[2];
        typeInfo[0] = Types.STRING();
        typeInfo[1] = Types.STRING();
        return new TableSchema(new String[] {"ItemID", "LineID"}, typeInfo);
    }

    @Override
    public String explainSource() {
        return "Item ID and Line ID";
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {

        DataStream<Row> dummy = execEnv.fromElements("0,0","1,1","2,2").map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) throws Exception {
                String[] values = s.split(",");
                return Row.of(values[0],values[1]);
            }
        }).returns(Types.ROW(Types.STRING(),Types.STRING()));
        return dummy;
    }
}
