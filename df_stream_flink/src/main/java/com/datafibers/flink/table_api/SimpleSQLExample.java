package com.datafibers.flink.table_api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * Desc: Convert DataStreams to Tables
 */
public class SimpleSQLExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);

        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        Table tableA = tEnv.fromDataStream(orderA, "user, product, amount");

        tEnv.registerDataStream("OrderB", orderB, "user, product, amount");

        Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
                "SELECT * FROM OrderB WHERE amount < 2");

        tEnv.toAppendStream(result, Order.class).print();

        env.execute();
    }



    public static class Order {

        public Long user;

        public String product;

        public int amount;

        // public constructor to make it a Flink POJO
        public Order() {}

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order " + user + " " + product + " "  +amount;
        }
    }
}
