package com.lagou.edu.transformation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;


import java.util.ArrayList;

public class DataSetTransformation {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<String, Integer>> listData = new ArrayList<Tuple2<String, Integer>>();
        listData.add(new Tuple2<String,Integer>("java",1));
        listData.add(new Tuple2<String,Integer>("java",1));
        listData.add(new Tuple2<String,Integer>("scala",1));
        DataSource<Tuple2<String, Integer>> data = env.fromCollection(listData);
        UnsortedGrouping<Tuple2<String, Integer>> grouped = data.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> sum = grouped.sum(1);
        ReduceOperator<Tuple2<String, Integer>> reduced = grouped.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
               return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
            }
        });
        reduced.print();
//        sum.print();
    }
}
