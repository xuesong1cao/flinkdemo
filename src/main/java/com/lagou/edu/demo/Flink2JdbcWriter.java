package com.lagou.edu.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * @author nandy
 * @create 2021/3/2 17:13
 */
@Slf4j
public class Flink2JdbcWriter extends RichSinkFunction<List<Student>> {
    private static final long serialVersionUID = -5072869539213229634L;


    private transient Connection connection = null;
    private transient PreparedStatement ps = null;
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = ConnectionPoolUtils.getConnection();
        if (null != connection) {
            ps = connection.prepareStatement("insert  into student (name, age, createDate) values (?, ?, ?);");
        }
    }

    @Override
    public void invoke(List<Student> list, Context context) throws Exception {

        if (isRunning && null != ps) {
            for (Student one : list) {
                ps.setString(1, one.getName());
                ps.setInt(2, one.getAge());
                ps.setString(3, one.getCreateDate());
                ps.addBatch();
            }
            int [] count = ps.executeBatch();
            log.info("成功写入Mysql数量：" + count.length);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        isRunning = false;
    }
}

