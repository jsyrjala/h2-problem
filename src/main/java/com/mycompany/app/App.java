package com.mycompany.app;


import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.System.*;

public class App {
    public static void main(String[] args) throws SQLException, InterruptedException {
        new App().start();
    }

    public void start() throws SQLException, InterruptedException {
        out.println("Create tables");
        Connection connCreate = getConnection();
        createTable(connCreate);
        out.println("Create rows");
        int batchSize = 100;
        int reserveSize = 75;
        for (int i = 0; i < batchSize; i++) {
            insertRow(connCreate);
        }
        connCreate.commit();

        Connection connPoll1 = getConnection();
        Connection connPoll2 = getConnection();

        Poller poller1 = new Poller(connPoll1, 1, reserveSize);
        Poller poller2 = new Poller(connPoll2, 2, reserveSize);

        Thread thread1 = new Thread(poller1);
        Thread thread2 = new Thread(poller2);

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();
        out.println("Reservations done");
        out.println("poller 1 reserved " + poller1.results.size());
        out.println("poller 2 reserved " + poller2.results.size());

        if(poller1.results.size() + poller2.results.size() > batchSize) {
            err.println("ERROR: reserved more than total number of rows");
        }
        List<Integer> intersection = new ArrayList<>(poller1.results);
        intersection.retainAll(poller2.results);

        if (!intersection.isEmpty()) {
            err.println("Some rows got reserved twice: "  + intersection);
        }
    }

    public void createTable(Connection conn) throws SQLException {
        String sql = "CREATE TABLE t1 " +
                "(id INT AUTO_INCREMENT PRIMARY KEY, " +
                "version INT, " +
                "reserved INT)";
        conn.prepareStatement(sql).execute();
    }

    public void insertRow(Connection conn) throws SQLException {
        String sql = "insert into t1 (version) values (0)";
        conn.prepareStatement(sql).execute();
    }

    public Connection getConnection() throws SQLException {
        // return DriverManager.getConnection("jdbc:h2:mem:testdb;TRACE_LEVEL_FILE=4;DB_CLOSE_DELAY=-1");
        Connection conn =  DriverManager.getConnection("jdbc:h2:mem:testdb");
        conn.setAutoCommit(false);
        return conn;
    }

    static class Poller implements Runnable {
        final Connection conn;
        final int poller;
        final int batchSize;
        public List<Integer> results;
        public Poller(Connection conn, int poller, int batchSize) {
            this.conn = conn;
            this.poller = poller;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            System.out.println("Poller " + poller + " starting");
            try {
                results = poll();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public List<Integer> poll() throws SQLException {
            List<Integer> ids = getFreeIds();
            System.out.println("Poller " + poller + " got " + ids.size() + " rows");
            return reserve(ids);
        }

        public List<Integer> getFreeIds() throws SQLException {
            String sql = "select * from t1 where reserved is null order by id limit " + batchSize;
            PreparedStatement stm = conn.prepareStatement(sql);
            ResultSet rs = stm.executeQuery();

            List<Integer> ids = new ArrayList<>();
            while(rs.next()) {
                ids.add(rs.getInt("id"));
            }
            rs.close();
            stm.close();
            return ids;
        }

        public List<Integer> reserve(List<Integer> ids) throws  SQLException {
            System.out.println("Poller " + poller + ": reserving " + ids.size() + " rows");
            String sql = "update t1 set reserved = ? where id = ? and reserved is null";

            PreparedStatement stm = conn.prepareStatement(sql);
            for(Integer id: ids) {
                stm.setInt(1, poller);
                stm.setInt(2, id);
                stm.addBatch();
            }
            int[] results = stm.executeBatch();
            int index = -1;
            Iterator<Integer> iter = ids.iterator();
            while(iter.hasNext()) {
                Integer id  = iter.next();
                index ++;
                if (results[index] == 0) {
                    iter.remove();
                }
                System.out.println(poller + ": Update stat:" + results[index] + " id=" + id);
            }
            conn.commit();
            return ids;
        }
    }

}