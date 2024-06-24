package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.quartz.SchedulerException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class PostgreSQLListener {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5433/siem";
        String user = "postgres";
        String password = "cherrab123";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            PGConnection pgConn = conn.unwrap(PGConnection.class);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("LISTEN job_change");
                System.out.println("Listening for job_change notifications...");
            }

            while (true) {
                // Sleep for a while before checking again
                Thread.sleep(1000);

                // Poll for notifications
                PGNotification[] notifications = pgConn.getNotifications();
                if (notifications != null) {
                    for (PGNotification notification : notifications) {
                        System.out.println("Got notification: " + notification.getName());
                        JobScheduler.update_jobs();

                    }
                } else {
                    System.out.println("No notifications received");
                }
            }

        } catch (SQLException | InterruptedException | SchedulerException e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, Object>> getJobs() {
        Data data = new Data();
        System.out.println(data.get_jobs());
        return data.get_jobs();
    }


}
