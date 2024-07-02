package org.example;


import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Calendar;

public class JobScheduler {

    private static final Map<Integer, Scheduler> schedulers = new HashMap<>();
    private static List<Map<String, Object>> jobs ;
//    private static final String bootstrapServers = "kafka:9092";
    private static final String bootstrapServers = "192.168.98.3:9092";
//    private static final String bootstrapServers = "localhost:9092";

    public static void scheduleSparkJob(Map<String, Object> job, String bootstrapServers) throws SchedulerException, ParseException {
        Map<String, Object> conf = (Map<String, Object>) job.get("conf");

        String dateStart = (String) conf.get("date_start");
        String dateEnd = (String) conf.get("date_end");
        String timeStart = (String) conf.get("time");
        String timeEnd = (String) conf.get("time");


        System.out.println(job);
        Date startDateTime = getDateTime(dateStart, timeStart);
        Date endDateTime = getDateTime(dateEnd, timeEnd);

        JobDetail jobDetail = JobBuilder.newJob(SparkJob.class)
                .withIdentity("sparkJob_" + job.get("id"), "group" + job.get("id"))
                .build();

        jobDetail.getJobDataMap().put("job_spark", job);
        jobDetail.getJobDataMap().put("bootstrapServers", bootstrapServers);
        jobDetail.getJobDataMap().put("timeEnd", timeEnd);
        jobDetail.getJobDataMap().put("dateEnd", dateEnd);

        String cronExpression = getCronExpression(timeStart);

        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("trigger_" + job.get("id"), "group" + job.get("id"))
                .startAt(startDateTime)
                .endAt(endDateTime)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();

        StdSchedulerFactory factory = new StdSchedulerFactory();
        Properties props = new Properties();
        props.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "Scheduler_" + job.get("id"));
        props.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
        props.setProperty("org.quartz.threadPool.threadCount", "3");
        factory.initialize(props);

        Scheduler scheduler = factory.getScheduler();
        schedulers.put((int) job.get("id"), scheduler);

        scheduler.start();
        scheduler.scheduleJob(jobDetail, trigger);

        System.out.println("Job scheduled with cron expression: " + cronExpression);
        System.out.println("Job will start at: " + startDateTime);
        System.out.println("Job will end at: " + endDateTime);
    }

    private static String getCronExpression(String timeString) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        Date date = sdf.parse(timeString);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        String hour = String.valueOf(calendar.get(Calendar.HOUR_OF_DAY));
        String minute = String.valueOf(calendar.get(Calendar.MINUTE));
        return String.format("0 %s %s * * ?", minute, hour);
    }

    public static String getDateOnly(String isoDateString) throws ParseException {
        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Date date = isoFormat.parse(isoDateString);
        SimpleDateFormat dateOnlyFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateOnlyFormat.format(date);
    }

    private static Date getDateTime(String dateString, String timeString) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateOnly = getDateOnly(dateString);
        String dateTimeString = dateOnly + " " + timeString;
        return sdf.parse(dateTimeString);
    }

    public static void stopJob(int jobId) throws SchedulerException {
        Scheduler scheduler = schedulers.get(jobId);
        if (scheduler != null) {
            scheduler.deleteJob(new JobKey("sparkJob_" + jobId, "group" + jobId));
            scheduler.shutdown();
            schedulers.remove(jobId);
            System.out.println("Job with ID " + jobId + " has been stopped.");
        } else {
            System.out.println("No job found with ID " + jobId);
        }
    }

    public static void main(String[] args) {
        try {
            jobs = getJobs();
            new Thread(() -> {
                try {
                    postgresListener();
                } catch ( Exception e) {
                    e.printStackTrace();
                }
            }).start();

            for (Map<String, Object> job : jobs) {
                new Thread(() -> {
                    try {
                        scheduleSparkJob(job, bootstrapServers);
                    } catch (SchedulerException | ParseException e) {
                        e.printStackTrace();
                    }
                }).start();
            }

//            Thread.sleep(40000); // Sleep for 40 seconds to let the job start
//            int jobIdToStop = 8;
//            stopJob(jobIdToStop); // Stopping job with ID 8
//            SparkJob.stopJob(jobIdToStop); // Stopping Kafka consumer associated with job ID 8

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, Object>> getJobs() {
        Data data = new Data();
        return data.get_jobs();
    }


    private static void postgresListener(){
        String url = "jdbc:postgresql://localhost:5432/siem";
        String user = "postgres";
        String password = "karim123";

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
                        update_jobs();

                    }
                } else {
                    System.out.println("No notifications received");
                }
            }

        } catch (SQLException | InterruptedException | SchedulerException e) {
            e.printStackTrace();
        }
    }


    public static void update_jobs() throws SchedulerException {

        List<Map<String, Object>>  new_jobs = getJobs();
        System.out.println("size old_job : " + jobs.size() );
        System.out.println("size new_job : " + new_jobs.size() );
        //new job in database
        for (Map<String, Object> new_job : new_jobs) {

            if(!check_job_excite(jobs , new_job)){
                new Thread(() -> {
                    try {
                        scheduleSparkJob(new_job, bootstrapServers);
                    } catch (SchedulerException | ParseException e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        }


        //delete job

        for (Map<String, Object> job : jobs) {

            if(!check_job_excite(new_jobs , job)){
                int jobIdToStop = (int) job.get("id");
                stopJob(jobIdToStop);
                SparkJob.stopJob(jobIdToStop);
            }
        }


        jobs = new_jobs;
    }

    private static boolean check_job_excite(List<Map<String, Object>> jobs,Map<String, Object> job_test ){
        for (Map<String, Object> job : jobs) {
            if(job.get("id") == job_test.get("id")){
                return true;
            }
        }

        return false;

    }




}