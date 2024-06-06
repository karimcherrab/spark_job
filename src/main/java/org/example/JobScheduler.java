package org.example;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Calendar;

public class JobScheduler {

    public static void scheduleSparkJob(Map<String, Object> job, String bootstrapServers) throws SchedulerException, ParseException {
        Map<String, Object> conf = (Map<String, Object>) job.get("conf");

        // Extract configuration details
        String dateStart = (String) conf.get("date_start");
        String dateEnd = (String) conf.get("date_end");
        String timeStart = (String) conf.get("time");

        // Combine date and time to create start and end Date objects
        Date startDateTime = getDateTime(dateStart, timeStart);
        Date endDateTime = getDateTime(dateEnd, "17:34:00");

        // Create job detail
        JobDetail jobDetail = JobBuilder.newJob(SparkJob.class)
                .withIdentity("sparkJob_" + job.get("id"), "group1")
                .build();

        // Pass job data to job detail
        jobDetail.getJobDataMap().put("job_spark", job);
        jobDetail.getJobDataMap().put("bootstrapServers", bootstrapServers);

        // Create cron expression for running the job daily at the specified time
        String cronExpression = getCronExpression(timeStart);

        // Create trigger with start and end time
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("trigger_" + job.get("id"), "group1")
                .startAt(startDateTime)  // Set the start time
                .endAt(endDateTime)      // Set the end time
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();

        // Start scheduler
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        Scheduler scheduler = schedulerFactory.getScheduler();
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
        // Parse the ISO 8601 date string
        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Date date = isoFormat.parse(isoDateString);

        // Format the date to "yyyy-MM-dd"
        SimpleDateFormat dateOnlyFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateOnlyFormat.format(date);
    }
    private static Date getDateTime(String dateString, String timeString) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateOnly = getDateOnly(dateString);

        String dateTimeString = dateOnly + " " + timeString;

        System.out.println(dateString);
        System.out.println(dateTimeString);
        return sdf.parse(dateTimeString);
    }

    public static void main(String[] args) {
        try {
            List<Map<String, Object>> jobs = getJobs(); // Replace with your method to get jobs
            String bootstrapServers = "localhost:9092";
            for (Map<String, Object> job : jobs) {
                scheduleSparkJob(job, bootstrapServers);
            }
        } catch (SchedulerException | ParseException e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, Object>> getJobs() {
        Data data = new Data();
        return data.get_jobs();
    }
}
