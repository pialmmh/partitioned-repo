# Monitoring Requirements for Microservice (e.g. partitioned-repo)

1. Purpose:
   Continuously monitor and report runtime status and custom health metrics from a Java microservice (e.g., partitioned-repo), which uses Quartz Scheduler and handles auto-maintenance of partitioned tables.

2. Reporting Interval:
   Metrics should be collected and reported periodically (e.g., every 30 or 60 seconds).

3. Metrics to Monitor:

    - Scheduler:
        - Last run timestamp of Quartz job (epoch or formatted)
        - Next scheduled time of the job
        - Next partitions to be deleted
        - Duration of last job run

    - Partition Maintenance:
        - Repository type: Partitioned or Multitable
        - Repository name: Prefix of the repo e.g. sms
        - No of Partitions (from "show create table <table name>")
        - No of prefixed tables (from "show tables from <database>")
        - Total list of partition
        - Number of partitions created in last run
        - Number of partitions deleted in last run

    - Errors:
        - Whether the last maintenance job failed or succeeded
        - Total number of failures

    - Service Info:
        - Hostname or instance ID
        - Startup timestamp (optional)

4. Delivery Options (any of the following):
    - Expose via HTTP (e.g., `/metrics` endpoint in Prometheus format)
    - Push to a REST endpoint periodically (e.g., JSON payload)
    - Push to Kafka (optional)
    - Log via SLF4J on a schedule (if external scraping is used)

5. Framework Agnostic:
   The solution should be pluggable into both standalone Java services and Spring Boot/Quarkus environments.

6. Low Overhead:
   Use a lightweight metrics library or custom scheduler. Should not require external database writes or blocking operations during reporting.

7. Future Extensibility:
   The system should allow adding new custom metrics easily, like:
    - Number of SQL queries run
    - Cache hit/miss
    - DB connection pool stats