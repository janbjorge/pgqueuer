# Dashboard

The dashboard command provides a real-time view of job processing statistics, which can be refreshed at a specified interval. This is particularly useful for monitoring the status of jobs dynamically. Below are the options available for customizing the dashboard display:

  - `--interval <seconds>`: Set the refresh interval in seconds for updating the dashboard display. If not set, the dashboard will update only once and then exit.
  - `--tail <number>`: Specify the number of the most recent log entries to display.
  - `--table-format <format>`: Choose the format of the table used to display statistics. Supported formats include grid, plain, html, and others provided by the tabulate library.

Example command to launch the dashboard:
```bash
python -m PgQueuer dashboard --interval 10 --tail 25 --table-format grid
```

Example output from the dashboard:
```bash
+---------------------------+-------+------------+--------------------------+------------+----------+
|          Created          | Count | Entrypoint | Time in Queue (HH:MM:SS) |   Status   | Priority |
+---------------------------+-------+------------+--------------------------+------------+----------+
| 2024-05-05 16:44:26+00:00 |  49   |    sync    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:26+00:00 |  82   |   async    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:26+00:00 | 1615  |    sync    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:26+00:00 | 1586  |   async    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:25+00:00 |  198  |    sync    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:25+00:00 |  230  |   async    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:25+00:00 | 1802  |    sync    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:25+00:00 | 1778  |   async    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:24+00:00 | 1500  |    sync    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:24+00:00 | 1506  |   async    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:23+00:00 | 1505  |   async    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:23+00:00 | 1500  |    sync    |         0:00:00          | successful |    0     |
+---------------------------+-------+------------+--------------------------+------------+----------+
```