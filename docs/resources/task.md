---
description: >-
  A scheduled task in Snowflake.
---

# Task

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-task) | Snowcap CLI label: `task`

Represents a scheduled task in Snowflake that performs a specified SQL statement at a recurring interval.


## Examples

### YAML

```yaml
tasks:
  - name: some_task
    warehouse: some_warehouse
    schedule: USING CRON 0 9 * * * UTC
    state: SUSPENDED
    as_: SELECT 1
```

### Python

```python
task = Task(
    name="some_task",
    warehouse="some_warehouse",
    schedule="USING CRON 0 9 * * * UTC",
    state="SUSPENDED",
    as_="SELECT 1"
)
```


## Fields

* `name` (string, required) - The name of the task.
* `owner` (string or [Role](role.md)) - The owner of the task. Defaults to "SYSADMIN".
* `warehouse` (string or [Warehouse](warehouse.md)) - The warehouse used by the task. Cannot be set if `user_task_managed_initial_warehouse_size` is specified.
* `user_task_managed_initial_warehouse_size` (string or WarehouseSize) - The initial warehouse size for serverless tasks. If neither this nor `warehouse` is set, defaults to MEDIUM for serverless execution.
* `schedule` (string) - The schedule on which the task runs (e.g., `"USING CRON 0 9 * * * UTC"` or `"1 MINUTE"`).
* `config` (string) - Configuration settings for the task in JSON format.
* `allow_overlapping_execution` (bool) - Whether the task can have overlapping executions. Defaults to False.
* `user_task_timeout_ms` (int) - The timeout in milliseconds after which the task is aborted.
* `suspend_task_after_num_failures` (int) - The number of consecutive failures after which the task is suspended. Defaults to 10 for root tasks.
* `error_integration` (string) - The notification integration used for error handling.
* `copy_grants` (bool) - Whether to copy grants when replacing the task.
* `comment` (string) - A comment for the task.
* `after` (list) - A list of predecessor tasks that must complete before this task runs. Used for task DAGs.
* `when` (string) - A conditional expression (e.g., `SYSTEM$STREAM_HAS_DATA('mystream')`) that determines when the task runs.
* `as_` (string) - The SQL statement that the task executes.
* `state` (string or TaskState) - The initial state of the task. Defaults to SUSPENDED.


