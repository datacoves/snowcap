---
description: >-
  A resource monitor in Snowflake.
---

# ResourceMonitor

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-resource-monitor)

Manages the monitoring of resource usage within an account.


## Examples

### YAML

```yaml
resource_monitors:
  - name: some_resource_monitor
    credit_quota: 1000
    frequency: DAILY
    start_timestamp: "2049-01-01 00:00"
    end_timestamp: "2049-12-31 23:59"
    notify_users:
      - user1
      - user2
```


### Python

```python
resource_monitor = ResourceMonitor(
    name="some_resource_monitor",
    credit_quota=1000,
    frequency="DAILY",
    start_timestamp="2049-01-01 00:00",
    end_timestamp="2049-12-31 23:59",
    notify_users=["user1", "user2"]
)
```


## Fields

* `name` (string, required) - The name of the resource monitor.
* `credit_quota` (int) - The amount of credits that can be used within the monitoring period.
* `frequency` (string or ResourceMonitorFrequency) - The interval at which the credit usage resets. Valid values: `MONTHLY`, `DAILY`, `WEEKLY`, `YEARLY`, `NEVER`. Defaults to `MONTHLY` when `start_timestamp` is set.
* `start_timestamp` (string) - The start time for the monitoring period (e.g., `"2024-01-01 00:00"`).
* `end_timestamp` (string) - The end time for the monitoring period.
* `notify_users` (list) - A list of user names to notify when thresholds are reached.

**Note:** Resource monitors can only be owned by ACCOUNTADMIN.


