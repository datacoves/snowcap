---
description: >-
  A resource monitor in Snowflake. Manage it as code with Snowcap, with YAML and Python examples and a reference for every field, its type and its default.
---

# ResourceMonitor

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-resource-monitor) | Snowcap CLI label: `resource_monitor`

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
    triggers:
      - threshold: 75
        action: NOTIFY
      - threshold: 100
        action: SUSPEND
      - threshold: 110
        action: SUSPEND_IMMEDIATE
```


### Python

```python
resource_monitor = ResourceMonitor(
    name="some_resource_monitor",
    credit_quota=1000,
    frequency="DAILY",
    start_timestamp="2049-01-01 00:00",
    end_timestamp="2049-12-31 23:59",
    notify_users=["user1", "user2"],
    triggers=[
        {"threshold": 75, "action": "NOTIFY"},
        {"threshold": 100, "action": "SUSPEND"},
        {"threshold": 110, "action": "SUSPEND_IMMEDIATE"},
    ],
)
```


## Fields

* `name` (string, required) - The name of the resource monitor.
* `credit_quota` (int) - The amount of credits that can be used within the monitoring period.
* `frequency` (string or ResourceMonitorFrequency) - The interval at which the credit usage resets. Valid values: `MONTHLY`, `DAILY`, `WEEKLY`, `YEARLY`, `NEVER`. Defaults to `MONTHLY` when `start_timestamp` is set.
* `start_timestamp` (string) - The start time for the monitoring period (e.g., `"2024-01-01 00:00"`).
* `end_timestamp` (string) - The end time for the monitoring period.
* `notify_users` (list) - A list of user names to notify when thresholds are reached. Snowcap uppercases an unquoted name, as Snowflake does. Quote a name to keep its case, for example `'"alice"'`.
* `triggers` (list) - The actions to take at a percentage of the credit quota. Each trigger has a `threshold`, a whole percentage of 0 or more that can exceed 100, and an `action`: `NOTIFY`, `SUSPEND`, or `SUSPEND_IMMEDIATE`. Snowflake replaces every trigger whenever one changes. Omit `triggers` to leave the triggers unmanaged. An empty list is rejected, because Snowflake has no statement that removes every trigger.
* `owner` (string or [Role](role.md)) - The role that owns the monitor. Defaults to `ACCOUNTADMIN`. Snowcap creates the monitor as `ACCOUNTADMIN` and transfers it to this role. Later changes run as this role, because Snowflake lets only the owner alter or drop a monitor.


