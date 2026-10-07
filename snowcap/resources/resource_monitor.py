from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import (
    EnumProp,
    IntProp,
    Props,
    StringListProp,
    StringProp,
    TriggersProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role


class ResourceMonitorFrequency(ParseableEnum):
    MONTHLY = "MONTHLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    YEARLY = "YEARLY"
    NEVER = "NEVER"


class ResourceMonitorAction(ParseableEnum):
    NOTIFY = "NOTIFY"
    SUSPEND = "SUSPEND"
    SUSPEND_IMMEDIATE = "SUSPEND_IMMEDIATE"


@dataclass(unsafe_hash=True)
class _ResourceMonitor(ResourceSpec):
    name: ResourceName
    owner: Role = "ACCOUNTADMIN"
    credit_quota: int = None
    frequency: ResourceMonitorFrequency = None
    start_timestamp: str = None
    end_timestamp: str = None
    notify_users: list[str] = None
    triggers: list[dict] = None

    def __post_init__(self):
        super().__post_init__()
        if self.credit_quota is not None and not isinstance(self.credit_quota, int):
            raise ValueError("credit_quota must be an integer or None")
        if self.start_timestamp and self.frequency is None:
            self.frequency = ResourceMonitorFrequency.MONTHLY
        # Snowflake reports both lists in its own order, and neither order carries meaning,
        # so both sides of a diff are sorted the same way.
        if self.notify_users:
            self.notify_users = sorted(self.notify_users)
        if self.triggers is not None:
            if not self.triggers:
                raise ValueError(
                    "triggers cannot be empty: Snowflake has no statement that removes every trigger "
                    "from a resource monitor. Omit triggers to leave them unmanaged."
                )
            if any(not isinstance(trigger["threshold"], int) for trigger in self.triggers):
                raise ValueError("trigger thresholds must be whole percentages, for example 75")
            self.triggers = sorted(
                (
                    {"threshold": trigger["threshold"], "action": ResourceMonitorAction(trigger["action"])}
                    for trigger in self.triggers
                ),
                key=lambda trigger: (trigger["threshold"], trigger["action"].value),
            )


class ResourceMonitor(NamedResource, Resource):
    """
    Description:
        Manages the monitoring of resource usage within an account.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-resource-monitor

    Fields:
        name (string, required): The name of the resource monitor.
        credit_quota (int): The amount of credits that can be used by this monitor. Defaults to None.
        frequency (string or ResourceMonitorFrequency): The frequency of monitoring. Defaults to None.
        start_timestamp (string): The start time for the monitoring period. Defaults to None.
        end_timestamp (string): The end time for the monitoring period. Defaults to None.
        notify_users (list): A list of users to notify when thresholds are reached. Defaults to None.
        triggers (list): The actions to take at a percentage of the credit quota. Each trigger has a
            `threshold` (an integer percentage, which can exceed 100) and an `action` (NOTIFY,
            SUSPEND or SUSPEND_IMMEDIATE). Snowflake replaces every trigger whenever one changes.
            Defaults to None, which leaves the triggers unmanaged.
        owner (string or Role): The role that owns the monitor. Snowcap creates the monitor as
            ACCOUNTADMIN and transfers it to this role. Defaults to "ACCOUNTADMIN".

    Python:

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

    Yaml:

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
    """

    resource_type = ResourceType.RESOURCE_MONITOR
    props = Props(
        _start_token="WITH",
        credit_quota=IntProp("credit_quota"),
        frequency=EnumProp("frequency", ResourceMonitorFrequency),
        start_timestamp=StringProp("start_timestamp", alt_tokens=["IMMEDIATELY"]),
        end_timestamp=StringProp("end_timestamp"),
        notify_users=StringListProp("notify_users", parens=True),
        triggers=TriggersProp("triggers", ResourceMonitorAction),
    )
    scope = AccountScope()
    spec = _ResourceMonitor

    def __init__(
        self,
        name: str,
        credit_quota: int = None,
        frequency: ResourceMonitorFrequency = None,
        start_timestamp: str = None,
        end_timestamp: str = None,
        notify_users: list[str] = None,
        triggers: list[dict] = None,
        owner: str = "ACCOUNTADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ResourceMonitor = _ResourceMonitor(
            name=self._name,
            credit_quota=credit_quota,
            frequency=frequency,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            notify_users=notify_users,
            triggers=triggers,
            owner=owner,
        )
        # TODO: rely on notify_users
