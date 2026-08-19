from dataclasses import dataclass
from typing import Optional

from ..enums import ResourceType, TaskState
from ..props import AlertConditionProp, Props, QueryProp, StringProp, TagsProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec
from .tag import TaggableResource
from .warehouse import Warehouse


@dataclass(unsafe_hash=True)
class _Alert(ResourceSpec):
    name: ResourceName
    condition: str
    then: str
    # A triggered/manual alert can omit schedule (it runs only via EXECUTE ALERT or a trigger)
    # and warehouse (serverless, needs the EXECUTE MANAGED ALERT priv).
    schedule: Optional[str] = None
    warehouse: Optional[Warehouse] = None
    owner: RoleRef = "SYSADMIN"
    comment: str = None
    # Alerts are created SUSPENDED; STARTED is reached via ALTER ALERT ... RESUME.
    # TaskState (STARTED/SUSPENDED) is the same started/suspended scheduling state.
    state: TaskState = TaskState.SUSPENDED


class Alert(NamedResource, TaggableResource, Resource):
    """
    Description:
        Alerts trigger notifications when certain conditions are met.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-alert

    Fields:
        name (string, required): The name of the alert.
        warehouse (string or Warehouse): The warehouse to run the query on. Omit for a serverless alert.
        schedule (string): The schedule for the alert to run on. Omit for a triggered/manual alert.
        condition (string): The condition for the alert to trigger on.
        then (string): The query to run when the alert triggers.
        owner (string or Role): The owner role of the alert. Defaults to "SYSADMIN".
        comment (string): A comment for the alert. Defaults to None.
        state (string or TaskState): The scheduling state of the alert, STARTED or SUSPENDED. Defaults to SUSPENDED.
        tags (dict): Tags for the alert. Defaults to None.

    Python:

        ```python
        alert = Alert(
            name="some_alert",
            warehouse="some_warehouse",  # omit for a serverless alert
            schedule="USING CRON * * * * *",
            condition="SELECT COUNT(*) FROM some_table",
            then="CALL SYSTEM$SEND_EMAIL('example@example.com', 'Alert Triggered', 'The alert condition was met.')",
            state="STARTED",
        )
        ```

    Yaml:

        ```yaml
        alerts:
          - name: some_alert
            warehouse: some_warehouse  # omit for a serverless alert
            schedule: USING CRON * * * * *
            state: STARTED
            condition: SELECT COUNT(*) FROM some_table
            then: CALL SYSTEM$SEND_EMAIL('example@example.com', 'Alert Triggered', 'The alert condition was met.')
        ```
    """

    resource_type = ResourceType.ALERT
    props = Props(
        warehouse=StringProp("warehouse"),
        schedule=StringProp("schedule"),
        comment=StringProp("comment"),
        tags=TagsProp(),
        condition=AlertConditionProp(),
        then=QueryProp("then"),
    )
    scope = SchemaScope()
    spec = _Alert

    def __init__(
        self,
        name: str,
        condition: str,
        then: str,
        schedule: str = None,
        warehouse: Warehouse = None,
        owner: str = "SYSADMIN",
        comment: str = None,
        state: TaskState = TaskState.SUSPENDED,
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Alert = _Alert(
            name=self._name,
            warehouse=warehouse,
            schedule=schedule,
            condition=condition,
            then=then,
            owner=owner,
            comment=comment,
            state=state,
        )
        self.set_tags(tags)
        if self._data.warehouse:
            self.requires(self._data.warehouse)
