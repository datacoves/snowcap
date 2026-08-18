from dataclasses import dataclass, field
from typing import Optional

from ..enums import ResourceType, TaskState, WarehouseSize
from ..props import (
    BoolProp,
    EnumProp,
    ExpressionProp,
    FlagProp,
    IdentifierListProp,
    IdentifierProp,
    IntProp,
    Props,
    QueryProp,
    StringProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec, convert_to_resource
from .warehouse import Warehouse


@dataclass(unsafe_hash=True)
class _Task(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    warehouse: Optional[Warehouse] = None
    user_task_managed_initial_warehouse_size: Optional[WarehouseSize] = None
    schedule: Optional[str] = None
    config: Optional[str] = None
    allow_overlapping_execution: bool = False
    user_task_timeout_ms: Optional[int] = None  # Uses Snowflake default (3600000) if not specified
    suspend_task_after_num_failures: Optional[int] = None
    error_integration: Optional[str] = None
    success_integration: Optional[str] = None
    serverless_task_min_statement_size: Optional[WarehouseSize] = None
    serverless_task_max_statement_size: Optional[WarehouseSize] = None
    task_auto_retry_attempts: Optional[int] = None
    user_task_minimum_trigger_interval_in_seconds: Optional[int] = None
    target_completion_interval: Optional[str] = None
    # ponytail: finalize wires a task into a DAG's finalizer slot at creation; not fetched
    # back (create-only, like copy_grants), so declaring it on a live task is a no-op.
    finalize: Optional[str] = field(default=None, metadata={"fetchable": False})
    copy_grants: bool = field(default=None, metadata={"fetchable": False})
    comment: Optional[str] = None
    after: Optional[list[str]] = None
    when: Optional[str] = None
    state: TaskState = TaskState.SUSPENDED
    as_: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        if self.as_ is not None:
            self.as_ = self.as_.strip()

        if self.warehouse is not None and self.user_task_managed_initial_warehouse_size is not None:
            raise ValueError("user_task_managed_initial_warehouse_size cannot be set if warehouse is set")
        if self.warehouse is None and self.user_task_managed_initial_warehouse_size is None:
            # Default to serverless task
            self.user_task_managed_initial_warehouse_size = WarehouseSize.MEDIUM

        if self.suspend_task_after_num_failures is None and len(self.after or []) == 0:
            # Set default only if non-child task
            self.suspend_task_after_num_failures = 10

        if self.after is not None:
            self.after = [convert_to_resource(Task, task) for task in self.after]


class Task(NamedResource, Resource):
    """
    Description:
        Represents a scheduled task in Snowflake that performs a specified SQL statement at a recurring interval.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-task

    Fields:
        warehouse (string or Warehouse): The warehouse used by the task.
        user_task_managed_initial_warehouse_size (string or WarehouseSize): The initial warehouse size when the task is managed by the user. Defaults to None.
        schedule (string): The schedule on which the task runs.
        config (string): Configuration settings for the task.
        allow_overlapping_execution (bool): Whether the task can have overlapping executions.
        user_task_timeout_ms (int): The timeout in milliseconds after which the task is aborted.
        suspend_task_after_num_failures (int): The number of consecutive failures after which the task is suspended.
        error_integration (string): The notification integration used for error handling.
        success_integration (string): The notification integration used for success notifications.
        serverless_task_min_statement_size (string or WarehouseSize): Minimum warehouse size for a serverless task.
        serverless_task_max_statement_size (string or WarehouseSize): Maximum warehouse size for a serverless task.
        task_auto_retry_attempts (int): Number of automatic retry attempts for a failed task run.
        user_task_minimum_trigger_interval_in_seconds (int): Minimum interval between triggered task executions, in seconds.
        target_completion_interval (string): Target completion time for the task, e.g. "1 minutes".
        finalize (string): Name of the root task this task finalizes. Set at creation only.
        copy_grants (bool): Whether to copy grants from the referenced objects.
        comment (string): A comment for the task.
        after (list): A list of tasks that must be completed before this task runs.
        when (string): A conditional expression that determines when the task runs.
        as_ (string): The SQL statement that the task executes.
        state (string or TaskState, required): The initial state of the task. Defaults to SUSPENDED.

    Python:

        ```python
        task = Task(
            name="some_task",
            warehouse="some_warehouse",
            schedule="USING CRON 0 9 * * * UTC",
            state="SUSPENDED",
            as_="SELECT 1"
        )
        ```

    Yaml:

        ```yaml
        tasks:
          - name: some_task
            warehouse: some_warehouse
            schedule: "USING CRON 0 9 * * * UTC"
            state: SUSPENDED
            as_: |
                SELECT 1
        ```
    """

    resource_type = ResourceType.TASK
    props = Props(
        warehouse=IdentifierProp("warehouse"),
        user_task_managed_initial_warehouse_size=EnumProp("user_task_managed_initial_warehouse_size", WarehouseSize),
        schedule=StringProp("schedule"),
        config=StringProp("config"),
        allow_overlapping_execution=BoolProp("allow_overlapping_execution"),
        user_task_timeout_ms=IntProp("user_task_timeout_ms"),
        suspend_task_after_num_failures=IntProp("suspend_task_after_num_failures"),
        error_integration=StringProp("error_integration"),
        success_integration=StringProp("success_integration"),
        serverless_task_min_statement_size=EnumProp("serverless_task_min_statement_size", WarehouseSize),
        serverless_task_max_statement_size=EnumProp("serverless_task_max_statement_size", WarehouseSize),
        task_auto_retry_attempts=IntProp("task_auto_retry_attempts"),
        user_task_minimum_trigger_interval_in_seconds=IntProp("user_task_minimum_trigger_interval_in_seconds"),
        target_completion_interval=StringProp("target_completion_interval"),
        finalize=IdentifierProp("finalize"),
        copy_grants=FlagProp("copy grants"),
        comment=StringProp("comment"),
        after=IdentifierListProp("after", eq=False, parens=False),
        when=ExpressionProp("when"),
        as_=QueryProp("as"),
    )
    scope = SchemaScope()
    spec = _Task

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        warehouse: Warehouse = None,
        user_task_managed_initial_warehouse_size: WarehouseSize = None,
        schedule: str = None,
        config: str = None,
        allow_overlapping_execution: bool = False,
        user_task_timeout_ms: int = None,
        suspend_task_after_num_failures: int = None,
        error_integration: str = None,
        success_integration: str = None,
        serverless_task_min_statement_size: WarehouseSize = None,
        serverless_task_max_statement_size: WarehouseSize = None,
        task_auto_retry_attempts: int = None,
        user_task_minimum_trigger_interval_in_seconds: int = None,
        target_completion_interval: str = None,
        finalize: str = None,
        copy_grants: bool = None,
        comment: str = None,
        after: list[str] = None,
        when: str = None,
        as_: str = None,
        state: TaskState = TaskState.SUSPENDED,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data = _Task(
            name=self._name,
            owner=owner,
            warehouse=warehouse,
            user_task_managed_initial_warehouse_size=user_task_managed_initial_warehouse_size,
            schedule=schedule,
            config=config,
            allow_overlapping_execution=allow_overlapping_execution,
            user_task_timeout_ms=user_task_timeout_ms,
            suspend_task_after_num_failures=suspend_task_after_num_failures,
            error_integration=error_integration,
            success_integration=success_integration,
            serverless_task_min_statement_size=serverless_task_min_statement_size,
            serverless_task_max_statement_size=serverless_task_max_statement_size,
            task_auto_retry_attempts=task_auto_retry_attempts,
            user_task_minimum_trigger_interval_in_seconds=user_task_minimum_trigger_interval_in_seconds,
            target_completion_interval=target_completion_interval,
            finalize=finalize,
            copy_grants=copy_grants,
            comment=comment,
            after=after,
            when=when,
            as_=as_,
            state=state,
        )
        if self._data.after is not None:
            self.requires(*self._data.after)
