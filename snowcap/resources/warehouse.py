from dataclasses import dataclass, field, fields
from typing import Optional, Union

from ..enums import AccountEdition, ParseableEnum, ResourceType, WarehouseSize
from ..props import (
    BoolProp,
    EnumProp,
    IdentifierProp,
    IntProp,
    Props,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .resource_monitor import ResourceMonitor
from .role import Role
from .tag import TaggableResource


class WarehouseType(ParseableEnum):
    STANDARD = "STANDARD"
    SNOWPARK_OPTIMIZED = "SNOWPARK-OPTIMIZED"
    ADAPTIVE = "ADAPTIVE"


class WarehouseScalingPolicy(ParseableEnum):
    STANDARD = "STANDARD"
    ECONOMY = "ECONOMY"


class WarehouseGeneration(ParseableEnum):
    GEN1 = "1"
    GEN2 = "2"

    @classmethod
    def synonyms(cls):
        return {
            "GEN1": "GEN1",
            "GEN2": "GEN2",
        }


class WarehouseResourceConstraint(ParseableEnum):
    STANDARD_GEN_1 = "STANDARD_GEN_1"
    STANDARD_GEN_2 = "STANDARD_GEN_2"
    MEMORY_1X = "MEMORY_1X"
    MEMORY_1X_X86 = "MEMORY_1X_X86"
    MEMORY_16X = "MEMORY_16X"
    MEMORY_16X_X86 = "MEMORY_16X_X86"
    MEMORY_64X = "MEMORY_64X"
    MEMORY_64X_X86 = "MEMORY_64X_X86"


STANDARD_RESOURCE_CONSTRAINTS = {
    WarehouseResourceConstraint.STANDARD_GEN_1,
    WarehouseResourceConstraint.STANDARD_GEN_2,
}
SNOWPARK_RESOURCE_CONSTRAINTS = {
    WarehouseResourceConstraint.MEMORY_1X,
    WarehouseResourceConstraint.MEMORY_1X_X86,
    WarehouseResourceConstraint.MEMORY_16X,
    WarehouseResourceConstraint.MEMORY_16X_X86,
    WarehouseResourceConstraint.MEMORY_64X,
    WarehouseResourceConstraint.MEMORY_64X_X86,
}
GENERATION_RESOURCE_CONSTRAINTS = {
    WarehouseGeneration.GEN1: WarehouseResourceConstraint.STANDARD_GEN_1,
    WarehouseGeneration.GEN2: WarehouseResourceConstraint.STANDARD_GEN_2,
}

# Properties that don't apply to ADAPTIVE warehouses (Snowflake computes them automatically).
# Names only: __post_init__ reads each field's real default from dataclasses.fields at
# validation time rather than hand-copying defaults here. "generation" is deliberately
# excluded — the existing generation-vs-STANDARD check already rejects it for ADAPTIVE.
ADAPTIVE_UNSUPPORTED_FIELDS = (
    "warehouse_size",
    "min_cluster_count",
    "max_cluster_count",
    "scaling_policy",
    "auto_suspend",
    "auto_resume",
    "initially_suspended",
    "enable_query_acceleration",
    "query_acceleration_max_scale_factor",
    "resource_constraint",
)


@dataclass(unsafe_hash=True)
class _Warehouse(ResourceSpec):
    name: ResourceName
    owner: Role = "SYSADMIN"
    warehouse_type: WarehouseType = WarehouseType.STANDARD
    warehouse_size: WarehouseSize = WarehouseSize.XSMALL
    generation: Optional[WarehouseGeneration] = None
    resource_constraint: Optional[WarehouseResourceConstraint] = None
    max_query_performance_level: Optional[WarehouseSize] = None
    max_cluster_count: int = field(
        default=1,
        metadata={"edition": {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}},
    )
    min_cluster_count: int = field(
        default=1,
        metadata={"edition": {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}},
    )
    scaling_policy: WarehouseScalingPolicy = field(
        default=WarehouseScalingPolicy.STANDARD,
        metadata={"edition": {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}},
    )
    auto_suspend: int = 600
    auto_resume: bool = True
    initially_suspended: bool = field(default=False, metadata={"fetchable": False})
    resource_monitor: ResourceMonitor = None
    comment: str = None
    enable_query_acceleration: Optional[bool] = field(
        default=None,
        metadata={"edition": {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}},
    )
    query_acceleration_max_scale_factor: Optional[int] = field(
        default=None,
        metadata={"edition": {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}},
    )
    max_concurrency_level: Optional[int] = None  # Uses Snowflake default (8) if not specified
    statement_queued_timeout_in_seconds: Optional[int] = None  # Uses Snowflake default (0) if not specified
    statement_timeout_in_seconds: Optional[int] = None  # Uses Snowflake default (172800) if not specified

    def __post_init__(self):
        super().__post_init__()

        if self.generation is not None and self.warehouse_type != WarehouseType.STANDARD:
            raise ValueError("generation is only valid for STANDARD warehouses")

        if self.resource_constraint is not None:
            if self.warehouse_type == WarehouseType.STANDARD:
                if self.resource_constraint not in STANDARD_RESOURCE_CONSTRAINTS:
                    raise ValueError("STANDARD warehouses only support STANDARD_GEN_1 or STANDARD_GEN_2")
            elif self.warehouse_type == WarehouseType.SNOWPARK_OPTIMIZED:
                if self.resource_constraint not in SNOWPARK_RESOURCE_CONSTRAINTS:
                    raise ValueError("SNOWPARK-OPTIMIZED warehouses only support MEMORY_* resource constraints")

        if self.generation is not None and self.resource_constraint is not None:
            expected_constraint = GENERATION_RESOURCE_CONSTRAINTS[self.generation]
            if self.resource_constraint != expected_constraint:
                raise ValueError("generation and resource_constraint must describe the same warehouse generation")

        uses_standard_gen2 = (
            self.generation == WarehouseGeneration.GEN2
            or self.resource_constraint == WarehouseResourceConstraint.STANDARD_GEN_2
        )
        if uses_standard_gen2 and self.warehouse_size in {WarehouseSize.X5LARGE, WarehouseSize.X6LARGE}:
            raise ValueError("Gen2 standard warehouses do not support X5LARGE or X6LARGE sizes")

        if self.warehouse_type == WarehouseType.ADAPTIVE:
            for field_name in ADAPTIVE_UNSUPPORTED_FIELDS:
                if getattr(self, field_name) != _ADAPTIVE_FIELD_DEFAULTS[field_name]:
                    raise ValueError(f"{field_name} does not apply to ADAPTIVE warehouses")
                setattr(self, field_name, None)

        if self.max_query_performance_level is not None:
            if self.warehouse_type != WarehouseType.ADAPTIVE:
                raise ValueError("max_query_performance_level is only valid for ADAPTIVE warehouses")
            if self.max_query_performance_level in {WarehouseSize.X5LARGE, WarehouseSize.X6LARGE}:
                raise ValueError("max_query_performance_level supports XSMALL through X4LARGE")


# Real dataclass defaults for the adaptive-inapplicable fields, computed once from
# dataclasses.fields (the single source of truth) instead of rebuilding this dict on
# every ADAPTIVE warehouse construction.
_ADAPTIVE_FIELD_DEFAULTS = {f.name: f.default for f in fields(_Warehouse) if f.name in ADAPTIVE_UNSUPPORTED_FIELDS}


class Warehouse(NamedResource, TaggableResource, Resource):
    """
    Description:
        A virtual warehouse, often referred to simply as a "warehouse", is a cluster of compute resources in Snowflake. It provides the necessary CPU, memory, and temporary storage to execute SQL SELECT statements, perform DML operations such as INSERT, UPDATE, DELETE, and manage data loading and unloading.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-warehouse

    Fields:
        name (string, required): The name of the warehouse.
        owner (string): The owner of the warehouse. Defaults to "SYSADMIN".
        warehouse_type (string or WarehouseType): The type of the warehouse: STANDARD, SNOWPARK-OPTIMIZED, or ADAPTIVE. Defaults to STANDARD. ADAPTIVE warehouses do not support warehouse_size, min_cluster_count, max_cluster_count, scaling_policy, auto_suspend, auto_resume, initially_suspended, enable_query_acceleration, query_acceleration_max_scale_factor, resource_constraint, or generation.
        warehouse_size (string or WarehouseSize): The size of the warehouse which defines the compute and storage capacity.
        generation (string or WarehouseGeneration): The standard warehouse generation, either "1" or "2".
        resource_constraint (string or WarehouseResourceConstraint): The warehouse resource constraint, either STANDARD_GEN_1/2 for standard warehouses or MEMORY_* for Snowpark-optimized warehouses.
        max_query_performance_level (string or WarehouseSize): The maximum size an ADAPTIVE warehouse may scale to: XSMALL, SMALL, MEDIUM, LARGE, XLARGE, XXLARGE, XXXLARGE, or X4LARGE. Only valid for ADAPTIVE warehouses; Snowflake defaults to XLARGE if omitted.
        max_cluster_count (int): The maximum number of clusters for the warehouse.
        min_cluster_count (int): The minimum number of clusters for the warehouse.
        scaling_policy (string or WarehouseScalingPolicy): The policy that defines how the warehouse scales.
        auto_suspend (int): The time in seconds of inactivity after which the warehouse is automatically suspended.
        auto_resume (bool): Whether the warehouse should automatically resume when queries are submitted.
        initially_suspended (bool): Whether the warehouse should start in a suspended state.
        resource_monitor (string or ResourceMonitor): The resource monitor that tracks the warehouse's credit usage and other metrics.
        comment (string): A comment about the warehouse.
        enable_query_acceleration (bool): Whether query acceleration is enabled to improve performance. If omitted, Snowflake's default applies.
        query_acceleration_max_scale_factor (int): The maximum scale factor for query acceleration. If omitted, Snowflake's default applies.
        max_concurrency_level (int): The maximum number of concurrent queries that the warehouse can handle.
        statement_queued_timeout_in_seconds (int): The time in seconds a statement can be queued before it times out.
        statement_timeout_in_seconds (int): The time in seconds a statement can run before it times out.
        tags (dict): Tags for the warehouse.

    Python:

        ```python
        warehouse = Warehouse(
            name="some_warehouse",
            owner="SYSADMIN",
            warehouse_type="STANDARD",
            warehouse_size="XSMALL",
            generation="2",
            resource_constraint="STANDARD_GEN_2",
            max_cluster_count=10,
            min_cluster_count=1,
            scaling_policy="STANDARD",
            auto_suspend=600,
            auto_resume=True,
            initially_suspended=False,
            resource_monitor=None,
            comment="This is a test warehouse",
            enable_query_acceleration=False,
            query_acceleration_max_scale_factor=1,
            max_concurrency_level=8,
            statement_queued_timeout_in_seconds=0,
            statement_timeout_in_seconds=172800,
            tags={"env": "test"},
        )
        ```

        An adaptive warehouse sets max_query_performance_level instead of warehouse_size and cluster/scaling properties:
        ```python
        adaptive_warehouse = Warehouse(
            name="some_adaptive_warehouse",
            warehouse_type="ADAPTIVE",
            max_query_performance_level="LARGE",
        )
        ```

    Yaml:

        ```yaml
        warehouses:
          - name: some_warehouse
            owner: SYSADMIN
            warehouse_type: STANDARD
            warehouse_size: XSMALL
            generation: "2"
            resource_constraint: STANDARD_GEN_2
            max_cluster_count: 10
            min_cluster_count: 1
            scaling_policy: STANDARD
            auto_suspend: 600
            auto_resume: true
            initially_suspended: false
            resource_monitor: null
            comment: This is a test warehouse
            enable_query_acceleration: false
            query_acceleration_max_scale_factor: 1
            max_concurrency_level: 8
            statement_queued_timeout_in_seconds: 0
            statement_timeout_in_seconds: 172800
            tags:
              env: test
        ```

        An adaptive warehouse in yaml:
        ```yaml
        warehouses:
          - name: some_adaptive_warehouse
            warehouse_type: ADAPTIVE
            max_query_performance_level: LARGE
        ```
    """

    resource_type = ResourceType.WAREHOUSE
    props = Props(
        _start_token="WITH",
        warehouse_type=EnumProp("warehouse_type", WarehouseType, quoted=True),
        warehouse_size=EnumProp("warehouse_size", WarehouseSize),
        generation=EnumProp("GENERATION", WarehouseGeneration, quoted=True),
        resource_constraint=EnumProp("RESOURCE_CONSTRAINT", WarehouseResourceConstraint),
        max_query_performance_level=EnumProp("MAX_QUERY_PERFORMANCE_LEVEL", WarehouseSize),
        max_cluster_count=IntProp("max_cluster_count"),
        min_cluster_count=IntProp("min_cluster_count"),
        scaling_policy=EnumProp("scaling_policy", WarehouseScalingPolicy),
        auto_suspend=IntProp("auto_suspend", alt_tokens=["NULL"]),
        auto_resume=BoolProp("auto_resume"),
        initially_suspended=BoolProp("initially_suspended"),
        resource_monitor=IdentifierProp("resource_monitor"),
        comment=StringProp("comment"),
        enable_query_acceleration=BoolProp("enable_query_acceleration"),
        query_acceleration_max_scale_factor=IntProp("query_acceleration_max_scale_factor"),
        max_concurrency_level=IntProp("max_concurrency_level"),
        statement_queued_timeout_in_seconds=IntProp("statement_queued_timeout_in_seconds"),
        statement_timeout_in_seconds=IntProp("statement_timeout_in_seconds"),
        tags=TagsProp(),
    )
    scope = AccountScope()
    spec = _Warehouse

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        warehouse_type: str = "STANDARD",
        warehouse_size: str = "XSMALL",
        generation: str = None,
        resource_constraint: str = None,
        max_query_performance_level: str = None,
        max_cluster_count: int = 1,
        min_cluster_count: int = 1,
        scaling_policy: str = "STANDARD",
        auto_suspend: int = 600,
        auto_resume: bool = True,
        initially_suspended: bool = False,
        resource_monitor: Union[ResourceMonitor, str, None] = None,
        comment: str = None,
        enable_query_acceleration: Optional[bool] = None,
        query_acceleration_max_scale_factor: Optional[int] = None,
        max_concurrency_level: int = None,
        statement_queued_timeout_in_seconds: int = None,
        statement_timeout_in_seconds: int = None,
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Warehouse = _Warehouse(
            name=self._name,
            owner=owner,
            warehouse_type=warehouse_type,
            warehouse_size=warehouse_size,
            generation=generation,
            resource_constraint=resource_constraint,
            max_query_performance_level=max_query_performance_level,
            max_cluster_count=max_cluster_count,
            min_cluster_count=min_cluster_count,
            scaling_policy=scaling_policy,
            auto_suspend=auto_suspend,
            auto_resume=auto_resume,
            initially_suspended=initially_suspended,
            resource_monitor=resource_monitor,
            comment=comment,
            enable_query_acceleration=enable_query_acceleration,
            query_acceleration_max_scale_factor=query_acceleration_max_scale_factor,
            max_concurrency_level=max_concurrency_level,
            statement_queued_timeout_in_seconds=statement_queued_timeout_in_seconds,
            statement_timeout_in_seconds=statement_timeout_in_seconds,
        )
        self.set_tags(tags)
