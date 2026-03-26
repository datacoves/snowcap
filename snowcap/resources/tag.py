from dataclasses import dataclass
from typing import Optional

from ..enums import AccountEdition, ResourceType, TagPropagation
from ..identifiers import FQN, parse_FQN
from ..props import Props, StringListProp, StringProp, EnumProp, TagOnConflictProp
from ..resource_name import ResourceName
from ..resource_tags import ResourceTags
from ..role_ref import RoleRef
from ..scope import AccountScope, SchemaScope
from .resource import NamedResource, Resource, ResourcePointer, ResourceSpec


@dataclass(unsafe_hash=True)
class _Tag(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    comment: str = None
    allowed_values: list = None
    propagate: TagPropagation = None
    on_conflict: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.allowed_values is not None:
            self.allowed_values.sort()


class Tag(NamedResource, Resource):
    """
    Description:
        Represents a tag in Snowflake, which can be used to label various resources for better management and categorization.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-tag

    Fields:
        name (string, required): The name of the tag.
        allowed_values (list): A list of allowed values for the tag.
        propagate (string): Configures automatic tag propagation. Values:
            ON_DEPENDENCY_AND_DATA_MOVEMENT, ON_DEPENDENCY, ON_DATA_MOVEMENT.
        on_conflict (string): Behavior when propagated tag values conflict.
            Use ALLOWED_VALUES_SEQUENCE or a custom string.
        comment (string): A comment or description for the tag.

    Python:

        ```python
        tag = Tag(
            name="cost_center",
            allowed_values=["finance", "engineering", "sales"],
            comment="This is a sample tag",
        )

        # With auto-propagation
        tag = Tag(
            name="auto_pii",
            allowed_values=["sensitive", "highly_sensitive"],
            propagate="ON_DEPENDENCY_AND_DATA_MOVEMENT",
            on_conflict="ALLOWED_VALUES_SEQUENCE",
        )
        ```

    Yaml:

        ```yaml
        tags:
          - name: cost_center
            comment: This is a sample tag
            allowed_values:
              - finance
              - engineering
              - sales

          # With auto-propagation
          - name: auto_pii
            allowed_values:
              - sensitive
              - highly_sensitive
            propagate: ON_DEPENDENCY_AND_DATA_MOVEMENT
            on_conflict: ALLOWED_VALUES_SEQUENCE
        ```
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.TAG
    props = Props(
        allowed_values=StringListProp("allowed_values", eq=False, parens=False),
        propagate=EnumProp("propagate", TagPropagation),
        on_conflict=TagOnConflictProp(),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _Tag

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        comment: str = None,
        allowed_values: list = None,
        propagate: str = None,
        on_conflict: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Tag = _Tag(
            name=self._name,
            owner=owner,
            comment=comment,
            allowed_values=allowed_values,
            propagate=propagate,
            on_conflict=on_conflict,
        )


@dataclass(unsafe_hash=True)
class _TagReference(ResourceSpec):
    object_name: str
    object_domain: ResourceType
    tags: ResourceTags


class TagReference(Resource):

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.TAG_REFERENCE
    props = Props()
    scope = AccountScope()
    spec = _TagReference

    def __init__(
        self,
        object_name: str,
        object_domain: str,
        tags: dict[str, str],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _TagReference = _TagReference(
            object_name=object_name,
            object_domain=object_domain,
            tags=tags,
        )
        for tag in tags.keys():
            tag_ptr = ResourcePointer(name=tag, resource_type=ResourceType.TAG)
            self.requires(tag_ptr)

    @property
    def fqn(self):
        return tag_reference_fqn(self._data)

    @property
    def tags(self) -> Optional[ResourceTags]:
        return self._data.tags


def tag_reference_fqn(data: _TagReference) -> FQN:
    return FQN(
        name=ResourceName(data.object_name),
        params={
            "domain": str(data.object_domain),
        },
    )


def tag_reference_for_resource(resource: Resource, tags: dict[str, str]) -> TagReference:
    return TagReference(
        object_name=str(resource.fqn),
        object_domain=resource.resource_type,
        tags=tags,
    )


@dataclass(unsafe_hash=True)
class _TagMaskingPolicyReference(ResourceSpec):
    tag_name: str
    masking_policy_name: str


class TagMaskingPolicyReference(Resource):
    """
    Description:
        Associates a masking policy with a tag. When a tag with an associated masking policy
        is applied to a column, the masking policy is automatically enforced on that column.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/alter-tag

    Fields:
        tag_name (string, required): The fully qualified name of the tag (e.g., MY_DB.MY_SCHEMA.PII).
        masking_policy_name (string, required): The fully qualified name of the masking policy
            (e.g., MY_DB.MY_SCHEMA.MASK_PII).

    Python:

        ```python
        ref = TagMaskingPolicyReference(
            tag_name="MY_DB.MY_SCHEMA.PII",
            masking_policy_name="MY_DB.MY_SCHEMA.MASK_PII",
        )
        ```

    Yaml:

        ```yaml
        tag_masking_policy_references:
          - tag_name: MY_DB.MY_SCHEMA.PII
            masking_policy_name: MY_DB.MY_SCHEMA.MASK_PII
        ```
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.TAG_MASKING_POLICY_REFERENCE
    props = Props()
    scope = AccountScope()
    spec = _TagMaskingPolicyReference

    def __init__(
        self,
        tag_name: str,
        masking_policy_name: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _TagMaskingPolicyReference = _TagMaskingPolicyReference(
            tag_name=tag_name,
            masking_policy_name=masking_policy_name,
        )
        # Establish dependencies on both the tag and the masking policy
        tag_ptr = ResourcePointer(name=tag_name, resource_type=ResourceType.TAG)
        masking_policy_ptr = ResourcePointer(name=masking_policy_name, resource_type=ResourceType.MASKING_POLICY)
        self.requires(tag_ptr)
        self.requires(masking_policy_ptr)

    @property
    def fqn(self):
        return tag_masking_policy_reference_fqn(self._data)

    @property
    def tag_name(self) -> str:
        return self._data.tag_name

    @property
    def masking_policy_name(self) -> str:
        return self._data.masking_policy_name


def tag_masking_policy_reference_fqn(data: _TagMaskingPolicyReference) -> FQN:
    tag_fqn = parse_FQN(data.tag_name)
    return FQN(
        name=tag_fqn.name,
        database=tag_fqn.database,
        schema=tag_fqn.schema,
        params={
            "masking_policy": data.masking_policy_name,
        },
    )


class TaggableResource:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._tags: Optional[ResourceTags] = None

    def set_tags(self, tags: Optional[dict[str, str]]):
        if tags is None:
            return
        if self._tags is None:
            self._tags = ResourceTags(tags)
        else:
            raise ValueError("Tags cannot be set on a resource that already has tags")

    def create_tag_reference(self):
        if self._tags is None:
            return None
        ref = tag_reference_for_resource(self, self._tags)
        ref.requires(self)
        return ref

    @property
    def tags(self) -> Optional[ResourceTags]:
        return self._tags
