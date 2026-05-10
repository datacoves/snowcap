from dataclasses import dataclass

from ..enums import ResourceType
from ..props import (
    IdentifierProp,
    Props,
    StringProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _GitRepository(ResourceSpec):
    name: ResourceName
    origin: str
    api_integration: str
    git_credentials: str = None
    comment: str = None
    owner: RoleRef = "SYSADMIN"


class GitRepository(NamedResource, Resource):
    """
    Description:
        A Git Repository in Snowflake represents an externally hosted Git
        repository (GitHub, GitLab, Bitbucket, etc.) that has been registered
        for use with Snowflake's Git integration. Once registered, files in
        the repository can be referenced via stage syntax in COPY, EXECUTE
        IMMEDIATE, and other commands.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-git-repository

    Fields:
        name (string, required): The name of the git repository.
        origin (string, required): The URL of the externally hosted Git
            repository (e.g., "https://github.com/some-org/some-repo.git").
        api_integration (string, required): The name of the API integration
            object Snowflake will use to interact with the repository. The
            API integration's allowed prefixes must include the origin URL.
        git_credentials (string): The name of the secret holding credentials
            for accessing a private repository. Optional for public repos.
        comment (string): A comment for the git repository.
        owner (string or Role): The owner of the git repository. Defaults to
            SYSADMIN.

    Python:

        ```python
        git_repository = GitRepository(
            name="some_git_repository",
            origin="https://github.com/some-org/some-repo.git",
            api_integration="some_api_integration",
            git_credentials="some_secret",
            comment="some_comment",
            owner="SYSADMIN",
        )
        ```

    Yaml:

        ```yaml
        git_repositories:
          - name: some_git_repository
            origin: https://github.com/some-org/some-repo.git
            api_integration: some_api_integration
            git_credentials: some_secret
            comment: some_comment
            owner: SYSADMIN
        ```
    """

    resource_type = ResourceType.GIT_REPOSITORY
    props = Props(
        origin=StringProp("origin"),
        api_integration=IdentifierProp("api_integration"),
        git_credentials=IdentifierProp("git_credentials"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _GitRepository

    def __init__(
        self,
        name: str,
        origin: str,
        api_integration: str,
        git_credentials: str = None,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _GitRepository = _GitRepository(
            name=self._name,
            origin=origin,
            api_integration=api_integration,
            git_credentials=git_credentials,
            comment=comment,
            owner=owner,
        )
