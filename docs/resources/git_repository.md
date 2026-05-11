---
description: >-
  A git repository in Snowflake.
---

# GitRepository

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-git-repository) | Snowcap CLI label: `git_repository`

A Git Repository in Snowflake represents an externally hosted Git repository (GitHub,
GitLab, Bitbucket, etc.) that has been registered for use with Snowflake's Git
integration. Once registered, files in the repository can be referenced via stage
syntax in `COPY`, `EXECUTE IMMEDIATE`, and other commands.

A git repository depends on an [APIIntegration](api_integration.md) whose
`api_allowed_prefixes` covers the repository's `origin` URL, and optionally on a
[Secret](generic_secret.md) (for private repos) referenced via `git_credentials`.


## Examples

### YAML

```yaml
git_repositories:
  - name: some_git_repository
    database: some_db
    schema: some_schema
    origin: https://github.com/some-org/some-repo.git
    api_integration: some_api_integration
    git_credentials: some_secret
    comment: Example git repository
```


### Python

```python
git_repository = GitRepository(
    name="some_git_repository",
    database="some_db",
    schema="some_schema",
    origin="https://github.com/some-org/some-repo.git",
    api_integration="some_api_integration",
    git_credentials="some_secret",
    comment="Example git repository",
)
```


## Fields

* `name` (string, required) - The name of the git repository.
* `origin` (string, required) - The URL of the externally hosted Git repository
  (e.g., `https://github.com/some-org/some-repo.git`).
* `api_integration` (string, required) - The name of the API integration object
  Snowflake will use to interact with the repository. The API integration's
  `api_allowed_prefixes` must include the `origin` URL.
* `git_credentials` (string) - The name of a [Secret](generic_secret.md) holding
  credentials for accessing a private repository. Optional for public repos.
* `comment` (string) - A comment for the git repository.
* `owner` (string or [Role](role.md)) - The owner role of the git repository.
  Defaults to `SYSADMIN`.


## Grants

Snowcap supports `READ`, `WRITE`, and `OWNERSHIP` privileges on git repositories:

```yaml
grants:
  - priv: READ
    on: git repository some_db.some_schema.some_git_repository
    to: some_role
```
