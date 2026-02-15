# Changelog

All notable changes to snowcap will be documented in this file.

## [1.0.0] - 2025-02-14

### Overview

This is the first major release of **snowcap**, a fork of [Titan Core](https://github.com/Titan-Systems/titan) by Datacoves. The original project appeared unmaintained, so we've forked it to continue active development, fix bugs, and add new features.

Snowcap is a Snowflake infrastructure-as-code tool that lets you manage all Snowflake resources using YAML or Python configuration.

### Highlights

- **Renamed from Titan Core to snowcap** - Fresh identity, same powerful functionality
- **1656 tests** with 72% code coverage (up from ~400 tests)
- **60 resource types** fully documented
- **Comprehensive documentation** at [datacoves.github.io/snowcap](https://datacoves.github.io/snowcap)

### New Features

#### Multithreading Support
- Added parallel execution for improved performance when managing large numbers of resources
- Role switching and commands execute concurrently where possible
- Significant speed improvements for accounts with hundreds of resources

#### Enhanced Grants System
- Support for multiple privileges in a single grant definition
- Grant synchronization mode with `sync_resources` parameter
- Fixed bug with `ALL` privilege type grants
- Improved handling of grants on non-database/schema resources

#### Streamlit Resources
- Full support for Streamlit app resources in Snowflake
- Create, update, and manage Streamlit applications via configuration

#### Imported Databases
- Support for managing imported databases (databases shared from other accounts)
- Proper handling of cross-account resource references

#### Jinja Templating Improvements
- Added `parent` attribute support in Jinja templates
- `for_each` now properly converts strings back to integers when necessary
- Enhanced template variable handling

### Bug Fixes

- Fixed handling of collection types in resource specifications
- Fixed quoted names handling for identifiers with special characters
- Fixed resource type recognition in various edge cases
- Fixed levels computation when running commands in parallel
- Fixed refs check when resources are in the manifest but not in the database
- Fixed parallelism issues in sync mode
- Fixed role grant fetching from database
- Database creation now correctly uses ACCOUNTADMIN role
- Fixed system role modification warnings

### Improvements

- Better error handling throughout the codebase
- Caching and parallel execution of resource listing operations
- Improved resource comparison logic
- Loosened snowflake-connector-python version requirements for better compatibility
- Enhanced documentation for all resource types

### Resource Coverage

snowcap supports 60+ Snowflake resource types including:

- **Compute**: Warehouses, Compute Pools
- **Storage**: Databases, Schemas, Tables, Views, Dynamic Tables, Materialized Views
- **Security**: Roles, Users, Grants, Network Policies, Secrets
- **Data Loading**: Stages, Pipes, Streams, Tasks
- **Integrations**: Storage Integrations, Notification Integrations, External Access Integrations
- **Governance**: Tags, Masking Policies, Row Access Policies, Aggregation Policies
- **Applications**: Streamlit, Functions, Procedures, UDFs

### Testing Infrastructure

- Comprehensive unit test suite with 1656 tests
- Integration tests for Snowflake Standard and Enterprise editions
- Static test resources for reproducible integration testing
- 72% code coverage with XML reporting
- Parallel test execution with pytest-xdist

### Migration from Titan Core

If you're migrating from Titan Core:

1. Update your package: `pip install snowcap` (instead of `titan-core`)
2. Update imports: `from snowcap import ...` (instead of `from titan import ...`)
3. Update CLI commands: `snowcap plan/apply` (instead of `titan plan/apply`)
4. Your existing YAML configurations should work without changes

### Requirements

- Python 3.10 or higher
- snowflake-connector-python >= 3.12.3
- snowflake-snowpark-python >= 1.24.0

### Acknowledgments

This project builds on the excellent foundation created by the original Titan Systems team. We're grateful for their work in creating and open-sourcing this project under the Apache 2.0 license.

### Links

- Documentation: [datacoves.github.io/snowcap](https://datacoves.github.io/snowcap)
- PyPI: [pypi.org/project/snowcap](https://pypi.org/project/snowcap)
- GitHub: [github.com/datacoves/snowcap](https://github.com/datacoves/snowcap)
- Issues: [github.com/datacoves/snowcap/issues](https://github.com/datacoves/snowcap/issues)
