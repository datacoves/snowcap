import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import snowflake.connector.errors
from inflection import pluralize

from snowcap.client import UNSUPPORTED_FEATURE
from snowcap.data_provider import fetch_resource, list_resource, populate_account_usage_caches
from snowcap.enums import ResourceType
from snowcap.identifiers import URN, resource_label_for_type
from snowcap.operations.connector import connect
from snowcap.resources.grant import grant_yaml

logger = logging.getLogger("snowcap")

DEFAULT_EXPORT_THREADS = 4

# Snowflake never returns a key pair's public key, only its fingerprint, so an exported
# key pair can't be applied without the operator pasting the key material back in. A
# sweep export is meant to produce config you can plan with, so these are left out of it
# unless they are asked for by name.
EXPORT_ONLY_WHEN_ASKED_FOR = (ResourceType.USER_KEY_PAIR,)


def export_resources(
    session=None,
    include: Optional[list[ResourceType]] = None,
    exclude: Optional[list[ResourceType]] = None,
    threads: int = DEFAULT_EXPORT_THREADS,
    use_account_usage: bool = True,
) -> dict[str, list]:
    if session is None:
        session = connect()

    # Pre-populate ACCOUNT_USAGE caches before parallel fetching to avoid
    # multiple threads hitting the slow ACCOUNT_USAGE access check simultaneously
    if threads > 1 and use_account_usage:
        try:
            populate_account_usage_caches(session)
        except Exception as e:
            logger.debug(f"Could not pre-populate ACCOUNT_USAGE caches: {e}")

    config = {}
    for resource_type in ResourceType:
        if include and resource_type not in include:
            continue
        if exclude and resource_type in exclude:
            continue
        if not include and resource_type in EXPORT_ONLY_WHEN_ASKED_FOR:
            logger.warning(
                f"Skipping {resource_type} because Snowflake does not return the key material an export would need. "
                f"Export them on their own with --resource {resource_label_for_type(resource_type)}."
            )
            continue
        try:
            config.update(export_resource(session, resource_type, threads=threads, use_account_usage=use_account_usage))
        # No list method for resource
        except AttributeError:
            logger.warning(f"Skipping {resource_type} because it has no list method")
            continue
        # Resource not supported
        except snowflake.connector.errors.ProgrammingError as err:
            if err.errno == UNSUPPORTED_FEATURE:
                logger.warning(f"Skipping {resource_type} because it is not supported")
                continue
            else:
                raise
    return config


def _fetch_resource_safe(session, urn: URN):
    """Fetch a resource with retry logic for transient connection errors."""
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            return fetch_resource(session, urn)
        except snowflake.connector.errors.DatabaseError as e:
            # Connection closed error - retry
            if "250003" in str(e.errno) or "Connection" in str(e):
                if attempt < max_retries:
                    logger.debug(f"Retrying fetch for {urn} after connection error (attempt {attempt + 1})")
                    continue
            raise
    return None


def export_resource(
    session,
    resource_type: ResourceType,
    threads: int = DEFAULT_EXPORT_THREADS,
    use_account_usage: bool = True,
) -> dict[str, list]:
    resource_label = resource_label_for_type(resource_type)
    # list_resource forwards only the kwargs a given list_* function declares, so this is a
    # no-op for listers that do not take it.
    resource_names = list_resource(session, resource_label, use_account_usage=use_account_usage)
    if len(resource_names) == 0:
        return {}

    # Build URNs for all resources
    urns = [URN(resource_type, fqn, account_locator="") for fqn in resource_names]

    # For small numbers of resources, don't bother with parallelism
    if len(urns) <= 2 or threads <= 1:
        resources = []
        for urn in urns:
            try:
                resource = fetch_resource(session, urn)
            except Exception as e:
                logger.warning(f"Failed to fetch resource {urn}: {e}")
                continue
            if resource is None:
                logger.warning(f"Found resource {urn} in metadata but failed to fetch")
                continue
            try:
                resources.append(_format_resource_config(urn, resource, resource_type))
            except Exception as e:
                logger.warning(f"Failed to format resource {urn}: {e}")
                continue
        return {pluralize(resource_label): resources}

    # Fetch resources in parallel
    resources = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_urn = {executor.submit(_fetch_resource_safe, session, urn): urn for urn in urns}
        for future in as_completed(future_to_urn):
            urn = future_to_urn[future]
            try:
                resource = future.result()
            except Exception as e:
                # Skip resources that can't be fetched (e.g., unsupported notification integration types)
                # This allows export to continue with other resources in the account
                logger.warning(f"Failed to fetch resource {urn}: {e}")
                continue
            if resource is None:
                logger.warning(f"Found resource {urn} in metadata but failed to fetch")
                continue
            try:
                resources.append(_format_resource_config(urn, resource, resource_type))
            except Exception as e:
                logger.warning(f"Failed to format resource {urn}: {e}")
                continue
    return {pluralize(resource_label): resources}


def _format_resource_config(urn: URN, resource: dict, resource_type: ResourceType) -> dict:
    if resource_type == ResourceType.GRANT:
        return grant_yaml(resource)
    if resource_type == ResourceType.USER_KEY_PAIR:
        # fingerprint and has_expiration are derived from key material and a duration that
        # only config has. Emitting them would produce a block the loader rejects; what an
        # operator has to supply is the public key itself.
        resource = {k: v for k, v in resource.items() if k not in ("fingerprint", "has_expiration")}
        resource["public_key"] = None
    # Sort dict based on key name
    resource = {k: resource[k] for k in sorted(resource)}
    # Put name field at the top of the dict
    first_fields = {}
    if "name" in resource:
        first_fields = {"name": resource.pop("name")}

    if resource_type == ResourceType.SCHEMA:
        first_fields["database"] = str(urn.database().fqn)

    return {**first_fields, **resource}
