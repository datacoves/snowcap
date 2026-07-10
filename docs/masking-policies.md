# Tag-Based Masking Policies

This guide describes a recommended pattern for implementing column-level data masking in Snowflake using Snowcap. The pattern uses **tags** to automatically apply masking policies to columns, providing scalable data protection without manually attaching policies to individual columns.

## Overview

Tag-based masking separates the concerns of data protection:

1. **Tags** define the classification (e.g., PII, CONFIDENTIAL)
2. **Masking Policies** define the transformation logic for each data type
3. **Tag-to-Policy References** connect them together
4. **Columns** are tagged, and masking is automatically enforced

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              COLUMNS                                         │
│                  email, phone, account_balance, birth_date                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (tagged with)
┌─────────────────────────────────────────────────────────────────────────────┐
│                                TAG                                           │
│                                PII                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (has policies for each data type)
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MASKING POLICIES                                     │
│  MASK_PII_STRING, MASK_PII_NUMBER, MASK_PII_FLOAT, MASK_PII_DATE, MASK_PII_TIMESTAMP │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (checks role for visibility)
┌─────────────────────────────────────────────────────────────────────────────┐
│                          UNMASK ROLE                                         │
│                          Z_UNMASK__PII                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

When you tag a column as `PII`, Snowflake automatically applies the correct masking policy based on the column's data type—no need to worry about whether it's a string, number, or date.

## Why Tag-Based Masking?

| Approach | Pros | Cons |
|----------|------|------|
| **Direct column masking** | Simple for few columns | Doesn't scale; must attach policy to each column |
| **Tag-based masking** | Scalable; tag once, mask everywhere | Requires initial setup |

With tag-based masking:

- Tag a column as `PII` and masking is automatically enforced
- Add new tables with PII columns—just tag them, no policy changes needed
- Change masking logic in one place, applies to all tagged columns

## Recommended Approach

The options below trade off against each other, so here is the default we recommend. Start here and deviate only when you have a specific reason.

1. **Define governance objects with Snowcap.** Create the tags, the per-data-type masking policies, the tag-to-policy references, and the unmask roles as shown in Steps 1 through 6. This is the foundation for every option.

2. **Tag known-sensitive raw data at load time** ([Option 1](#option-1-during-data-load-dlt)). Protect PII the moment it lands, before dbt or any analyst can read it. The loader role tags but does not get the unmask role, so it can move the data without seeing it.

3. **In dbt, tag at creation time using contracts and constraints** ([Option 4](#option-4-at-creation-time-dbt-contracts-and-constraints)), not post-hooks. The tag is applied inside the `CREATE TABLE`, so there is no window where fresh PII sits unmasked and no partial-run failure that leaves a table untagged. Fall back to a post-hook ([Option 2](#option-2-during-transformation-dbt)) only for view columns produced by a transformation, which constraints cannot cover.

4. **For schemas that are sensitive end to end, set the tag on the schema and go fail-closed** ([Default Masking at the Schema Level](#fail-closed-default-masking-at-the-schema-level)). New columns and new tables are then masked by default, and you allowlist reviewed non-sensitive columns instead of remembering to protect each sensitive one. Use column-level tagging for schemas that are only partly sensitive.

5. **Set `DEFAULT_SECONDARY_ROLES = ()` for masked users.** Masking checks `IS_ROLE_IN_SESSION()`, and active secondary roles can bring an unmask role into session and silently defeat the policy. See [Verifying Masking Works](#verifying-masking-works).

!!! tip "The short version"
    Tag raw data at load, tag dbt tables at creation time (not in a post-hook), default whole sensitive schemas to fail-closed, and turn off secondary roles for masked users. The post-hook is a fallback for derived view columns, not the default.

## Role Naming Convention

!!! info "Recommended, not required"
    This naming convention aligns with the [RBAC pattern](role-based-access-control.md). Use whatever works for your organization.

| Role Type | Naming Pattern | Example |
|-----------|----------------|---------|
| Unmask role | `z_unmask__<classification>` | `z_unmask__pii` |

The `z_` prefix keeps these roles sorted at the bottom of role lists, making functional roles more visible.

## Configuration Examples

### Step 1: Create the Governance Database and Schema

First, create a dedicated location for your governance objects:

```yaml
# resources/databases.yml
databases:
  - name: governance
    owner: sysadmin
```

```yaml
# resources/schemas.yml
schemas:
  - name: governance.tags
    managed_access: true
  - name: governance.policies
    managed_access: true
```

### Step 2: Create Tags

Define tags that represent your data classifications, along with a role to grant the `APPLY` privilege:

```yaml
# resources/tags.yml
tags:
  - name: governance.tags.pii
    comment: Personally Identifiable Information
    propagate: ON_DATA_MOVEMENT

  - name: governance.tags.confidential
    comment: Confidential business data
    propagate: ON_DATA_MOVEMENT

roles:
  - name: z_tag__apply__pii
  - name: z_tag__apply__confidential

grants:
  - priv: APPLY
    on: tag governance.tags.pii
    to: z_tag__apply__pii

  - priv: APPLY
    on: tag governance.tags.confidential
    to: z_tag__apply__confidential
```

!!! tip "Automatic tag propagation"
    Setting `propagate: ON_DATA_MOVEMENT` ensures that when data flows from tagged columns
    to new tables or views (e.g., via CTAS or INSERT...SELECT), the tags are automatically
    propagated. This means downstream objects inherit the masking protection without manual
    re-tagging. For masking tags, the `on_conflict` parameter is optional since the tag value
    doesn't affect masking behavior—only the tag's presence matters.

### Step 3: Create Unmask Roles

Create roles that grant the ability to see unmasked data:

```yaml
# resources/roles__unmasking.yml
roles:
  - name: z_unmask__pii
    comment: Grants access to unmasked PII data

  - name: z_unmask__confidential
    comment: Grants access to unmasked confidential data
```

### Step 4: Create Masking Policies

Create a masking policy for each data type. All policies check for the same unmask role:

```yaml
# resources/masking_policies.yml
masking_policies:
  # String/VARCHAR columns
  - name: governance.policies.mask_pii_string
    args:
      - name: val
        data_type: VARCHAR
    returns: VARCHAR
    body: |-
      CASE
        WHEN IS_ROLE_IN_SESSION('Z_UNMASK__PII') THEN val
        ELSE '***MASKED***'
      END
    comment: Masks PII string data

  # Numeric columns
  - name: governance.policies.mask_pii_number
    args:
      - name: val
        data_type: NUMBER
    returns: NUMBER
    body: |-
      CASE
        WHEN IS_ROLE_IN_SESSION('Z_UNMASK__PII') THEN val
        ELSE NULL
      END
    comment: Masks PII numeric data

  # Float columns
  - name: governance.policies.mask_pii_float
    args:
      - name: val
        data_type: FLOAT
    returns: FLOAT
    body: |-
      CASE
        WHEN IS_ROLE_IN_SESSION('Z_UNMASK__PII') THEN val
        ELSE NULL
      END
    comment: Masks PII float data

  # Date columns
  - name: governance.policies.mask_pii_date
    args:
      - name: val
        data_type: DATE
    returns: DATE
    body: |-
      CASE
        WHEN IS_ROLE_IN_SESSION('Z_UNMASK__PII') THEN val
        ELSE NULL
      END
    comment: Masks PII date data

  # Timestamp columns
  - name: governance.policies.mask_pii_timestamp
    args:
      - name: val
        data_type: TIMESTAMP_NTZ
    returns: TIMESTAMP_NTZ
    body: |-
      CASE
        WHEN IS_ROLE_IN_SESSION('Z_UNMASK__PII') THEN val
        ELSE NULL
      END
    comment: Masks PII timestamp data
```

### Step 5: Associate All Policies with the Tag

Attach all masking policies to the same tag. Snowflake automatically applies the correct policy based on the column's data type:

```yaml
# resources/tag_masking_policies.yml
tag_masking_policy_references:
  - tag_name: governance.tags.pii
    masking_policy_name: governance.policies.mask_pii_string

  - tag_name: governance.tags.pii
    masking_policy_name: governance.policies.mask_pii_number

  - tag_name: governance.tags.pii
    masking_policy_name: governance.policies.mask_pii_float

  - tag_name: governance.tags.pii
    masking_policy_name: governance.policies.mask_pii_date

  - tag_name: governance.tags.pii
    masking_policy_name: governance.policies.mask_pii_timestamp
```

!!! tip "One tag, multiple policies"
    When you tag a VARCHAR column as `PII`, the `mask_pii_string` policy applies. When you tag a NUMBER column as `PII`, the `mask_pii_number` policy applies. You don't need to think about data types when tagging—just tag the column as `PII`.

!!! warning "Create policies for all data types you use"
    You need a separate masking policy for each distinct data type in your schema. If a tagged column's data type doesn't have a matching policy, the column will **not** be masked.

    Audit your schema's data types and create policies accordingly. See [Snowflake Data Types](https://docs.snowflake.com/en/sql-reference/data-types){:target="_blank"} for the complete list.

### Step 6: Grant Unmask Roles to Functional Roles

Grant the unmask roles to users or functional roles that need access:

```yaml
# resources/roles__unmasking.yml (continued)
role_grants:
  # Data stewards can see all PII
  - to_role: data_steward
    roles:
      - z_unmask__pii
      - z_unmask__confidential

  # Compliance team can see PII for audits
  - to_role: compliance_auditor
    roles:
      - z_unmask__pii
```

## Complete Directory Structure

```
snowcap/
├── resources/
│   ├── databases.yml
│   ├── schemas.yml
│   ├── warehouses.yml
│   ├── roles__base.yml
│   ├── roles__functional.yml
│   ├── roles__unmasking.yml        # Unmask roles + grants
│   ├── tags.yml                    # Tags + apply roles + grants
│   ├── masking_policies.yml        # Masking policy definitions
│   ├── tag_masking_policies.yml    # Tag-to-policy associations
│   ├── users.yml
│   └── ...
├── plan.sh
└── apply.sh
```

## Applying Tags to Columns

Once your tags and masking policies are deployed with Snowcap, you need to apply tags to columns. There are four approaches:

1. **At load time** - Apply tags immediately after loading data (e.g., with dlt)
2. **At transformation time** - Apply tags during dbt model builds via post-hooks
3. **Manual** - Apply tags directly via SQL for one-off needs
4. **At creation time** - Apply tags inline via dbt contracts and constraints (closes the post-hook timing window)

All four tag individual columns, so they are **fail-open**: an untagged column is unprotected. To flip the default to **fail-closed** (mask everything unless allowlisted), see [Default Masking at the Schema Level](#fail-closed-default-masking-at-the-schema-level).

### Option 1: During Data Load (dlt)

If you're using [dlt](https://dlthub.com/) to load data into Snowflake, you can apply tags immediately after the load completes. This ensures sensitive columns are protected from the moment data lands in Snowflake.

Create a utility function to apply tags:

```python
# load/dlt/utils/datacoves_utils.py
def apply_pii_tag(pipeline, table: str, columns: list[str]):
    """Apply the GOVERNANCE.TAGS.PII tag to specified columns after loading.

    Args:
        pipeline: A dlt pipeline instance with a Snowflake destination.
        table: Table name containing the columns to tag.
        columns: List of column names to apply the PII tag to.
    """
    with pipeline.sql_client() as client:
        for col in columns:
            client.execute_sql(
                f"ALTER TABLE {pipeline.dataset_name}.{table} "
                f"ALTER COLUMN {col} SET TAG GOVERNANCE.TAGS.PII = 'true'"
            )
    print(f"PII tag applied to {table}: {', '.join(columns)}")
```

Then call it after your pipeline runs:

```python
# load/dlt/loans_data.py
import dlt
from utils.datacoves_utils import apply_pii_tag

@dlt.resource(write_disposition="replace")
def personal_loans():
    # ... load logic
    yield df

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="loans",
        destination=dlt.destinations.snowflake(destination_name="datacoves_snowflake"),
        dataset_name="loans"
    )

    load_info = pipeline.run(personal_loans())
    print(load_info)

    # Apply PII tags to sensitive columns immediately after load
    apply_pii_tag(pipeline, "personal_loans", ["addr_state", "annual_inc"])
```

!!! tip "When to use load-time tagging"
    Use this approach when:

    - You want columns protected immediately upon load
    - The sensitive columns in the source are known
    - Users have access to the loaded data (not just raw/staging layers)

#### Grant permissions to the loader role

The role that runs dlt needs access to the governance database/schema and the `APPLY` privilege on the tag. Grant these to your loader role:

```yaml
# resources/roles__functional.yml
role_grants:
  - to_role: loader
    roles:
      # Access to governance database and schemas where tags are defined
      - z_db__governance
      - z_schemas__db__governance

      # Permission to apply the PII tag
      - z_tag__apply__pii
```

!!! note "Loader role cannot see masked data"
    The loader role applies tags but typically should **not** have the `z_unmask__pii` role. This means the loader cannot read the sensitive data it just loaded—it can only write and tag it. This is intentional: the loader doesn't need to see the data, just move it securely into Snowflake.

### Option 2: During Transformation (dbt)

A common approach is to apply tags during dbt model builds using post-hooks. This works well when your transformation layer is the primary interface for data consumers.

!!! warning "Understand the post-hook trade-offs"
    Post-hooks apply the tag *after* the table is materialized, using a separate `ALTER TABLE ... SET TAG` statement. This introduces a few risks you should account for:

    - **A brief unmasked window on every build.** The table exists with real data before the post-hook runs. Between materialization and the `ALTER TABLE` that applies the tag, the column holds unmasked PII with no policy attached. The window is usually short, but it exists on every run, not just the first.
    - **`CREATE OR REPLACE` drops the tag each time.** dbt materializes tables with `CREATE OR REPLACE`. That drops the old table and everything attached to it, including tags. The post-hook re-applies the tag on each build, so the protection is only ever as current as the last successful post-hook.
    - **A failed or interrupted run leaves data unmasked.** If the model builds but the post-hook fails (permissions, a typo in the tag name, a cancelled run), the table is left populated and untagged. Nothing masks it until the next successful build.
    - **Post-hooks run on every build.** They re-issue the `ALTER TABLE` even when nothing changed, adding metadata operations to each run.

    If a persistent unmasked window is unacceptable for your data, apply the tag at creation time instead (see [Option 4](#option-4-at-creation-time-dbt-contracts-and-constraints)), or tag at load time (Option 1) so the raw data is protected before dbt ever reads it.

#### Snowcap Macros

Snowcap can generate dbt macros that apply Snowflake tags to tables and columns based on `meta` configuration in your schema.yml files.

#### 1. Generate the macros

Run the following command:

```bash
snowcap generate dbt-macros
```

This will prompt you for:

- **dbt project path** - The path to your dbt project (auto-detected from `DATACOVES__DBT_HOME` or `DBT_HOME` environment variables)
- **Tag database** (default: `GOVERNANCE`) - The database where your tags are defined
- **Tag schema** (default: `TAGS`) - The schema where your tags are defined
- **Policy database** (default: `GOVERNANCE`) - The database where row access policies are defined
- **Policy schema** (default: `POLICIES`) - The schema where row access policies are defined

You can also pass all options directly:

```bash
snowcap generate dbt-macros \
  --dbt-path ./transform \
  --tag-database GOVERNANCE \
  --tag-schema TAGS \
  --policy-database GOVERNANCE \
  --policy-schema POLICIES
```

The command creates `<dbt-path>/macros/snowcap_apply_tags.sql` containing:

- `snowcap_apply_policies()` - Main entry point for post-hook
- `snowcap_apply_masking_tags()` - Applies tags from column-level `meta.masking_tag`
- `snowcap_apply_row_access_policy()` - Applies row access policies from model-level `meta.row_access_policy`

#### 2. Configure dbt_project.yml

Add the variables and post-hook to your `dbt_project.yml`:

```yaml
# dbt_project.yml
vars:
  snowcap_tag_database: GOVERNANCE
  snowcap_tag_schema: TAGS

models:
  your_project:
    +post-hook:
      - "{{ snowcap_apply_policies() }}"
```

#### 3. Define tags in schema.yml using meta

Use the `meta` property to specify which tags to apply:

```yaml
# models/staging/schema.yml
models:
  - name: stg_customers
    columns:
      - name: id
      - name: email
        meta:
          masking_tag: pii        # Applied to column for masking policies
      - name: phone_number
        meta:
          masking_tag: pii
      - name: city
      - name: birth_date
        meta:
          masking_tag: pii
```

!!! tip "Row access policies"
    For row-level security, see [Row Access Policies](row-access-policies.md).

#### 4. Grant the tag apply role to your dbt role

The role that runs dbt needs the `APPLY` privilege on the tag. The `z_tag__apply__pii` role and grant were already created in [Step 2](#step-2-create-tags). Grant it to your dbt execution role:

```yaml
# resources/roles__functional.yml
role_grants:
  - to_role: transformer_dbt
    roles:
      - z_tag__apply__pii
```

### Option 3: Manual Tag Application

For one-off tagging or small deployments, apply tags directly in Snowflake:

```sql
-- Tag individual columns (same tag, different data types)
ALTER TABLE analytics.staging.stg_customers
  ALTER COLUMN email SET TAG governance.public.pii = 'true';

ALTER TABLE analytics.staging.stg_customers
  ALTER COLUMN account_balance SET TAG governance.public.pii = 'true';

ALTER TABLE analytics.staging.stg_customers
  ALTER COLUMN birth_date SET TAG governance.public.pii = 'true';
```

### Option 4: At Creation Time (dbt Contracts and Constraints)

To close the unmasked window described in Option 2, apply the tag *as part of the `CREATE TABLE` statement* rather than in a post-hook. dbt supports this through [model contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts){:target="_blank"} and a `custom` column constraint that renders the tag inline.

Snowcap still owns the tag and the masking policies (Steps 1 through 5). dbt only applies the tag to the column, and it does so atomically when the table is built:

```yaml
# models/staging/schema.yml
models:
  - name: stg_customers
    config:
      contract:
        enforced: true          # required for dbt to render column constraints
    columns:
      - name: id
        data_type: number
      - name: email
        data_type: varchar
        constraints:
          - type: custom
            expression: "with tag (governance.tags.pii = 'true')"
      - name: birth_date
        data_type: date
        constraints:
          - type: custom
            expression: "with tag (governance.tags.pii = 'true')"
```

dbt compiles this into the column definition, so the tag (and therefore the masking policy) is present the moment the table exists:

```sql
create or replace table analytics.staging.stg_customers (
  id number,
  email varchar with tag (governance.tags.pii = 'true'),
  birth_date date with tag (governance.tags.pii = 'true')
)
as ( ... );
```

There is no separate `ALTER TABLE`, so there is no window where the data sits unmasked, and a partial run cannot leave a populated-but-untagged table. The dbt role still needs the `z_tag__apply__pii` role granted in [Step 6 / Option 2](#4-grant-the-tag-apply-role-to-your-dbt-role).

!!! warning "Requirements and limits of the constraint approach"
    - **Contracts must be enforced.** dbt only renders column constraints when `contract: enforced: true`. This also requires an explicit `data_type` for every column and turns column mismatches into build errors rather than silent drift.
    - **Tables only, not views.** dbt does not render constraints into view DDL. For view columns that pass a source column straight through, Snowflake propagates the masking policy automatically. For view columns produced by a transformation (hashing, concatenation, aggregation), the lineage is broken and neither the constraint nor auto-propagation applies. A post-hook (Option 2) remains the fallback for those columns.
    - **No dynamic tables.** Constraints are not rendered for the `dynamic_table` materialization.

## Fail-Closed: Default Masking at the Schema Level

The four options above are all **fail-open**: a column is exposed until someone remembers to tag it. Forget the tag, mistype the column name, or add a new PII column without updating `schema.yml`, and the data is readable by everyone. The failure mode is a leak.

You can invert this. Because tags are inherited down the object hierarchy, setting the masking tag on a **schema** (or database, or table) applies the associated masking policy to **every column whose data type matches a policy on the tag**, including columns in tables added later. Nothing is visible unless a role holds the unmask role. The failure mode becomes over-masking instead of exposure.

Snowcap sets a tag on a schema through the schema's `tags` property:

```yaml
# resources/schemas.yml
schemas:
  - name: analytics.sensitive
    managed_access: true
    tags:
      governance.tags.pii: 'true'   # every matching-type column here is masked by default
```

New tables built into `analytics.sensitive`, and new columns added to existing tables, are masked from the moment they exist. There is no per-column tagging step to forget.

!!! tip "The escape hatch is direct assignment"
    A masking policy assigned **directly** to a column takes precedence over the inherited tag-based one (see [Caveats](#caveats-and-limitations)). So the schema tag masks everything by default, and you explicitly allowlist a reviewed non-sensitive column by attaching a pass-through policy (one that always returns `val`) directly to it. Default-deny, with a documented exception list.

!!! warning "Know what fail-closed costs"
    - **It masks non-sensitive columns too.** Every VARCHAR in the schema is masked, not just the PII ones, so `city` and `product_name` come back `***MASKED***` for anyone without the unmask role. Only put genuinely sensitive schemas behind a schema-level tag, or be ready to maintain the allowlist above.
    - **Broad blast radius.** Changing the policy on the tag re-evaluates every inheriting column in the schema.
    - **No materialized views.** You cannot create a materialized view on any table protected by a tag-based masking policy, which now includes every table in the schema.
    - **Analysts need the unmask role to do anything useful.** Plan the RBAC grants before flipping a schema to default-masked, or you will block legitimate work.

## Verifying Masking Works

Test that masking is applied correctly:

```sql
-- As a user WITHOUT z_unmask__pii role
USE ROLE analyst;
USE SECONDARY ROLES NONE;
SELECT email, account_balance, birth_date
  FROM analytics.staging.stg_customers LIMIT 5;
-- Result: '***MASKED***', NULL, NULL

-- As a user WITH z_unmask__pii role
USE ROLE data_steward;
USE SECONDARY ROLES NONE;
SELECT email, account_balance, birth_date
  FROM analytics.staging.stg_customers LIMIT 5;
-- Result: 'john@example.com', 50000.00, '1985-03-15'
```

!!! warning "Secondary roles can bypass masking"
    Masking policies use `IS_ROLE_IN_SESSION()` to check access. When `USE SECONDARY ROLES ALL` is active, **all** roles granted to the user are in session — including unmask roles. This means a user with both `analyst` and `analyst_pii` roles will see unmasked data when secondary roles are enabled.

    To ensure masking works as expected, set `DEFAULT_SECONDARY_ROLES` to empty for users who should not have automatic access to all their roles:

    ```sql
    ALTER USER <username> SET DEFAULT_SECONDARY_ROLES = ();
    ```

## Caveats and Limitations

Tag-based masking is the right default for most teams, but it has behaviors worth knowing before you rely on it.

- **A directly assigned policy silently wins.** If a column has both a masking policy attached directly *and* a tag-based policy, the directly assigned policy takes precedence and the tag-based one is ignored. There is no error. Pick one mechanism per column so you always know which policy is in effect.
- **One policy per tag per data type.** A tag can hold only one masking policy for each data type. You cannot, for example, attach two different VARCHAR policies to the same tag. This is why Step 4 defines a single policy per data type.
- **Silent gap when a data type has no policy.** If you tag a column whose data type has no matching policy on the tag, the column is simply not masked. Audit the data types in your schema and make sure each has a policy (see the warning in Step 5).
- **Dependencies are locked while associated.** You cannot drop a tag or a masking policy while the policy is assigned to the tag. Unset the reference first.
- **Materialized views are blocked.** You cannot create a materialized view on a table that is protected by a tag-based masking policy.
- **Propagated tags depend on lineage.** `propagate: ON_DATA_MOVEMENT` carries tags to downstream tables and to view columns that pass a source column straight through. It does **not** cover columns derived by a transformation (hashing, concatenation, aggregation), because that breaks the lineage Snowflake uses to propagate. Tag or mask those columns explicitly.
- **Enterprise Edition required.** Masking policies and tag-based masking are Enterprise Edition (or higher) features.

Also review the [secondary roles warning](#verifying-masking-works) below: masking can be bypassed if `USE SECONDARY ROLES ALL` brings an unmask role into session.

## Benefits of This Pattern

1. **Scalable** - Tag once, mask everywhere. New tables just need column tags.

2. **Data-type agnostic** - One tag works for all data types; the correct policy is applied automatically.

3. **Separation of concerns** - Governance teams manage classifications; data engineers build tables.

4. **Role-based** - Unmask roles integrate with your existing RBAC hierarchy.

5. **Auditable** - Tags on columns provide clear documentation of data sensitivity.

6. **Maintainable** - Change masking logic in one policy, applies to all tagged columns.

7. **dbt-native** - Works with your existing dbt workflow via post-hooks or creation-time constraints.

## See Also

- [Row Access Policies](row-access-policies.md)
- [Role-Based Access Control](role-based-access-control.md)
- [MaskingPolicy](resources/masking-policy.md)
- [Tag](resources/tag.md)
- [TagMaskingPolicyReference](resources/tag-masking-policy-reference.md)
- [Snowflake: Introduction to Object Tagging](https://docs.snowflake.com/en/user-guide/object-tagging/introduction){:target="_blank"}
