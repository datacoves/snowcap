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
│       MASK_PII_STRING, MASK_PII_NUMBER, MASK_PII_DATE, MASK_PII_TIMESTAMP   │
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

  - name: governance.tags.confidential
    comment: Confidential business data

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
    body: |
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
    body: |
      CASE
        WHEN IS_ROLE_IN_SESSION('Z_UNMASK__PII') THEN val
        ELSE NULL
      END
    comment: Masks PII numeric data

  # Date columns
  - name: governance.policies.mask_pii_date
    args:
      - name: val
        data_type: DATE
    returns: DATE
    body: |
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
    body: |
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

## Applying Tags to Columns with dbt

Once your tags and masking policies are deployed with Snowcap, you need to apply tags to columns. The recommended approach is to use dbt to tag columns during the build process.

### Option 1: dbt-tags Package

The [dbt-tags](https://hub.getdbt.com/infinitelambda/dbt_tags/latest/){:target="_blank"} package applies tags to columns during the dbt build process. See the [dbt-tags getting started guide](https://dbt-tags.iflambda.com/latest/getting-started.html#6-apply-tags-to-columns){:target="_blank"} for full documentation.

#### 1. Add dbt-tags to packages.yml

```yaml
# packages.yml
packages:
  - package: infinitelambda/dbt_tags
    version: 1.9.0
```

Then run `dbt deps` to install.

#### 2. Configure dbt_project.yml variables

Since Snowcap manages the tags in a dedicated governance database, you need to tell dbt-tags where to find them. Add these variables to your `dbt_project.yml`:

```yaml
# dbt_project.yml
vars:
  dbt_tags__opt_in_default_naming_config: false
  dbt_tags__database: "GOVERNANCE"
  dbt_tags__schema: "TAGS"
```

!!! note "Why `dbt_tags__opt_in_default_naming_config: false`?"
    By default, dbt-tags uses dbt's `generate_schema_name` and `generate_database_name` macros to resolve the tag location. If your project overrides these macros (which is common), this can cause errors. Setting this to `false` uses the `dbt_tags__database` and `dbt_tags__schema` values directly.

#### 3. Add the post-hook

Add the `apply_column_tags()` post-hook so tags are applied every time a model is built:

```yaml
# dbt_project.yml
models:
  your_project:
    +post-hook:
      - >
        {% if flags.WHICH in ('run', 'build') %}
        {{ dbt_tags.apply_column_tags() }}
        {% endif %}
```

#### 4. Define tags on columns in schema.yml

Tag columns in your model's schema file. The tag name must match the tag created in Snowcap (e.g., `pii` matches `governance.tags.pii`):

```yaml
# models/staging/schema.yml
models:
  - name: stg_customers
    columns:
      - name: id
      - name: email
        tags:
          - pii
      - name: phone_number
        tags:
          - pii
      - name: city
      - name: birth_date
        tags:
          - pii
```

#### 5. Grant APPLY privilege to the dbt role

The role that runs dbt needs the `APPLY` privilege on the tag. In your Snowcap config:

```yaml
# resources/tags.yml
roles:
  - name: z_tag__apply__pii

grants:
  - priv: APPLY
    on: tag governance.tags.pii
    to: z_tag__apply__pii
```

Then grant this role to your dbt execution role:

```yaml
# resources/roles__functional.yml
role_grants:
  - to_role: transformer_dbt
    roles:
      - z_tag__apply__pii
```

!!! warning "dbt-tags manages the full lifecycle"
    This package can also create tags and masking policies. If you're using Snowcap to manage those, you only need the `apply_column_tags()` macro. See [Documentation](https://dbt-tags.iflambda.com/latest/getting-started.html#6-apply-tags-to-columns){:target="_blank"} for details.

### Option 2: Manual Tag Application

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

## Benefits of This Pattern

1. **Scalable** - Tag once, mask everywhere. New tables just need column tags.

2. **Data-type agnostic** - One tag works for all data types; the correct policy is applied automatically.

3. **Separation of concerns** - Governance teams manage classifications; data engineers build tables.

4. **Role-based** - Unmask roles integrate with your existing RBAC hierarchy.

5. **Auditable** - Tags on columns provide clear documentation of data sensitivity.

6. **Maintainable** - Change masking logic in one policy, applies to all tagged columns.

7. **dbt-native** - Works with your existing dbt workflow via post-hooks.

## See Also

- [Role-Based Access Control](role-based-access-control.md)
- [MaskingPolicy](resources/masking-policy.md)
- [Tag](resources/tag.md)
- [TagMaskingPolicyReference](resources/tag-masking-policy-reference.md)
- [Snowflake: Introduction to Object Tagging](https://docs.snowflake.com/en/user-guide/object-tagging/introduction){:target="_blank"}
- [dbt-tags Package](https://hub.getdbt.com/infinitelambda/dbt_tags/latest/){:target="_blank"}
