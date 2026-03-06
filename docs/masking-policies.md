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
# resources/governance.yml
databases:
  - name: governance

schemas:
  - name: governance.public
```

### Step 2: Create Tags

Define tags that represent your data classifications:

```yaml
# resources/governance.yml (continued)
tags:
  - name: governance.public.pii
    comment: Personally Identifiable Information

  - name: governance.public.confidential
    comment: Confidential business data
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
# resources/governance.yml (continued)
masking_policies:
  # String/VARCHAR columns
  - name: governance.public.mask_pii_string
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
  - name: governance.public.mask_pii_number
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
  - name: governance.public.mask_pii_date
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
  - name: governance.public.mask_pii_timestamp
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
# resources/governance.yml (continued)
tag_masking_policy_references:
  # Use fully qualified names: database.schema.name
  - tag_name: governance.public.pii
    masking_policy_name: governance.public.mask_pii_string

  - tag_name: governance.public.pii
    masking_policy_name: governance.public.mask_pii_number

  - tag_name: governance.public.pii
    masking_policy_name: governance.public.mask_pii_date

  - tag_name: governance.public.pii
    masking_policy_name: governance.public.mask_pii_timestamp
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
│   ├── roles__unmasking.yml     # Unmask roles + grants
│   ├── governance.yml           # Tags, masking policies, and references
│   ├── users.yml
│   └── ...
├── plan.sh
└── apply.sh
```

## Applying Tags to Columns with dbt

Once your tags and masking policies are deployed with Snowcap, you need to apply tags to columns. The recommended approach is to use dbt to tag columns during the build process.

### Option 1: dbt-tags Package

The [dbt-tags](https://dbt-tags.iflambda.com/latest/getting-started.html#6-apply-tags-to-columns){:target="_blank"} package applies tags to columns during the dbt build process.

**Steps:**

1. Install the dbt-tags package
2. Define tags on columns in your model's schema.yml
3. Add a post-hook to apply the tags

**Define tags in schema.yml:**

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

**Add post-hook in dbt_project.yml:**

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

!!! warning "dbt-tags manages the full lifecycle"
    This package can also create tags and masking policies. If you're using Snowcap to manage those, you only need the `apply_column_tags()` macro. See [Documentation](https://dbt-tags.iflambda.com/latest/getting-started.html#6-apply-tags-to-columns){:target="_blank"} for  details.

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
SELECT email, account_balance, birth_date
  FROM analytics.staging.stg_customers LIMIT 5;
-- Result: '***MASKED***', NULL, NULL

-- As a user WITH z_unmask__pii role
USE ROLE data_steward;
SELECT email, account_balance, birth_date
  FROM analytics.staging.stg_customers LIMIT 5;
-- Result: 'john@example.com', 50000.00, '1985-03-15'
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
- [dbt-tags Package](https://dbt-tags.iflambda.com/latest/getting-started.html#6-apply-tags-to-columns){:target="_blank"}
