# Row Access Policies

This guide describes a pattern for implementing row-level security in Snowflake using Snowcap. The pattern uses **roles** to control which rows users can see, without requiring a separate mapping table.

## Overview

Row access policies filter which rows a user can see based on their granted roles. This pattern encodes access directly in role names, making it simple to manage.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER                                           │
│                              alice                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (has role)
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FUNCTIONAL ROLE                                     │
│                         analyst_emea                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (granted)
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ROW ACCESS ROLE                                     │
│                    z_row_access__region__emea                               │
│                              │                                              │
│              ┌───────────────┼───────────────┐                              │
│              ▼               ▼               ▼                              │
│   z_row_access__     z_row_access__    z_row_access__                       │
│   country__es        country__fr       country__it                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (policy checks IS_ROLE_IN_SESSION)
┌─────────────────────────────────────────────────────────────────────────────┐
│                       ROW ACCESS POLICY                                     │
│  IS_ROLE_IN_SESSION('Z_ROW_ACCESS__COUNTRY__' || UPPER(country_code))      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (filters)
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TABLE ROWS                                        │
│            Only rows where country_code IN ('ES', 'FR', 'IT')              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Why This Pattern?

| Approach | Pros | Cons |
|----------|------|------|
| **Mapping table** | Flexible values | Extra table to maintain |
| **Role-based (this pattern)** | No mapping table; role hierarchy handles groups | Role names must follow convention |

With role-based row access:

- No mapping table to maintain
- Role hierarchy handles regional access (EMEA → ES, FR, IT)
- Adding a new country = create role + grant to region
- Consistent with Snowflake RBAC patterns

## Role Naming Convention

| Role Type | Pattern | Example |
|-----------|---------|---------|
| Leaf role | `z_row_access__<type>__<value>` | `z_row_access__country__us` |
| Parent role | `z_row_access__<type>__<region>` | `z_row_access__region__emea` |

!!! tip "Use IDs/codes for values"
    Use clean identifiers for filter values to avoid special character issues:

    - Country: `us`, `fr`, `es` (ISO codes)
    - Territory: `t001`, `t002` (IDs)
    - Product: `prod001`, `widget` (codes)
    - Organization: `org001`, `org002` (IDs)

## Configuration Examples

### Step 1: Create Governance Schema

If you haven't already, create a governance database and schema for policies. See [Tag-Based Masking Policies](masking-policies.md#step-1-create-the-governance-database-and-schema) for details.

### Step 2: Create Row Access Roles and Hierarchy

Create two files: one for the country list, and one for the roles and grants.

**`resources/row_access__country.yml`** - Define the list of countries:

```yaml
vars:
  - name: countries
    type: list
    default:
      - us
      - ca
      - mx
      - es
      - fr
      - it
      - de
```

Adding a new country is as simple as adding it to this list.

**`resources/row_access__country_roles.yml`** - Create roles and hierarchy:

```yaml
# Leaf roles - one per country (created from var list)
roles:
  - for_each: var.countries
    name: "z_row_access__country__{{ each.value }}"

# Parent roles - for regional access
  - name: z_row_access__region__americas
  - name: z_row_access__region__emea

# Hierarchy - grant leaf roles to parent roles
role_grants:
  - to_role: z_row_access__region__americas
    roles:
      - z_row_access__country__us
      - z_row_access__country__ca
      - z_row_access__country__mx

  - to_role: z_row_access__region__emea
    roles:
      - z_row_access__country__es
      - z_row_access__country__fr
      - z_row_access__country__it
      - z_row_access__country__de
```

When a user has `z_row_access__region__emea`, they inherit access to all EMEA countries because `IS_ROLE_IN_SESSION()` sees inherited roles.

### Step 3: Create Row Access Policy

The policy checks if the user has a role matching the row's value:

```yaml
# resources/row_access_policies.yml
row_access_policies:
  - name: governance.policies.rap_country
    args:
      - name: country_val
        data_type: VARCHAR
    body: |-
      country_val IS NOT NULL
      AND IS_ROLE_IN_SESSION('Z_ROW_ACCESS__COUNTRY__' || UPPER(country_val))
    comment: Filters rows by country based on user role
```

The policy:

1. Takes the country column value as input
2. Constructs the expected role name (`Z_ROW_ACCESS__COUNTRY__US`)
3. Checks if that role (or a parent) is in the user's session

### Step 4: Grant to Functional Roles

Grant row access roles to your functional roles:

```yaml
# resources/roles__functional.yml
role_grants:
  - to_role: analyst_americas
    roles:
      - z_row_access__region__americas

  - to_role: analyst_emea
    roles:
      - z_row_access__region__emea

  - to_role: analyst_global
    roles:
      - z_row_access__region__americas
      - z_row_access__region__emea
```

## Applying Row Access Policies with dbt

### Option 1: Snowcap Macros

Snowcap can generate dbt macros that apply row access policies based on `meta` configuration.

#### 1. Generate the macros

```bash
snowcap generate dbt-macros
```

This creates `macros/snowcap_apply_tags.sql` with the governance macros.

#### 2. Configure dbt_project.yml

```yaml
# dbt_project.yml
vars:
  snowcap_policy_database: GOVERNANCE
  snowcap_policy_schema: POLICIES

models:
  your_project:
    +post-hook:
      - "{{ snowcap_apply_policies() }}"
```

#### 3. Configure schema.yml

Specify the policy and column in your model's config.meta (dbt 1.10+ requires meta under config):

```yaml
# models/staging/schema.yml
models:
  - name: stg_orders
    config:
      meta:
        row_access_policy: rap_country
        row_access_column: country_code
    columns:
      - name: order_id
      - name: country_code
      - name: order_total
```

The macro generates:

```sql
ALTER TABLE <table> ADD ROW ACCESS POLICY
  governance.policies.rap_country ON (country_code);
```

### Option 2: Manual Application

Apply row access policies directly in Snowflake:

```sql
ALTER TABLE analytics.staging.stg_orders
  ADD ROW ACCESS POLICY governance.policies.rap_country
  ON (country_code);
```

## Multiple Filter Types

You can have different filter types for different tables:

```yaml
# Row access policies for different filter types
row_access_policies:
  - name: governance.policies.rap_country
    args:
      - name: val
        data_type: VARCHAR
    body: |-
      val IS NOT NULL
      AND IS_ROLE_IN_SESSION('Z_ROW_ACCESS__COUNTRY__' || UPPER(val))

  - name: governance.policies.rap_territory
    args:
      - name: val
        data_type: VARCHAR
    body: |-
      val IS NOT NULL
      AND IS_ROLE_IN_SESSION('Z_ROW_ACCESS__TERRITORY__' || UPPER(val))

  - name: governance.policies.rap_product
    args:
      - name: val
        data_type: VARCHAR
    body: |-
      val IS NOT NULL
      AND IS_ROLE_IN_SESSION('Z_ROW_ACCESS__PRODUCT__' || UPPER(val))

  - name: governance.policies.rap_org
    args:
      - name: val
        data_type: VARCHAR
    body: |-
      val IS NOT NULL
      AND IS_ROLE_IN_SESSION('Z_ROW_ACCESS__ORG__' || UPPER(val))
```

Then in dbt:

```yaml
models:
  - name: orders
    config:
      meta:
        row_access_policy: rap_country
        row_access_column: country_code

  - name: sales_targets
    config:
      meta:
        row_access_policy: rap_territory
        row_access_column: territory_id

  - name: product_costs
    config:
      meta:
        row_access_policy: rap_product
        row_access_column: product_code
```

## Verifying Row Access

Test that filtering works correctly:

```sql
-- As a user with z_row_access__region__emea (via analyst_emea role)
USE ROLE analyst_emea;
SELECT DISTINCT country_code FROM orders;
-- Result: 'ES', 'FR', 'IT', 'DE'

-- As a user with z_row_access__country__us only
USE ROLE analyst_us;
SELECT DISTINCT country_code FROM orders;
-- Result: 'US'

-- As a user with no row access roles
USE ROLE analyst_restricted;
SELECT * FROM orders;
-- Result: (empty - no rows visible)
```

## Best Practices

1. **Use clean identifiers** - Use IDs/codes rather than names to avoid special characters

2. **Follow the naming convention** - `z_row_access__<type>__<value>` ensures policies work correctly

3. **Use var lists for leaf roles** - Makes adding new values simple

4. **Build hierarchies with grants** - Regional access = grant leaf roles to parent role

5. **Handle NULLs in policies** - Include `val IS NOT NULL` to handle NULL column values

6. **One filter type per table** - Each table should have at most one row access policy

## See Also

- [Tag-Based Masking Policies](masking-policies.md)
- [RowAccessPolicy](resources/row-access-policy.md)
- [Role-Based Access Control](role-based-access-control.md)
- [Snowflake: Row Access Policies](https://docs.snowflake.com/en/user-guide/security-row-intro){:target="_blank"}
