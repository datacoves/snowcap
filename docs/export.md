# Export Existing Resources

Already have a Snowflake environment? Generate config from your existing setup:

```sh
snowcap export \
  --resource=warehouse,role,grant \
  --out=snowcap.yml
```

## Export Specific Resource Types

Export only the resources you need:

```sh
# Export warehouses only
snowcap export --resource=warehouse --out=warehouses.yml

# Export roles and grants
snowcap export --resource=role,grant --out=rbac.yml

# Export all supported resources
snowcap export --resource=all --out=snowcap.yml
```

## Output Format

The exported YAML can be used directly with `snowcap plan` and `snowcap apply`:

```yaml
# Exported warehouses.yml
warehouses:
  - name: ANALYTICS
    warehouse_size: XSMALL
    auto_suspend: 60
    auto_resume: true

  - name: LOADING
    warehouse_size: SMALL
    auto_suspend: 300
    auto_resume: true
```

## Workflow: Migrate from Manual Management

1. Export your current Snowflake configuration:
   ```sh
   snowcap export --resource=all --out=snowcap.yml
   ```

2. Review and organize the exported config into separate files if needed

3. Add the config to version control:
   ```sh
   git add snowcap.yml
   git commit -m "Import existing Snowflake configuration"
   ```

4. From now on, manage changes through Snowcap:
   ```sh
   # Edit snowcap.yml, then:
   snowcap plan --config snowcap.yml
   snowcap apply --config snowcap.yml
   ```
