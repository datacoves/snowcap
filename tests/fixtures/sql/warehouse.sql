
CREATE WAREHOUSE IF NOT EXISTS XSMALL_WH
    WITH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    GENERATION = '2'
    RESOURCE_CONSTRAINT = STANDARD_GEN_2
    AUTO_SUSPEND = 60
    AUTO_RESUME = FALSE
    initially_suspended = true
    RESOURCE_MONITOR = my_mon
    COMMENT = 'My XSMALL warehouse'
;



-- CREATE WAREHOUSE IF NOT EXISTS XSMALL_WH2 AUTO_SUSPEND = NULL;

CREATE WAREHOUSE lowercase_wh
warehouse_size = x6large
warehouse_type = snowpark-optimized
resource_constraint = memory_16x_x86
scaling_policy = economy
initially_suspended = true
;

CREATE WAREHOUSE ADAPTIVE_WH
    WITH
    WAREHOUSE_TYPE = 'ADAPTIVE'
    MAX_QUERY_PERFORMANCE_LEVEL = LARGE
    STATEMENT_TIMEOUT_IN_SECONDS = 3600
    COMMENT = 'My adaptive warehouse'
;

