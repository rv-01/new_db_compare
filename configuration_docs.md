# Configuration Reference

Complete reference for configuring the Oracle Database Table Comparison Tool.

## 📋 Configuration File Structure

The tool uses YAML format for configuration. Here's the complete structure:

```yaml
# Database connections
source_db:
  user: string              # Required: Source database username
  password: string          # Required: Source database password  
  dsn: string              # Required: Source database DSN (host:port/service)

target_db:
  user: string              # Required: Target database username
  password: string          # Required: Target database password
  dsn: string              # Required: Target database DSN (host:port/service)

# Table comparison settings
table_config:
  schema: string            # Required: Schema name (case sensitive)
  table_name: string        # Required: Table name (case sensitive)
  primary_keys: list        # Required: List of primary key column names
  batch_size: integer       # Required: Number of rows per batch (default: 1000)
  max_threads: integer      # Required: Maximum concurrent threads (default: 4)

# Output file paths
paths:
  audit_log: string         # Required: Path to audit log file
  source_sql_output: string # Required: Path to source SQL output file
  target_sql_output: string # Required: Path to target SQL output file

# Feature flags
flags:
  enable_reverification: boolean  # Optional: Enable PK verification (default: true)
  enable_restart: boolean         # Optional: Enable restart capability (default: true)

# Optional advanced settings
logging:
  level: string             # Optional: Log level (DEBUG, INFO, WARNING, ERROR)
  format: string            # Optional: Custom log format

advanced:
  connection_timeout: integer     # Optional: DB connection timeout (default: 30)
  query_timeout: integer         # Optional: Query timeout (default: 300)
  hash_algorithm: string         # Optional: Hash algorithm (md5, sha1, sha256)

# Optional notification settings
notifications:
  enabled: boolean          # Optional: Enable notifications (default: false)
  smtp_server: string       # Optional: SMTP server hostname
  smtp_port: integer        # Optional: SMTP server port
  sender_email: string      # Optional: Sender email address
  recipient_emails: list    # Optional: List of recipient email addresses
```

## 🔧 Configuration Sections

### Database Connections

#### source_db / target_db
Configure connections to source and target Oracle databases.

**Required Fields:**
- `user`: Database username with SELECT privileges on comparison tables and CREATE TABLE privileges for audit tables
- `password`: Database password (consider using environment variables)
- `dsn`: Oracle connection string in format `hostname:port/service_name`

**Examples:**
```yaml
# Basic connection
source_db:
  user: hr_user
  password: hr_password
  dsn: oracle-prod:1521/ORCL

# Connection with specific service
target_db:
  user: hr_replica
  password: replica_pass
  dsn: oracle-replica.company.com:1521/HR_SERVICE
```

**Security Best Practices:**
- Use dedicated database users with minimal required privileges
- Consider Oracle Wallet for password-less authentication
- Use environment variables for sensitive credentials:
  ```yaml
  source_db:
    user: ${SOURCE_DB_USER}
    password: ${SOURCE_DB_PASSWORD}
    dsn: ${SOURCE_DB_DSN}
  ```

### Table Configuration

#### table_config
Defines which table to compare and how to process it.

**Required Fields:**
- `schema`: Schema name (case-sensitive, usually uppercase in Oracle)
- `table_name`: Table name (case-sensitive, usually uppercase in Oracle)
- `primary_keys`: List of column names that uniquely identify rows
- `batch_size`: Number of rows to process in each batch
- `max_threads`: Maximum number of concurrent processing threads

**Examples:**
```yaml
# Simple table with single primary key
table_config:
  schema: HR
  table_name: EMPLOYEES
  primary_keys: [EMPLOYEE_ID]
  batch_size: 1000
  max_threads: 4

# Composite primary key
table_config:
  schema: SALES
  table_name: ORDER_LINES
  primary_keys: [ORDER_ID, LINE_NUMBER]
  batch_size: 2000
  max_threads: 6

# Large table optimization
table_config:
  schema: WAREHOUSE
  table_name: INVENTORY_HISTORY
  primary_keys: [ITEM_ID, TRANSACTION_DATE, SEQUENCE_NO]
  batch_size: 5000
  max_threads: 8
```

**Performance Tuning Guidelines:**

| Table Size | Recommended batch_size | Recommended max_threads |
|------------|----------------------|------------------------|
| < 100K rows | 500-1000 | 2-4 |
| 100K-1M rows | 1000-2000 | 4-6 |
| 1M-10M rows | 2000-5000 | 6-8 |
| > 10M rows | 5000-10000 | 8-16 |

### Output Paths

#### paths
Configure where output files are written.

**Required Fields:**
- `audit_log`: Path to audit log file (will be created if doesn't exist)
- `source_sql_output`: Path for SQL statements to sync source database
- `target_sql_output`: Path for SQL statements to sync target database

**Examples:**
```yaml
# Basic paths
paths:
  audit_log: ./logs/comparison_audit.log
  source_sql_output: ./output/source_sync.sql
  target_sql_output: ./output/target_sync.sql

# Organized by date
paths:
  audit_log: ./logs/hr_employees_{{ date }}/audit.log
  source_sql_output: ./output/hr_employees_{{ date }}/source_sync.sql
  target_sql_output: ./output/hr_employees_{{ date }}/target_sync.sql

# Network paths (if accessible)
paths:
  audit_log: /shared/logs/table_comparison/audit.log
  source_sql_output: /shared/output/source_sync_statements.sql
  target_sql_output: /shared/output/target_sync_statements.sql
```

### Feature Flags

#### flags
Enable or disable optional features.

**Optional Fields:**
- `enable_reverification`: Verify INSERT statements against primary key constraints (default: true)
- `enable_restart`: Enable checkpoint-based restart capability (default: true)

**Examples:**
```yaml
# Maximum safety (recommended for production)
flags:
  enable_reverification: true
  enable_restart: true

# Performance mode (faster but less safe)
flags:
  enable_reverification: false
  enable_restart: false

# Development mode
flags:
  enable_reverification: true
  enable_restart: false  # Fresh start each run
```

## 🎯 Use Case Examples

### Production Environment
```yaml
# Production configuration with high safety
source_db:
  user: ${PROD_SOURCE_USER}
  password: ${PROD_SOURCE_PASS}
  dsn: oracle-prod-primary:1521/PROD

target_db:
  user: ${PROD_TARGET_USER}
  password: ${PROD_TARGET_PASS}
  dsn: oracle-prod-replica:1521/PROD

table_config:
  schema: SALES
  table_name: TRANSACTIONS
  primary_keys: [TRANSACTION_ID]
  batch_size: 2000
  max_threads: 6

paths:
  audit_log: /prod/logs/table_comparison/sales_transactions.log
  source_sql_output: /prod/sync/sales_transactions_source.sql
  target_sql_output: /prod/sync/sales_transactions_target.sql

flags:
  enable_reverification: true
  enable_restart: true

logging:
  level: INFO

advanced:
  connection_timeout: 60
  query_timeout: 600
```

### Development Environment
```yaml
# Development configuration for testing
source_db:
  user: dev_user
  password: dev_pass
  dsn: localhost:1521/XE

target_db:
  user: dev_user
  password: dev_pass
  dsn: localhost:1522/XE

table_config:
  schema: TEST
  table_name: SAMPLE_DATA
  primary_keys: [ID]
  batch_size: 100
  max_threads: 2

paths:
  audit_log: ./dev_logs/audit.log
  source_sql_output: ./dev_output/source_sync.sql
  target_sql_output: ./dev_output/target_sync.sql

flags:
  enable_reverification: true
  enable_restart: false

logging:
  level: DEBUG
```

### High-Performance Large Table
```yaml
# Optimized for very large tables
source_db:
  user: warehouse_user
  password: ${WAREHOUSE_PASS}
  dsn: oracle-warehouse:1521/DWH

target_db:
  user: warehouse_replica
  password: ${WAREHOUSE_REPLICA_PASS}
  dsn: oracle-warehouse-dr:1521/DWH

table_config:
  schema: WAREHOUSE
  table_name: FACT_SALES_HISTORY
  primary_keys: [SALES_DATE, PRODUCT_ID, STORE_ID]
  batch_size: 10000
  max_threads: 16

paths:
  audit_log: /warehouse/logs/fact_sales_comparison.log
  source_sql_output: /warehouse/sync/fact_sales_source.sql
  target_sql_output: /warehouse/sync/fact_sales_target.sql

flags:
  enable_reverification: false  # Skip for performance
  enable_restart: true

advanced:
  connection_timeout: 120
  query_timeout: 1800  # 30 minutes for large batches
  hash_algorithm: md5
```

## 🔍 Validation Rules

The tool validates configuration on startup:

1. **Required Fields**: All required fields must be present
2. **Database Connectivity**: Both source and target databases must be accessible
3. **Table Access**: User must have SELECT privileges on specified tables
4. **Primary Keys**: All specified primary key columns must exist in the table
5. **File Paths**: Parent directories for output files must be writable
6. **Thread Limits**: max_threads must be positive and reasonable (≤ 32)
7. **Batch Size**: batch_size must be positive and reasonable (≥ 10, ≤ 100000)

## 🚨 Common Configuration Errors

### Database Connection Issues
```yaml
# ❌ Wrong - Missing port
dsn: oracle-server/ORCL

# ✅ Correct - Include port
dsn: oracle-server:1521/ORCL

# ❌ Wrong - Case sensitive schema
schema: hr

# ✅ Correct - Usually uppercase in Oracle
schema: HR
```

### Primary Key Configuration
```yaml
# ❌ Wrong - Column doesn't exist
primary_keys: [ID, CREATED_DATE]

# ✅ Correct - Verify column names in database
primary_keys: [EMPLOYEE_ID, DEPARTMENT_ID]

# ❌ Wrong - Not unique combination
primary_keys: [STATUS]  # Multiple rows can have same status

# ✅ Correct - Unique combination
primary_keys: [ORDER_ID, LINE_NUMBER]
```

### Performance Issues
```yaml
# ❌ Wrong - Too small batches (slow)
batch_size: 10
max_threads: 1

# ❌ Wrong - Too large batches (memory issues)
batch_size: 100000
max_threads: 32

# ✅ Correct - Balanced configuration
batch_size: 2000
max_threads: 6
```

## 📚 Environment Variables

Support for environment variable substitution:

```bash
# Set environment variables
export SOURCE_DB_USER="hr_source"
export SOURCE_DB_PASS="secure_password"
export TARGET_DB_USER="hr_target"
export TARGET_DB_PASS="secure_password"

# Use in configuration
source_db:
  user: ${SOURCE_DB_USER}
  password: ${SOURCE_DB_PASS}
  dsn: oracle-prod:1521/HR
```

## 🔧 Advanced Configuration

### Custom Logging Format
```yaml
logging:
  level: INFO
  format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
```

### Email Notifications (Future Feature)
```yaml
notifications:
  enabled: true
  smtp_server: smtp.company.com
  smtp_port: 587
  sender_email: noreply@company.com
  recipient_emails:
    - dba@company.com
    - ops-team@company.com
```

For more configuration examples, see the `examples/configs/` directory.
