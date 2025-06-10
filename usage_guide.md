# Oracle Database Table Comparison Tool

A production-ready Python script for comparing tables between two Oracle databases with advanced features including multi-threading, restart capability, and comprehensive auditing.

## 🚀 Features

- **Row-level hashing** for accurate data comparison
- **Multi-threaded processing** for optimal performance
- **Restart/Resume capability** with checkpoint management
- **Comprehensive auditing** with database logging
- **SQL generation** for synchronization (separate source/target files)
- **Primary key verification** to prevent constraint violations
- **Progress tracking** with tqdm progress bars
- **YAML-based configuration** for easy management

## 📋 Prerequisites

- Python 3.8 or higher
- Oracle Database access (source and target)
- Required Python packages (see requirements.txt)

## 🛠️ Installation

1. **Clone or download the script files:**
   ```bash
   mkdir oracle-table-compare
   cd oracle-table-compare
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Oracle Instant Client** (if not already installed):
   - Download Oracle Instant Client from Oracle website
   - Set environment variables (LD_LIBRARY_PATH on Linux, PATH on Windows)

## ⚙️ Configuration

1. **Copy the sample configuration:**
   ```bash
   cp config.yaml.sample config.yaml
   ```

2. **Edit config.yaml with your database details:**
   ```yaml
   source_db:
     user: your_source_user
     password: your_source_password
     dsn: source_host:1521/source_service_name

   target_db:
     user: your_target_user
     password: your_target_password
     dsn: target_host:1521/target_service_name

   table_config:
     schema: YOUR_SCHEMA
     table_name: YOUR_TABLE
     primary_keys: [ID, CODE]  # Adjust based on your table
     batch_size: 1000
     max_threads: 4

   paths:
     audit_log: ./logs/audit.log
     source_sql_output: ./output/source_sync_statements.sql
     target_sql_output: ./output/target_sync_statements.sql

   flags:
     enable_reverification: true
     enable_restart: true
   ```

## 🏃‍♂️ Usage

### Basic Usage
```bash
python table_comparator.py config.yaml
```

### Running with Logging
```bash
python table_comparator.py config.yaml 2>&1 | tee comparison_run.log
```

### Running in Background
```bash
nohup python table_comparator.py config.yaml > comparison.log 2>&1 &
```

## 📊 Output Files

The tool generates several output files:

1. **Source Sync SQL** (`./output/source_sync_statements.sql`)
   - INSERT/UPDATE/DELETE statements for source database

2. **Target Sync SQL** (`./output/target_sync_statements.sql`)
   - INSERT/UPDATE/DELETE statements for target database

3. **Verified SQL** (`./output/target_sync_statements_verified.sql`)
   - Verified INSERT statements (if reverification enabled)

4. **Audit Log** (`./logs/audit.log`)
   - Detailed comparison events and statistics

## 🔄 Restart and Resume

The tool supports automatic restart/resume functionality:

1. **Enable in config:**
   ```yaml
   flags:
     enable_restart: true
   ```

2. **On failure or interruption:**
   - The tool saves progress to `TABLE_COMPARISON_METADATA` table
   - Simply re-run the same command to resume from last checkpoint

3. **Force fresh start:**
   ```yaml
   flags:
     enable_restart: false
   ```

## 📈 Performance Tuning

### Optimal Settings for Large Tables

```yaml
table_config:
  batch_size: 5000      # Larger batches for better performance
  max_threads: 8        # Adjust based on available CPU cores

# Monitor system resources during execution
```

### Memory Considerations
- Each thread processes one batch at a time
- Memory usage ≈ `batch_size × max_threads × row_size`
- Reduce batch_size if encountering memory issues

## 🔍 Troubleshooting

### Common Issues

1. **Connection Errors:**
   ```
   ORA-12514: TNS:listener does not currently know of service
   ```
   - Verify DSN format: `host:port/service_name`
   - Test connection with Oracle SQL tools first

2. **Permission Errors:**
   ```
   ORA-00942: table or view does not exist
   ```
   - Ensure user has SELECT privileges on source/target tables
   - Grant CREATE TABLE privileges for audit tables

3. **Memory Issues:**
   ```
   MemoryError: Unable to allocate array
   ```
   - Reduce `batch_size` in configuration
   - Reduce `max_threads` for large tables

4. **Performance Issues:**
   - Enable parallel execution on Oracle side
   - Create indexes on primary key columns
   - Consider partitioning for very large tables

### Logging Levels

Set logging level in config for troubleshooting:
```yaml
logging:
  level: DEBUG  # DEBUG, INFO, WARNING, ERROR
```

## 🧪 Testing

### Test with Small Tables
```yaml
table_config:
  batch_size: 100
  max_threads: 2
```

### Verify Output
1. Check audit log for summary statistics
2. Review generated SQL files
3. Validate primary key verification results

## 📊 Monitoring and Auditing

### Database Audit Table
The tool creates and maintains audit tables:
- `TABLE_COMPARISON_AUDIT`: Job statistics and results
- `TABLE_COMPARISON_METADATA`: Restart checkpoint data

### Query Audit Information
```sql
-- View recent comparison jobs
SELECT * FROM TABLE_COMPARISON_AUDIT 
ORDER BY created_time DESC;

-- Check restart metadata
SELECT * FROM TABLE_COMPARISON_METADATA 
WHERE schema_name = 'YOUR_SCHEMA' 
AND table_name = 'YOUR_TABLE';
```

## 🔒 Security Considerations

1. **Password Security:**
   - Use environment variables for passwords
   - Consider Oracle Wallet for password-less authentication

2. **Database Privileges:**
   - Grant minimum required privileges
   - Use dedicated comparison user accounts

3. **Network Security:**
   - Use encrypted connections (Oracle Advanced Security)
   - Run from secure network zones

## 📚 Advanced Usage

### Comparing Multiple Tables
Create separate config files for each table:
```bash
python table_comparator.py config_table1.yaml
python table_comparator.py config_table2.yaml
```

### Automated Scheduling
Add to crontab for regular comparisons:
```bash
# Daily comparison at 2 AM
0 2 * * * /path/to/python /path/to/table_comparator.py /path/to/config.yaml
```

### Integration with CI/CD
```bash
#!/bin/bash
# integration_test.sh
python table_comparator.py test_config.yaml
if [ $? -eq 0 ]; then
    echo "Data comparison passed"
    exit 0
else
    echo "Data comparison failed"
    exit 1
fi
```

## 🤝 Contributing

To extend the tool:
1. Follow the modular design pattern
2. Add comprehensive error handling
3. Include unit tests for new features
4. Update configuration schema as needed

## 📝 License

This tool is provided as-is for production use. Modify as needed for your environment.
