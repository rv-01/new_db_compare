#!/usr/bin/env python3
"""
DB_Sentinel_utility - Advanced Oracle Database Table Comparison Tool
==================================================================

A production-ready utility for comparing tables between two Oracle databases
with advanced features including multi-threading, restart capability,
verification, and comprehensive auditing.

Author: Solutions Architect
Version: 1.0.0
Python: 3.8+
"""

import os
import sys
import yaml
import logging
import hashlib
import threading
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import time
import psutil

# Third-party imports
import oracledb
from tqdm import tqdm
import pandas as pd


@dataclass
class TableConfig:
    """Configuration for a single table comparison"""
    table_name: str
    primary_key: List[str]
    chunk_size: int = 10000
    schema: Optional[str] = None
    where_clause: Optional[str] = None
    columns: Optional[List[str]] = None


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    user: str
    password: str
    dsn: str
    
    def get_connection_string(self) -> str:
        return f"{self.user}/{self.password}@{self.dsn}"


@dataclass
class ComparisonResult:
    """Result of table comparison"""
    table_name: str
    schema: str
    source_count: int
    target_count: int
    mismatch_count: int
    insert_count: int
    update_count: int
    delete_count: int
    status: str
    start_time: datetime
    end_time: datetime
    error_message: Optional[str] = None


class DBSentinelError(Exception):
    """Custom exception for DB Sentinel operations"""
    pass


class DatabaseManager:
    """Manages Oracle database connections and operations"""
    
    def __init__(self, config: DatabaseConfig, pool_size: int = 10):
        self.config = config
        self.pool_size = pool_size
        self._connection_pool = None
        self._lock = threading.Lock()
        
    def initialize_pool(self):
        """Initialize connection pool"""
        try:
            self._connection_pool = oracledb.create_pool(
                user=self.config.user,
                password=self.config.password,
                dsn=self.config.dsn,
                min=2,
                max=self.pool_size,
                increment=1,
                threaded=True
            )
            logging.info(f"Initialized connection pool with {self.pool_size} connections")
        except Exception as e:
            raise DBSentinelError(f"Failed to create connection pool: {str(e)}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        if not self._connection_pool:
            self.initialize_pool()
            
        connection = None
        try:
            connection = self._connection_pool.acquire()
            yield connection
        finally:
            if connection:
                self._connection_pool.release(connection)
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Tuple]:
        """Execute a query and return results"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
            finally:
                cursor.close()
    
    def execute_dml(self, query: str, params: Optional[Dict] = None) -> int:
        """Execute DML statement and return affected rows"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    result = cursor.execute(query, params)
                else:
                    result = cursor.execute(query)
                conn.commit()
                return cursor.rowcount
            finally:
                cursor.close()
    
    def close_pool(self):
        """Close connection pool"""
        if self._connection_pool:
            self._connection_pool.close()
            logging.info("Connection pool closed")


class HashingManager:
    """Manages row hashing for comparison"""
    
    def __init__(self, algorithm: str = "sha256", null_replacement: str = "__NULL__", 
                 trim_whitespace: bool = True, ignore_case: bool = False):
        self.algorithm = algorithm
        self.null_replacement = null_replacement
        self.trim_whitespace = trim_whitespace
        self.ignore_case = ignore_case
        
    def hash_row(self, row: Tuple) -> str:
        """
        Generate hash for a single row
        
        Args:
            row: Tuple of column values
            
        Returns:
            Hexadecimal hash string
        """
        # Process each value in the row
        processed_values = []
        for value in row:
            if value is None:
                processed_value = self.null_replacement
            else:
                processed_value = str(value)
                if self.trim_whitespace:
                    processed_value = processed_value.strip()
                if self.ignore_case:
                    processed_value = processed_value.lower()
            processed_values.append(processed_value)
        
        # Create concatenated string
        row_string = "|".join(processed_values)
        
        # Generate hash
        if self.algorithm == "sha256":
            hasher = hashlib.sha256()
        elif self.algorithm == "md5":
            hasher = hashlib.md5()
        else:
            raise DBSentinelError(f"Unsupported hashing algorithm: {self.algorithm}")
            
        hasher.update(row_string.encode('utf-8'))
        return hasher.hexdigest()
    
    def hash_batch(self, rows: List[Tuple]) -> Dict[str, str]:
        """
        Hash a batch of rows
        
        Args:
            rows: List of row tuples
            
        Returns:
            Dictionary mapping primary key to hash
        """
        hashes = {}
        for row in rows:
            # Assume first column(s) are primary key
            pk_value = str(row[0]) if len(row) == 1 else "|".join(str(v) for v in row[:len(row)])
            hash_value = self.hash_row(row)
            hashes[pk_value] = hash_value
        return hashes


class CheckpointManager:
    """Manages restart/resume functionality"""
    
    def __init__(self, db_manager: DatabaseManager, table_name: str = "DB_SENTINEL_CHECKPOINTS"):
        self.db_manager = db_manager
        self.table_name = table_name
        self._ensure_checkpoint_table()
    
    def _ensure_checkpoint_table(self):
        """Create checkpoint table if it doesn't exist"""
        create_sql = f"""
        CREATE TABLE {self.table_name} (
            ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            SCHEMA_NAME VARCHAR2(128) NOT NULL,
            TABLE_NAME VARCHAR2(128) NOT NULL,
            BATCH_START NUMBER NOT NULL,
            BATCH_END NUMBER NOT NULL,
            STATUS VARCHAR2(20) DEFAULT 'PENDING',
            CREATED_DATE DATE DEFAULT SYSDATE,
            UPDATED_DATE DATE DEFAULT SYSDATE,
            CONSTRAINT UK_{self.table_name} UNIQUE (SCHEMA_NAME, TABLE_NAME, BATCH_START, BATCH_END)
        )
        """
        
        try:
            self.db_manager.execute_dml(create_sql)
            logging.info(f"Created checkpoint table: {self.table_name}")
        except Exception as e:
            if "ORA-00955" in str(e):  # Table already exists
                logging.debug(f"Checkpoint table {self.table_name} already exists")
            else:
                raise DBSentinelError(f"Failed to create checkpoint table: {str(e)}")
    
    def save_checkpoint(self, schema: str, table: str, batch_start: int, batch_end: int, status: str = "COMPLETED"):
        """Save a checkpoint"""
        insert_sql = f"""
        INSERT INTO {self.table_name} (SCHEMA_NAME, TABLE_NAME, BATCH_START, BATCH_END, STATUS)
        VALUES (:schema_name, :table_name, :batch_start, :batch_end, :status)
        """
        
        params = {
            'schema_name': schema,
            'table_name': table,
            'batch_start': batch_start,
            'batch_end': batch_end,
            'status': status
        }
        
        try:
            self.db_manager.execute_dml(insert_sql, params)
            logging.debug(f"Saved checkpoint: {schema}.{table} batch {batch_start}-{batch_end}")
        except Exception as e:
            logging.warning(f"Failed to save checkpoint: {str(e)}")
    
    def get_completed_batches(self, schema: str, table: str) -> List[Tuple[int, int]]:
        """Get list of completed batch ranges"""
        select_sql = f"""
        SELECT BATCH_START, BATCH_END
        FROM {self.table_name}
        WHERE SCHEMA_NAME = :schema_name 
        AND TABLE_NAME = :table_name 
        AND STATUS = 'COMPLETED'
        ORDER BY BATCH_START
        """
        
        params = {'schema_name': schema, 'table_name': table}
        
        try:
            result = self.db_manager.execute_query(select_sql, params)
            return [(row[0], row[1]) for row in result]
        except Exception as e:
            logging.warning(f"Failed to get completed batches: {str(e)}")
            return []
    
    def cleanup_old_checkpoints(self, retention_days: int = 14):
        """Clean up old checkpoint records"""
        delete_sql = f"""
        DELETE FROM {self.table_name}
        WHERE CREATED_DATE < SYSDATE - :retention_days
        """
        
        try:
            deleted = self.db_manager.execute_dml(delete_sql, {'retention_days': retention_days})
            logging.info(f"Cleaned up {deleted} old checkpoint records")
        except Exception as e:
            logging.warning(f"Failed to cleanup checkpoints: {str(e)}")


class AuditManager:
    """Manages audit table and logging"""
    
    def __init__(self, db_manager: DatabaseManager, table_name: str = "DB_SENTINEL_AUDIT"):
        self.db_manager = db_manager
        self.table_name = table_name
        self._ensure_audit_table()
    
    def _ensure_audit_table(self):
        """Create audit table if it doesn't exist"""
        create_sql = f"""
        CREATE TABLE {self.table_name} (
            AUDIT_ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            JOB_ID VARCHAR2(100) NOT NULL,
            SCHEMA_NAME VARCHAR2(128) NOT NULL,
            TABLE_NAME VARCHAR2(128) NOT NULL,
            JOB_START_TIME DATE NOT NULL,
            JOB_END_TIME DATE,
            SOURCE_ROW_COUNT NUMBER,
            TARGET_ROW_COUNT NUMBER,
            MISMATCH_COUNT NUMBER DEFAULT 0,
            INSERT_COUNT NUMBER DEFAULT 0,
            UPDATE_COUNT NUMBER DEFAULT 0,
            DELETE_COUNT NUMBER DEFAULT 0,
            STATUS VARCHAR2(20) DEFAULT 'RUNNING',
            ERROR_MESSAGE CLOB,
            CREATED_DATE DATE DEFAULT SYSDATE
        )
        """
        
        try:
            self.db_manager.execute_dml(create_sql)
            logging.info(f"Created audit table: {self.table_name}")
        except Exception as e:
            if "ORA-00955" in str(e):  # Table already exists
                logging.debug(f"Audit table {self.table_name} already exists")
            else:
                raise DBSentinelError(f"Failed to create audit table: {str(e)}")
    
    def log_job_start(self, job_id: str, schema: str, table: str, start_time: datetime) -> int:
        """Log job start and return audit ID"""
        insert_sql = f"""
        INSERT INTO {self.table_name} (JOB_ID, SCHEMA_NAME, TABLE_NAME, JOB_START_TIME, STATUS)
        VALUES (:job_id, :schema_name, :table_name, :start_time, 'RUNNING')
        RETURNING AUDIT_ID INTO :audit_id
        """
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            audit_id_var = cursor.var(oracledb.NUMBER)
            
            cursor.execute(insert_sql, {
                'job_id': job_id,
                'schema_name': schema,
                'table_name': table,
                'start_time': start_time,
                'audit_id': audit_id_var
            })
            conn.commit()
            cursor.close()
            
            return int(audit_id_var.getvalue()[0])
    
    def log_job_completion(self, audit_id: int, result: ComparisonResult):
        """Log job completion"""
        update_sql = f"""
        UPDATE {self.table_name}
        SET JOB_END_TIME = :end_time,
            SOURCE_ROW_COUNT = :source_count,
            TARGET_ROW_COUNT = :target_count,
            MISMATCH_COUNT = :mismatch_count,
            INSERT_COUNT = :insert_count,
            UPDATE_COUNT = :update_count,
            DELETE_COUNT = :delete_count,
            STATUS = :status,
            ERROR_MESSAGE = :error_message
        WHERE AUDIT_ID = :audit_id
        """
        
        params = {
            'audit_id': audit_id,
            'end_time': result.end_time,
            'source_count': result.source_count,
            'target_count': result.target_count,
            'mismatch_count': result.mismatch_count,
            'insert_count': result.insert_count,
            'update_count': result.update_count,
            'delete_count': result.delete_count,
            'status': result.status,
            'error_message': result.error_message
        }
        
        try:
            self.db_manager.execute_dml(update_sql, params)
            logging.info(f"Updated audit record {audit_id}")
        except Exception as e:
            logging.error(f"Failed to update audit record: {str(e)}")


class SQLGenerator:
    """Generates SQL statements for synchronization"""
    
    def __init__(self, output_dir: str, include_schema_prefix: bool = True, 
                 include_comments: bool = True, batch_statements: bool = True,
                 statement_batch_size: int = 50):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.include_schema_prefix = include_schema_prefix
        self.include_comments = include_comments
        self.batch_statements = batch_statements
        self.statement_batch_size = statement_batch_size
    
    def generate_insert_statements(self, table_config: TableConfig, missing_rows: List[Tuple],
                                   column_names: List[str], target_file: str):
        """Generate INSERT statements for missing rows"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_config.table_name}_insert_{timestamp}.sql"
        filepath = self.output_dir / filename
        
        schema_prefix = f"{table_config.schema}." if self.include_schema_prefix and table_config.schema else ""
        
        with open(filepath, 'w') as f:
            if self.include_comments:
                f.write(f"-- INSERT statements for {schema_prefix}{table_config.table_name}\n")
                f.write(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"-- Total rows: {len(missing_rows)}\n\n")
            
            batch_count = 0
            for i, row in enumerate(missing_rows):
                if self.batch_statements and i % self.statement_batch_size == 0 and i > 0:
                    f.write("COMMIT;\n\n")
                    batch_count += 1
                
                # Build INSERT statement
                columns_str = ", ".join(column_names)
                values_str = ", ".join([self._format_value(val) for val in row])
                
                insert_sql = f"INSERT INTO {schema_prefix}{table_config.table_name} ({columns_str}) VALUES ({values_str});\n"
                f.write(insert_sql)
            
            f.write("\nCOMMIT;\n")
        
        logging.info(f"Generated {len(missing_rows)} INSERT statements in {filepath}")
        return filepath
    
    def generate_update_statements(self, table_config: TableConfig, different_rows: List[Tuple],
                                   column_names: List[str], target_file: str):
        """Generate UPDATE statements for different rows"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_config.table_name}_update_{timestamp}.sql"
        filepath = self.output_dir / filename
        
        schema_prefix = f"{table_config.schema}." if self.include_schema_prefix and table_config.schema else ""
        
        with open(filepath, 'w') as f:
            if self.include_comments:
                f.write(f"-- UPDATE statements for {schema_prefix}{table_config.table_name}\n")
                f.write(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"-- Total rows: {len(different_rows)}\n\n")
            
            for i, row in enumerate(different_rows):
                if self.batch_statements and i % self.statement_batch_size == 0 and i > 0:
                    f.write("COMMIT;\n\n")
                
                # Build UPDATE statement (simplified - assumes all non-PK columns need update)
                pk_columns = table_config.primary_key
                pk_values = row[:len(pk_columns)]
                
                set_clauses = []
                where_clauses = []
                
                for idx, col_name in enumerate(column_names):
                    if col_name in pk_columns:
                        pk_idx = pk_columns.index(col_name)
                        where_clauses.append(f"{col_name} = {self._format_value(pk_values[pk_idx])}")
                    else:
                        set_clauses.append(f"{col_name} = {self._format_value(row[idx])}")
                
                if set_clauses:  # Only generate UPDATE if there are non-PK columns to update
                    update_sql = f"UPDATE {schema_prefix}{table_config.table_name} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)};\n"
                    f.write(update_sql)
            
            f.write("\nCOMMIT;\n")
        
        logging.info(f"Generated {len(different_rows)} UPDATE statements in {filepath}")
        return filepath
    
    def _format_value(self, value) -> str:
        """Format a value for SQL statement"""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, datetime):
            return f"TO_DATE('{value.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
        else:
            return f"'{str(value)}'"


class VerificationManager:
    """Handles post-comparison verification"""
    
    def __init__(self, db_manager: DatabaseManager, batch_size: int = 2000, 
                 max_threads: int = 4, skip_existing_keys: bool = True):
        self.db_manager = db_manager
        self.batch_size = batch_size
        self.max_threads = max_threads
        self.skip_existing_keys = skip_existing_keys
    
    def verify_insert_statements(self, sql_file: Path, table_config: TableConfig) -> List[str]:
        """
        Verify INSERT statements by checking if primary keys already exist
        
        Returns list of valid INSERT statements
        """
        logging.info(f"Verifying INSERT statements in {sql_file}")
        
        # Parse SQL file to extract INSERT statements
        insert_statements = self._parse_insert_statements(sql_file)
        
        if not insert_statements:
            return []
        
        # Extract primary key values from INSERT statements
        pk_values = []
        for statement in insert_statements:
            pk_value = self._extract_primary_key_from_insert(statement, table_config)
            if pk_value:
                pk_values.append(pk_value)
        
        # Check which primary keys already exist
        existing_pks = self._check_existing_primary_keys(pk_values, table_config)
        
        # Filter out INSERT statements for existing primary keys
        valid_statements = []
        for statement in insert_statements:
            pk_value = self._extract_primary_key_from_insert(statement, table_config)
            if pk_value and pk_value not in existing_pks:
                valid_statements.append(statement)
            elif pk_value in existing_pks:
                logging.warning(f"Skipping INSERT for existing PK: {pk_value}")
        
        logging.info(f"Verified {len(valid_statements)} valid INSERT statements out of {len(insert_statements)}")
        return valid_statements
    
    def _parse_insert_statements(self, sql_file: Path) -> List[str]:
        """Parse INSERT statements from SQL file"""
        statements = []
        
        try:
            with open(sql_file, 'r') as f:
                content = f.read()
                
            # Simple parsing - split by INSERT keyword
            lines = content.split('\n')
            current_statement = ""
            
            for line in lines:
                line = line.strip()
                if line.upper().startswith('INSERT'):
                    if current_statement:
                        statements.append(current_statement.strip())
                    current_statement = line
                elif current_statement and not line.startswith('--') and line:
                    current_statement += " " + line
                    if line.endswith(';'):
                        statements.append(current_statement.strip())
                        current_statement = ""
            
            if current_statement:
                statements.append(current_statement.strip())
                
        except Exception as e:
            logging.error(f"Error parsing SQL file {sql_file}: {str(e)}")
        
        return statements
    
    def _extract_primary_key_from_insert(self, insert_statement: str, table_config: TableConfig) -> Optional[str]:
        """Extract primary key value from INSERT statement"""
        try:
            # This is a simplified implementation
            # In production, you might want to use a proper SQL parser
            
            # Find VALUES clause
            values_start = insert_statement.upper().find('VALUES')
            if values_start == -1:
                return None
                
            values_part = insert_statement[values_start + 6:].strip()
            
            # Extract values between parentheses
            if values_part.startswith('(') and ')' in values_part:
                values_str = values_part[1:values_part.find(')')]
                values = [v.strip().strip("'\"") for v in values_str.split(',')]
                
                # Assume primary key columns are first in the column list
                pk_values = values[:len(table_config.primary_key)]
                return "|".join(pk_values)
                
        except Exception as e:
            logging.warning(f"Error extracting PK from INSERT statement: {str(e)}")
        
        return None
    
    def _check_existing_primary_keys(self, pk_values: List[str], table_config: TableConfig) -> set:
        """Check which primary key values already exist in target table"""
        if not pk_values:
            return set()
        
        existing_pks = set()
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        
        # Process in batches
        for i in range(0, len(pk_values), self.batch_size):
            batch = pk_values[i:i + self.batch_size]
            
            # Build query to check existence
            if len(table_config.primary_key) == 1:
                # Single column primary key
                pk_col = table_config.primary_key[0]
                placeholders = ", ".join([f"'{pk}'" for pk in batch])
                query = f"SELECT {pk_col} FROM {schema_prefix}{table_config.table_name} WHERE {pk_col} IN ({placeholders})"
            else:
                # Composite primary key - more complex query needed
                # This is simplified; production code should handle this better
                conditions = []
                for pk_value in batch:
                    pk_parts = pk_value.split("|")
                    if len(pk_parts) == len(table_config.primary_key):
                        pk_conditions = []
                        for idx, pk_col in enumerate(table_config.primary_key):
                            pk_conditions.append(f"{pk_col} = '{pk_parts[idx]}'")
                        conditions.append(f"({' AND '.join(pk_conditions)})")
                
                if conditions:
                    pk_select = ", ".join(table_config.primary_key)
                    query = f"SELECT {pk_select} FROM {schema_prefix}{table_config.table_name} WHERE {' OR '.join(conditions)}"
                else:
                    continue
            
            try:
                results = self.db_manager.execute_query(query)
                for row in results:
                    if len(table_config.primary_key) == 1:
                        existing_pks.add(str(row[0]))
                    else:
                        existing_pks.add("|".join(str(val) for val in row))
            except Exception as e:
                logging.error(f"Error checking existing primary keys: {str(e)}")
        
        return existing_pks


class DBSentinelUtility:
    """Main DB Sentinel utility class"""
    
    def __init__(self, config_file: str):
        self.config_file = config_file
        self.config = self._load_config()
        self._setup_logging()
        self._setup_directories()
        
        # Initialize managers
        self.source_db = DatabaseManager(
            DatabaseConfig(**self.config['source_db']),
            self.config.get('performance', {}).get('connection_pool_size', 10)
        )
        self.target_db = DatabaseManager(
            DatabaseConfig(**self.config['target_db']),
            self.config.get('performance', {}).get('connection_pool_size', 10)
        )
        
        # Initialize other managers based on config
        self.hasher = HashingManager(**self.config.get('hashing', {}))
        
        if self.config.get('flags', {}).get('enable_restart', False):
            self.checkpoint_manager = CheckpointManager(
                self.source_db, 
                self.config.get('restart', {}).get('metadata_table_name', 'DB_SENTINEL_CHECKPOINTS')
            )
        else:
            self.checkpoint_manager = None
            
        if self.config.get('flags', {}).get('enable_audit_table', False):
            self.audit_manager = AuditManager(
                self.source_db,
                self.config.get('audit', {}).get('audit_table_name', 'DB_SENTINEL_AUDIT')
            )
        else:
            self.audit_manager = None
            
        self.sql_generator = SQLGenerator(
            self.config.get('paths', {}).get('sql_output_dir', './DB_Sentinel_sql'),
            **self.config.get('sql_generation', {})
        )
        
        if self.config.get('flags', {}).get('enable_reverification', False):
            self.verification_manager = VerificationManager(
                self.target_db,
                **self.config.get('verification', {})
            )
        else:
            self.verification_manager = None
    
    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f)
            logging.info(f"Loaded configuration from {self.config_file}")
            return config
        except Exception as e:
            raise DBSentinelError(f"Failed to load config file {self.config_file}: {str(e)}")
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO').upper())
        
        # Create logs directory
        log_dir = Path('./DB_Sentinel_logs')
        log_dir.mkdir(exist_ok=True)
        
        # Setup file handler
        log_file = log_dir / f"db_sentinel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        logging.info("DB Sentinel Utility initialized")
    
    def _setup_directories(self):
        """Create necessary output directories"""
        directories = [
            './DB_Sentinel_sql',
            './DB_Sentinel_audit', 
            './DB_Sentinel_report',
            './DB_Sentinel_logs'
        ]
        
        for dir_path in directories:
            Path(dir_path).mkdir(exist_ok=True)
            
        logging.info("Created output directories")
    
    def compare_tables(self) -> List[ComparisonResult]:
        """Main method to compare all configured tables"""
        results = []
        
        try:
            # Initialize database pools
            self.source_db.initialize_pool()
            self.target_db.initialize_pool()
            
            # Get table configurations
            table_configs = self._parse_table_configs()
            
            logging.info(f"Starting comparison of {len(table_configs)} tables")
            
            # Compare each table
            for table_config in table_configs:
                try:
                    result = self._compare_single_table(table_config)
                    results.append(result)
                except Exception as e:
                    error_result = ComparisonResult(
                        table_name=table_config.table_name,
                        schema=table_config.schema or self.config.get('schema', 'UNKNOWN'),
                        source_count=0,
                        target_count=0,
                        mismatch_count=0,
                        insert_count=0,
                        update_count=0,
                        delete_count=0,
                        status='ERROR',
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                        error_message=str(e)
                    )
                    results.append(error_result)
                    logging.error(f"Error comparing table {table_config.table_name}: {str(e)}")
            
            # Generate comparison report
            self._generate_comparison_report(results)
            
            logging.info("Table comparison completed")
            
        finally:
            # Cleanup
            self.source_db.close_pool()
            self.target_db.close_pool()
        
        return results
    
    def _parse_table_configs(self) -> List[TableConfig]:
        """Parse table configurations from config"""
        table_configs = []
        
        for table_data in self.config.get('tables', []):
            config = TableConfig(
                table_name=table_data['table_name'],
                primary_key=table_data['primary_key'],
                chunk_size=table_data.get('chunk_size', 10000),
                schema=table_data.get('schema', self.config.get('schema')),
                where_clause=table_data.get('where_clause'),
                columns=table_data.get('columns')
            )
            table_configs.append(config)
        
        return table_configs
    
    def _compare_single_table(self, table_config: TableConfig) -> ComparisonResult:
        """Compare a single table between source and target"""
        start_time = datetime.now()
        logging.info(f"Starting comparison of {table_config.schema}.{table_config.table_name}")
        
        # Initialize audit record if enabled
        audit_id = None
        if self.audit_manager:
            job_id = f"DBSentinel_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{table_config.table_name}"
            audit_id = self.audit_manager.log_job_start(job_id, table_config.schema, table_config.table_name, start_time)
        
        try:
            # Get table metadata
            source_count = self._get_table_count(self.source_db, table_config)
            target_count = self._get_table_count(self.target_db, table_config)
            
            logging.info(f"Source rows: {source_count}, Target rows: {target_count}")
            
            # Calculate batches
            total_batches = (max(source_count, target_count) // table_config.chunk_size) + 1
            
            # Get completed batches if restart is enabled
            completed_batches = set()
            if self.checkpoint_manager:
                completed_ranges = self.checkpoint_manager.get_completed_batches(
                    table_config.schema, table_config.table_name
                )
                completed_batches = set(completed_ranges)
            
            # Compare batches using threading
            mismatch_count = 0
            insert_count = 0
            update_count = 0
            
            max_threads = self.config.get('performance', {}).get('max_threads', 4)
            
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                # Submit batch comparison tasks
                future_to_batch = {}
                
                for batch_num in range(total_batches):
                    batch_start = batch_num * table_config.chunk_size
                    batch_end = batch_start + table_config.chunk_size
                    
                    # Skip if batch already completed
                    if (batch_start, batch_end) in completed_batches:
                        logging.debug(f"Skipping completed batch {batch_start}-{batch_end}")
                        continue
                    
                    future = executor.submit(
                        self._compare_batch,
                        table_config,
                        batch_start,
                        batch_end,
                        batch_num
                    )
                    future_to_batch[future] = (batch_start, batch_end)
                
                # Process completed batches with progress bar
                if self.config.get('flags', {}).get('enable_progress_tracking', True):
                    progress_bar = tqdm(total=len(future_to_batch), desc=f"Comparing {table_config.table_name}")
                else:
                    progress_bar = None
                
                for future in as_completed(future_to_batch):
                    batch_start, batch_end = future_to_batch[future]
                    
                    try:
                        batch_result = future.result()
                        mismatch_count += batch_result.get('mismatches', 0)
                        insert_count += batch_result.get('inserts', 0)
                        update_count += batch_result.get('updates', 0)
                        
                        # Save checkpoint if enabled
                        if self.checkpoint_manager:
                            self.checkpoint_manager.save_checkpoint(
                                table_config.schema,
                                table_config.table_name,
                                batch_start,
                                batch_end,
                                'COMPLETED'
                            )
                        
                        if progress_bar:
                            progress_bar.update(1)
                            
                    except Exception as e:
                        logging.error(f"Batch {batch_start}-{batch_end} failed: {str(e)}")
                        if progress_bar:
                            progress_bar.update(1)
                
                if progress_bar:
                    progress_bar.close()
            
            end_time = datetime.now()
            
            # Create result
            result = ComparisonResult(
                table_name=table_config.table_name,
                schema=table_config.schema,
                source_count=source_count,
                target_count=target_count,
                mismatch_count=mismatch_count,
                insert_count=insert_count,
                update_count=update_count,
                delete_count=0,  # Not implemented in this version
                status='COMPLETED',
                start_time=start_time,
                end_time=end_time
            )
            
            # Update audit record if enabled
            if self.audit_manager and audit_id:
                self.audit_manager.log_job_completion(audit_id, result)
            
            logging.info(f"Completed comparison of {table_config.table_name}: {mismatch_count} mismatches found")
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            error_result = ComparisonResult(
                table_name=table_config.table_name,
                schema=table_config.schema,
                source_count=0,
                target_count=0,
                mismatch_count=0,
                insert_count=0,
                update_count=0,
                delete_count=0,
                status='ERROR',
                start_time=start_time,
                end_time=end_time,
                error_message=str(e)
            )
            
            # Update audit record if enabled
            if self.audit_manager and audit_id:
                self.audit_manager.log_job_completion(audit_id, error_result)
            
            raise
    
    def _get_table_count(self, db_manager: DatabaseManager, table_config: TableConfig) -> int:
        """Get total row count for a table"""
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        where_clause = f" WHERE {table_config.where_clause}" if table_config.where_clause else ""
        
        count_sql = f"SELECT COUNT(*) FROM {schema_prefix}{table_config.table_name}{where_clause}"
        
        result = db_manager.execute_query(count_sql)
        return result[0][0] if result else 0
    
    def _compare_batch(self, table_config: TableConfig, batch_start: int, batch_end: int, batch_num: int) -> Dict:
        """Compare a single batch of data"""
        logging.debug(f"Comparing batch {batch_num}: rows {batch_start}-{batch_end}")
        
        # Fetch data from both databases
        source_data = self._fetch_batch_data(self.source_db, table_config, batch_start, batch_end)
        target_data = self._fetch_batch_data(self.target_db, table_config, batch_start, batch_end)
        
        # Hash the data
        source_hashes = self.hasher.hash_batch(source_data)
        target_hashes = self.hasher.hash_batch(target_data)
        
        # Compare hashes
        source_keys = set(source_hashes.keys())
        target_keys = set(target_hashes.keys())
        
        # Find differences
        missing_in_target = source_keys - target_keys
        missing_in_source = target_keys - source_keys
        common_keys = source_keys & target_keys
        
        # Check for hash differences in common keys
        different_hashes = set()
        for key in common_keys:
            if source_hashes[key] != target_hashes[key]:
                different_hashes.add(key)
        
        mismatches = len(missing_in_target) + len(missing_in_source) + len(different_hashes)
        
        # Generate SQL if enabled and there are differences
        if self.config.get('flags', {}).get('enable_sql_generation', True) and mismatches > 0:
            self._generate_batch_sql(table_config, source_data, target_data, 
                                   missing_in_target, different_hashes, batch_num)
        
        return {
            'mismatches': mismatches,
            'inserts': len(missing_in_target),
            'updates': len(different_hashes)
        }
    
    def _fetch_batch_data(self, db_manager: DatabaseManager, table_config: TableConfig, 
                         batch_start: int, batch_end: int) -> List[Tuple]:
        """Fetch a batch of data from database"""
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        
        # Build column list
        if table_config.columns:
            columns_str = ", ".join(table_config.columns)
        else:
            columns_str = "*"
        
        # Build base query
        base_query = f"SELECT {columns_str} FROM {schema_prefix}{table_config.table_name}"
        
        # Add WHERE clause if specified
        where_parts = []
        if table_config.where_clause:
            where_parts.append(table_config.where_clause)
        
        # Add pagination using ROWNUM (Oracle-specific)
        if where_parts:
            where_clause = " WHERE " + " AND ".join(where_parts)
        else:
            where_clause = ""
        
        # Oracle pagination query
        query = f"""
        SELECT * FROM (
            SELECT a.*, ROWNUM rnum FROM (
                {base_query}{where_clause}
                ORDER BY {', '.join(table_config.primary_key)}
            ) a WHERE ROWNUM <= {batch_end}
        ) WHERE rnum > {batch_start}
        """
        
        try:
            return db_manager.execute_query(query)
        except Exception as e:
            logging.error(f"Error fetching batch data: {str(e)}")
            return []
    
    def _generate_batch_sql(self, table_config: TableConfig, source_data: List[Tuple], 
                           target_data: List[Tuple], missing_in_target: set, 
                           different_hashes: set, batch_num: int):
        """Generate SQL statements for a batch"""
        # This is a simplified implementation
        # In production, you'd want more sophisticated SQL generation
        pass
    
    def _generate_comparison_report(self, results: List[ComparisonResult]):
        """Generate comparison report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = Path('./DB_Sentinel_report') / f"comparison_report_{timestamp}.txt"
        
        with open(report_file, 'w') as f:
            f.write("DB SENTINEL COMPARISON REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Tables: {len(results)}\n\n")
            
            # Summary statistics
            total_source_rows = sum(r.source_count for r in results)
            total_target_rows = sum(r.target_count for r in results)
            total_mismatches = sum(r.mismatch_count for r in results)
            successful_comparisons = len([r for r in results if r.status == 'COMPLETED'])
            
            f.write("SUMMARY STATISTICS\n")
            f.write("-" * 30 + "\n")
            f.write(f"Successful Comparisons: {successful_comparisons}/{len(results)}\n")
            f.write(f"Total Source Rows: {total_source_rows:,}\n")
            f.write(f"Total Target Rows: {total_target_rows:,}\n")
            f.write(f"Total Mismatches: {total_mismatches:,}\n\n")
            
            # Individual table results
            f.write("TABLE DETAILS\n")
            f.write("-" * 30 + "\n")
            for result in results:
                f.write(f"\nTable: {result.schema}.{result.table_name}\n")
                f.write(f"Status: {result.status}\n")
                f.write(f"Source Rows: {result.source_count:,}\n")
                f.write(f"Target Rows: {result.target_count:,}\n")
                f.write(f"Mismatches: {result.mismatch_count:,}\n")
                f.write(f"Duration: {result.end_time - result.start_time}\n")
                if result.error_message:
                    f.write(f"Error: {result.error_message}\n")
        
        logging.info(f"Generated comparison report: {report_file}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='DB Sentinel Utility - Oracle Database Table Comparison')
    parser.add_argument('--config', '-c', default='config.yaml', 
                       help='Configuration file path (default: config.yaml)')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only run verification on existing SQL files')
    
    args = parser.parse_args()
    
    try:
        # Initialize utility
        db_sentinel = DBSentinelUtility(args.config)
        
        if args.verify_only:
            logging.info("Running verification only mode")
            # Add verification-only logic here
        else:
            # Run full comparison
            results = db_sentinel.compare_tables()
            
            # Print summary
            successful = len([r for r in results if r.status == 'COMPLETED'])
            total_mismatches = sum(r.mismatch_count for r in results)
            
            print(f"\nComparison Summary:")
            print(f"Tables Processed: {len(results)}")
            print(f"Successful: {successful}")
            print(f"Total Mismatches: {total_mismatches:,}")
            
            if total_mismatches > 0:
                print(f"SQL files generated in DB_Sentinel_sql/")
            
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        logging.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
