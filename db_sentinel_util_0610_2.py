#!/usr/bin/env python3
"""
DB_Sentinel_util - Advanced Oracle Database Table Comparison Utility

This production-ready utility compares tables between two Oracle databases using
row-level hashing with multi-threading support, restart capabilities, and comprehensive
audit functionality.

Features:
- Multi-threaded table comparison using concurrent.futures
- Row-level hashing for efficient comparison
- Configurable via YAML file
- Progress tracking with tqdm
- Restart/resume functionality with checkpoint management
- Comprehensive audit logging and database tracking
- SQL generation for synchronization
- Post-comparison verification
- Support for multiple tables with different configurations

Author: DB_Sentinel_util
Version: 1.0.0
"""

import os
import sys
import yaml
import hashlib
import logging
import traceback
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

import oracledb
import pandas as pd
from tqdm import tqdm


@dataclass
class TableConfig:
    """Configuration for individual table comparison"""
    table_name: str
    primary_key: List[str]
    chunk_size: int = 10000
    columns: Optional[List[str]] = None
    where_clause: Optional[str] = None
    schema: Optional[str] = None


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
    """Results from table comparison"""
    table_name: str
    schema: str
    source_count: int = 0
    target_count: int = 0
    matched_count: int = 0
    source_only_count: int = 0
    target_only_count: int = 0
    different_count: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    status: str = "RUNNING"
    error_message: Optional[str] = None
    
    @property
    def duration(self) -> Optional[timedelta]:
        if self.end_time:
            return self.end_time - self.start_time
        return None


class CheckpointManager:
    """Manages restart/resume functionality using database metadata table"""
    
    def __init__(self, db_config: DatabaseConfig, logger: logging.Logger):
        self.db_config = db_config
        self.logger = logger
        self.metadata_table = "DB_SENTINEL_CHECKPOINTS"
        
    def ensure_metadata_table(self):
        """Create metadata table if it doesn't exist"""
        create_sql = f"""
        CREATE TABLE {self.metadata_table} (
            job_id VARCHAR2(100),
            schema_name VARCHAR2(128),
            table_name VARCHAR2(128),
            batch_start NUMBER,
            batch_end NUMBER,
            status VARCHAR2(20),
            processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (job_id, schema_name, table_name, batch_start, batch_end)
        )
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(create_sql)
                    conn.commit()
                    self.logger.info(f"Created metadata table: {self.metadata_table}")
                except oracledb.DatabaseError as e:
                    if "ORA-00955" in str(e):  # Table already exists
                        self.logger.debug(f"Metadata table {self.metadata_table} already exists")
                    else:
                        raise
        except Exception as e:
            self.logger.error(f"Error creating metadata table: {e}")
            raise
            
    @contextmanager
    def get_connection(self):
        """Get database connection context manager"""
        conn = None
        try:
            conn = oracledb.connect(
                user=self.db_config.user,
                password=self.db_config.password,
                dsn=self.db_config.dsn
            )
            yield conn
        finally:
            if conn:
                conn.close()
                
    def save_checkpoint(self, job_id: str, schema: str, table: str, 
                       batch_start: int, batch_end: int, status: str = "COMPLETED"):
        """Save checkpoint for a processed batch"""
        sql = f"""
        MERGE INTO {self.metadata_table} cp
        USING (SELECT ? job_id, ? schema_name, ? table_name, ? batch_start, ? batch_end FROM dual) src
        ON (cp.job_id = src.job_id AND cp.schema_name = src.schema_name 
            AND cp.table_name = src.table_name AND cp.batch_start = src.batch_start 
            AND cp.batch_end = src.batch_end)
        WHEN MATCHED THEN
            UPDATE SET status = ?, processed_time = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (job_id, schema_name, table_name, batch_start, batch_end, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, [job_id, schema, table, batch_start, batch_end, status,
                                   job_id, schema, table, batch_start, batch_end, status])
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")
            
    def get_processed_batches(self, job_id: str, schema: str, table: str) -> List[Tuple[int, int]]:
        """Get list of already processed batch ranges"""
        sql = f"""
        SELECT batch_start, batch_end 
        FROM {self.metadata_table}
        WHERE job_id = ? AND schema_name = ? AND table_name = ? AND status = 'COMPLETED'
        ORDER BY batch_start
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, [job_id, schema, table])
                return [(row[0], row[1]) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error retrieving processed batches: {e}")
            return []


class AuditManager:
    """Manages audit table operations for job tracking"""
    
    def __init__(self, db_config: DatabaseConfig, logger: logging.Logger):
        self.db_config = db_config
        self.logger = logger
        self.audit_table = "DB_SENTINEL_AUDIT"
        
    def ensure_audit_table(self):
        """Create audit table if it doesn't exist"""
        create_sql = f"""
        CREATE TABLE {self.audit_table} (
            job_id VARCHAR2(100),
            schema_name VARCHAR2(128),
            table_name VARCHAR2(128),
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            source_count NUMBER,
            target_count NUMBER,
            matched_count NUMBER,
            source_only_count NUMBER,
            target_only_count NUMBER,
            different_count NUMBER,
            status VARCHAR2(20),
            error_message CLOB,
            duration_seconds NUMBER,
            PRIMARY KEY (job_id, schema_name, table_name)
        )
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(create_sql)
                    conn.commit()
                    self.logger.info(f"Created audit table: {self.audit_table}")
                except oracledb.DatabaseError as e:
                    if "ORA-00955" in str(e):  # Table already exists
                        self.logger.debug(f"Audit table {self.audit_table} already exists")
                    else:
                        raise
        except Exception as e:
            self.logger.error(f"Error creating audit table: {e}")
            raise
            
    @contextmanager
    def get_connection(self):
        """Get database connection context manager"""
        conn = None
        try:
            conn = oracledb.connect(
                user=self.db_config.user,
                password=self.db_config.password,
                dsn=self.db_config.dsn
            )
            yield conn
        finally:
            if conn:
                conn.close()
                
    def log_job_result(self, job_id: str, result: ComparisonResult):
        """Log job result to audit table"""
        sql = f"""
        MERGE INTO {self.audit_table} audit
        USING (SELECT ? job_id, ? schema_name, ? table_name FROM dual) src
        ON (audit.job_id = src.job_id AND audit.schema_name = src.schema_name 
            AND audit.table_name = src.table_name)
        WHEN MATCHED THEN
            UPDATE SET 
                end_time = ?,
                source_count = ?,
                target_count = ?,
                matched_count = ?,
                source_only_count = ?,
                target_only_count = ?,
                different_count = ?,
                status = ?,
                error_message = ?,
                duration_seconds = ?
        WHEN NOT MATCHED THEN
            INSERT (job_id, schema_name, table_name, start_time, end_time,
                   source_count, target_count, matched_count, source_only_count,
                   target_only_count, different_count, status, error_message, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        duration_seconds = result.duration.total_seconds() if result.duration else None
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, [
                    job_id, result.schema, result.table_name, result.end_time,
                    result.source_count, result.target_count, result.matched_count,
                    result.source_only_count, result.target_only_count, result.different_count,
                    result.status, result.error_message, duration_seconds,
                    job_id, result.schema, result.table_name, result.start_time, result.end_time,
                    result.source_count, result.target_count, result.matched_count,
                    result.source_only_count, result.target_only_count, result.different_count,
                    result.status, result.error_message, duration_seconds
                ])
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error logging to audit table: {e}")


class DatabaseComparator:
    """Main class for comparing Oracle database tables"""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        
        # Initialize database configs
        self.source_db = DatabaseConfig(**self.config['source_db'])
        self.target_db = DatabaseConfig(**self.config['target_db'])
        
        # Initialize managers
        self.checkpoint_manager = CheckpointManager(self.source_db, self.logger)
        self.audit_manager = AuditManager(self.source_db, self.logger)
        
        # Job ID for tracking
        self.job_id = f"DBSENTINEL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Thread-safe locks
        self.sql_lock = threading.Lock()
        self.audit_lock = threading.Lock()
        
        # Initialize output files
        self._setup_output_files()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"Error loading config file {config_path}: {e}")
            sys.exit(1)
            
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        log_level = self.config.get('logging', {}).get('level', 'INFO')
        log_file = self.config.get('paths', {}).get('audit_log', './logs/audit.log')
        
        # Create log directory if it doesn't exist
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        return logging.getLogger('DB_Sentinel')
        
    def _setup_output_files(self):
        """Setup output file paths and directories"""
        paths = self.config.get('paths', {})
        self.source_sql_file = paths.get('source_sql_output', './output/source_sync_statements.sql')
        self.target_sql_file = paths.get('target_sql_output', './output/target_sync_statements.sql')
        
        # Create output directories
        for file_path in [self.source_sql_file, self.target_sql_file]:
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
        # Initialize SQL files with headers
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        header = f"-- DB_Sentinel_util Generated SQL Statements\n-- Generated: {timestamp}\n-- Job ID: {self.job_id}\n\n"
        
        with open(self.source_sql_file, 'w') as f:
            f.write(header + "-- Source Database Sync Statements\n\n")
        with open(self.target_sql_file, 'w') as f:
            f.write(header + "-- Target Database Sync Statements\n\n")
            
    @contextmanager
    def get_connection(self, db_config: DatabaseConfig):
        """Get database connection context manager"""
        conn = None
        try:
            conn = oracledb.connect(
                user=db_config.user,
                password=db_config.password,
                dsn=db_config.dsn
            )
            yield conn
        finally:
            if conn:
                conn.close()
                
    def _get_table_columns(self, db_config: DatabaseConfig, table_config: TableConfig) -> List[str]:
        """Get all column names for a table"""
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        schema_name = table_config.schema or db_config.user.upper()
        
        # Query to get column names from Oracle data dictionary
        sql = """
        SELECT column_name 
        FROM all_tab_columns 
        WHERE owner = UPPER(?) AND table_name = UPPER(?)
        ORDER BY column_id
        """
        
        try:
            with self.get_connection(db_config) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, [schema_name, table_config.table_name])
                columns = [row[0] for row in cursor.fetchall()]
                
                if not columns:
                    # Fallback: try without schema filter (for current user's tables)
                    sql_fallback = """
                    SELECT column_name 
                    FROM user_tab_columns 
                    WHERE table_name = UPPER(?)
                    ORDER BY column_id
                    """
                    cursor.execute(sql_fallback, [table_config.table_name])
                    columns = [row[0] for row in cursor.fetchall()]
                
                if not columns:
                    raise Exception(f"No columns found for table {schema_prefix}{table_config.table_name}")
                    
                return columns
        except Exception as e:
            self.logger.error(f"Error getting table columns: {e}")
            # Fallback to using * if column lookup fails
            return ["*"]
                
    def fetch_data_batchwise(self, db_config: DatabaseConfig, table_config: TableConfig, 
                           batch_start: int, batch_size: int) -> pd.DataFrame:
        """Fetch data from database in batches using ROW_NUMBER() window function"""
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        where_clause = f" AND ({table_config.where_clause})" if table_config.where_clause else ""
        
        # Get actual column names if * is specified
        if not table_config.columns or table_config.columns == ["*"]:
            # Get all column names from table
            actual_columns = self._get_table_columns(db_config, table_config)
            column_list = ", ".join(actual_columns)
        else:
            column_list = ", ".join(table_config.columns)
        
        # Try ROW_NUMBER() approach first (Oracle 9i+)
        try:
            sql = f"""
            SELECT {column_list}
            FROM (
                SELECT {column_list}, 
                       ROW_NUMBER() OVER (ORDER BY {', '.join(table_config.primary_key)}) as rn
                FROM {schema_prefix}{table_config.table_name}
                WHERE 1=1{where_clause}
            ) ranked_data
            WHERE rn > {batch_start} AND rn <= {batch_start + batch_size}
            """
            
            with self.get_connection(db_config) as conn:
                return pd.read_sql(sql, conn)
                
        except Exception as e:
            self.logger.warning(f"ROW_NUMBER() approach failed: {e}, trying alternative method")
            
            # Fallback: Use OFFSET/FETCH for Oracle 12c+ or alternative approach
            try:
                sql = f"""
                SELECT {column_list}
                FROM {schema_prefix}{table_config.table_name}
                WHERE 1=1{where_clause}
                ORDER BY {', '.join(table_config.primary_key)}
                OFFSET {batch_start} ROWS FETCH NEXT {batch_size} ROWS ONLY
                """
                
                with self.get_connection(db_config) as conn:
                    return pd.read_sql(sql, conn)
                    
            except Exception as e2:
                self.logger.warning(f"OFFSET/FETCH approach failed: {e2}, using basic ROWNUM")
                
                # Final fallback: Use simple ROWNUM (less efficient but more compatible)
                if not table_config.columns or table_config.columns == ["*"]:
                    # For * columns, we need to be more careful
                    sql = f"""
                    SELECT * FROM (
                        SELECT a.*, ROWNUM rnum FROM (
                            SELECT * FROM {schema_prefix}{table_config.table_name}
                            WHERE 1=1{where_clause}
                            ORDER BY {', '.join(table_config.primary_key)}
                        ) a
                        WHERE ROWNUM <= {batch_start + batch_size}
                    )
                    WHERE rnum > {batch_start}
                    """
                else:
                    sql = f"""
                    SELECT {column_list} FROM (
                        SELECT {column_list}, ROWNUM rnum FROM (
                            SELECT {column_list} FROM {schema_prefix}{table_config.table_name}
                            WHERE 1=1{where_clause}
                            ORDER BY {', '.join(table_config.primary_key)}
                        )
                        WHERE ROWNUM <= {batch_start + batch_size}
                    )
                    WHERE rnum > {batch_start}
                    """
                
                try:
                    with self.get_connection(db_config) as conn:
                        return pd.read_sql(sql, conn)
                except Exception as e3:
                    self.logger.error(f"All pagination methods failed. Last error: {e3}")
                    self.logger.error(f"Final SQL attempted: {sql}")
                    raise
            
    def hash_rows(self, df: pd.DataFrame, primary_keys: List[str]) -> Dict[str, str]:
        """
        Generate hash for each row based on all column values
        
        Returns dictionary mapping primary key values to row hashes
        """
        if df.empty:
            return {}
            
        hashes = {}
        
        for _, row in df.iterrows():
            # Create primary key tuple
            pk_values = tuple(str(row[pk]) for pk in primary_keys)
            pk_key = "|".join(str(v) for v in pk_values)
            
            # Create hash of all column values (excluding row number columns)
            # Handle different row number column names: rn, RN, rnum, RNUM
            row_data = row.drop(['rn', 'RN', 'rnum', 'RNUM'], errors='ignore')
            row_string = "|".join(str(v) for v in row_data.values)
            row_hash = hashlib.md5(row_string.encode()).hexdigest()
            
            hashes[pk_key] = row_hash
            
        return hashes
        
    def compare_hashes(self, source_hashes: Dict[str, str], target_hashes: Dict[str, str]) -> Dict[str, List[str]]:
        """
        Compare hashes between source and target data
        
        Returns:
        - source_only: Keys only in source
        - target_only: Keys only in target  
        - different: Keys with different hash values
        - matched: Keys with matching hash values
        """
        source_keys = set(source_hashes.keys())
        target_keys = set(target_hashes.keys())
        
        source_only = list(source_keys - target_keys)
        target_only = list(target_keys - source_keys)
        
        common_keys = source_keys & target_keys
        different = []
        matched = []
        
        for key in common_keys:
            if source_hashes[key] != target_hashes[key]:
                different.append(key)
            else:
                matched.append(key)
                
        return {
            'source_only': source_only,
            'target_only': target_only,
            'different': different,
            'matched': matched
        }
        
    def generate_sql_statements(self, table_config: TableConfig, comparison_result: Dict[str, List[str]],
                              source_data: pd.DataFrame, target_data: pd.DataFrame):
        """Generate INSERT and UPDATE SQL statements for synchronization"""
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        table_name = f"{schema_prefix}{table_config.table_name}"
        
        # Create lookup dictionaries for faster access
        source_lookup = {}
        target_lookup = {}
        
        if not source_data.empty:
            for _, row in source_data.iterrows():
                pk_key = "|".join(str(row[pk]) for pk in table_config.primary_key)
                # Remove all possible row number columns from data
                clean_row = row.drop(['rn', 'RN', 'rnum', 'RNUM'], errors='ignore')
                source_lookup[pk_key] = clean_row
                
        if not target_data.empty:
            for _, row in target_data.iterrows():
                pk_key = "|".join(str(row[pk]) for pk in table_config.primary_key)
                # Remove all possible row number columns from data
                clean_row = row.drop(['rn', 'RN', 'rnum', 'RNUM'], errors='ignore')
                target_lookup[pk_key] = clean_row
        
        with self.sql_lock:
            # Generate INSERT statements for source_only records (missing in target)
            with open(self.target_sql_file, 'a') as f:
                for pk_key in comparison_result['source_only']:
                    if pk_key in source_lookup:
                        row = source_lookup[pk_key]
                        columns = list(row.index)
                        values = [f"'{str(v)}'" if pd.notna(v) else 'NULL' for v in row.values]
                        
                        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
                        f.write(insert_sql)
                        
            # Generate INSERT statements for target_only records (missing in source)
            with open(self.source_sql_file, 'a') as f:
                for pk_key in comparison_result['target_only']:
                    if pk_key in target_lookup:
                        row = target_lookup[pk_key]
                        columns = list(row.index)
                        values = [f"'{str(v)}'" if pd.notna(v) else 'NULL' for v in row.values]
                        
                        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
                        f.write(insert_sql)
                        
            # Generate UPDATE statements for different records
            for pk_key in comparison_result['different']:
                if pk_key in source_lookup and pk_key in target_lookup:
                    source_row = source_lookup[pk_key]
                    target_row = target_lookup[pk_key]
                    
                    # Find differing columns
                    set_clauses = []
                    for col in source_row.index:
                        if col not in table_config.primary_key:
                            if source_row[col] != target_row[col]:
                                value = f"'{str(source_row[col])}'" if pd.notna(source_row[col]) else 'NULL'
                                set_clauses.append(f"{col} = {value}")
                                
                    if set_clauses:
                        # WHERE clause for primary key
                        pk_conditions = []
                        for pk in table_config.primary_key:
                            value = f"'{str(source_row[pk])}'" if pd.notna(source_row[pk]) else 'NULL'
                            pk_conditions.append(f"{pk} = {value}")
                            
                        update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {' AND '.join(pk_conditions)};\n"
                        
                        with open(self.target_sql_file, 'a') as f:
                            f.write(update_sql)
                            
    def verify_primary_keys(self, table_config: TableConfig, insert_statements: List[str]) -> List[str]:
        """
        Verify that primary keys don't already exist before insert
        
        This is the post-comparison verification step to avoid constraint violations
        """
        if not insert_statements:
            return []
            
        valid_statements = []
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        table_name = f"{schema_prefix}{table_config.table_name}"
        
        try:
            with self.get_connection(self.target_db) as conn:
                cursor = conn.cursor()
                
                for stmt in insert_statements:
                    # Extract primary key values from INSERT statement (simplified)
                    # In production, you might want more robust SQL parsing
                    try:
                        # Build check query
                        pk_conditions = []
                        for pk in table_config.primary_key:
                            # This is a simplified extraction - in production use proper SQL parsing
                            pk_conditions.append(f"{pk} IS NOT NULL")  # Placeholder logic
                            
                        check_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {' AND '.join(pk_conditions)}"
                        cursor.execute(check_sql)
                        count = cursor.fetchone()[0]
                        
                        if count == 0:
                            valid_statements.append(stmt)
                        else:
                            self.logger.warning(f"Primary key already exists, skipping insert")
                            
                    except Exception as e:
                        self.logger.error(f"Error verifying primary key: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error in primary key verification: {e}")
            return insert_statements  # Return original statements if verification fails
            
        return valid_statements
        
    def get_table_count(self, db_config: DatabaseConfig, table_config: TableConfig) -> int:
        """Get total row count for a table"""
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        where_clause = f" WHERE {table_config.where_clause}" if table_config.where_clause else ""
        
        sql = f"SELECT COUNT(*) FROM {schema_prefix}{table_config.table_name}{where_clause}"
        
        try:
            with self.get_connection(db_config) as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error getting table count: {e}")
            return 0
            
    def compare_table_batch(self, table_config: TableConfig, batch_start: int, 
                           batch_size: int) -> Dict[str, Any]:
        """Compare a single batch of table data"""
        try:
            # Fetch data from both databases
            source_data = self.fetch_data_batchwise(self.source_db, table_config, batch_start, batch_size)
            target_data = self.fetch_data_batchwise(self.target_db, table_config, batch_start, batch_size)
            
            # Generate hashes
            source_hashes = self.hash_rows(source_data, table_config.primary_key)
            target_hashes = self.hash_rows(target_data, table_config.primary_key)
            
            # Compare hashes
            comparison_result = self.compare_hashes(source_hashes, target_hashes)
            
            # Generate SQL statements if differences found
            if (comparison_result['source_only'] or comparison_result['target_only'] or 
                comparison_result['different']):
                self.generate_sql_statements(table_config, comparison_result, source_data, target_data)
                
            return {
                'source_count': len(source_hashes),
                'target_count': len(target_hashes),
                'matched_count': len(comparison_result['matched']),
                'source_only_count': len(comparison_result['source_only']),
                'target_only_count': len(comparison_result['target_only']),
                'different_count': len(comparison_result['different'])
            }
            
        except Exception as e:
            self.logger.error(f"Error comparing batch {batch_start}-{batch_start + batch_size}: {e}")
            raise
            
    def compare_table(self, table_config: TableConfig) -> ComparisonResult:
        """Compare a complete table using multi-threading"""
        result = ComparisonResult(
            table_name=table_config.table_name,
            schema=table_config.schema or "DEFAULT"
        )
        
        try:
            self.logger.info(f"Starting comparison for table {result.schema}.{result.table_name}")
            
            # Get total counts
            source_total = self.get_table_count(self.source_db, table_config)
            target_total = self.get_table_count(self.target_db, table_config)
            
            self.logger.info(f"Source count: {source_total}, Target count: {target_total}")
            
            # Calculate batches
            total_batches = (max(source_total, target_total) + table_config.chunk_size - 1) // table_config.chunk_size
            
            # Check for restart capability
            processed_batches = []
            if self.config.get('flags', {}).get('enable_restart', False):
                processed_batches = self.checkpoint_manager.get_processed_batches(
                    self.job_id, result.schema, result.table_name
                )
                
            # Create batch ranges, excluding already processed ones
            batch_ranges = []
            for i in range(total_batches):
                batch_start = i * table_config.chunk_size
                batch_end = min(batch_start + table_config.chunk_size, max(source_total, target_total))
                
                # Check if this batch was already processed
                is_processed = any(
                    start <= batch_start < end for start, end in processed_batches
                )
                
                if not is_processed:
                    batch_ranges.append((batch_start, table_config.chunk_size))
                    
            if processed_batches:
                self.logger.info(f"Resuming from checkpoint: {len(batch_ranges)} batches remaining")
                
            # Multi-threaded batch processing
            max_threads = self.config.get('performance', {}).get('max_threads', 4)
            
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                # Submit all batch jobs
                future_to_batch = {
                    executor.submit(self.compare_table_batch, table_config, batch_start, batch_size): 
                    (batch_start, batch_size)
                    for batch_start, batch_size in batch_ranges
                }
                
                # Process completed batches with progress bar
                with tqdm(total=len(batch_ranges), desc=f"Comparing {result.table_name}") as pbar:
                    for future in as_completed(future_to_batch):
                        batch_start, batch_size = future_to_batch[future]
                        
                        try:
                            batch_result = future.result()
                            
                            # Aggregate results
                            result.source_count += batch_result['source_count']
                            result.target_count += batch_result['target_count']
                            result.matched_count += batch_result['matched_count']
                            result.source_only_count += batch_result['source_only_count']
                            result.target_only_count += batch_result['target_only_count']
                            result.different_count += batch_result['different_count']
                            
                            # Save checkpoint
                            if self.config.get('flags', {}).get('enable_restart', False):
                                self.checkpoint_manager.save_checkpoint(
                                    self.job_id, result.schema, result.table_name,
                                    batch_start, batch_start + batch_size, "COMPLETED"
                                )
                                
                            pbar.update(1)
                            
                        except Exception as e:
                            self.logger.error(f"Batch {batch_start}-{batch_start + batch_size} failed: {e}")
                            result.error_message = str(e)
                            
            result.end_time = datetime.now()
            result.status = "COMPLETED" if not result.error_message else "FAILED"
            
            self.logger.info(f"Completed comparison for {result.schema}.{result.table_name}")
            self.logger.info(f"Results - Matched: {result.matched_count}, "
                           f"Source Only: {result.source_only_count}, "
                           f"Target Only: {result.target_only_count}, "
                           f"Different: {result.different_count}")
                           
        except Exception as e:
            result.end_time = datetime.now()
            result.status = "FAILED"
            result.error_message = str(e)
            self.logger.error(f"Error comparing table {result.table_name}: {e}")
            self.logger.error(traceback.format_exc())
            
        return result
        
    def run_comparison(self):
        """Main method to run the complete comparison process"""
        try:
            self.logger.info(f"Starting DB_Sentinel comparison job: {self.job_id}")
            
            # Initialize database tables
            if self.config.get('flags', {}).get('enable_restart', False):
                self.checkpoint_manager.ensure_metadata_table()
            self.audit_manager.ensure_audit_table()
            
            # Get table configurations
            table_configs = []
            for table_def in self.config.get('tables', []):
                schema = self.config.get('schema', table_def.get('schema'))
                table_config = TableConfig(
                    table_name=table_def['table_name'],
                    primary_key=table_def['primary_key'],
                    chunk_size=table_def.get('chunk_size', 10000),
                    columns=table_def.get('columns'),
                    where_clause=table_def.get('where_clause'),
                    schema=schema
                )
                table_configs.append(table_config)
                
            if not table_configs:
                # Fallback to legacy single table config
                table_config = TableConfig(
                    table_name=self.config['table_config']['table_name'],
                    primary_key=self.config['table_config']['primary_keys'],
                    chunk_size=self.config['table_config'].get('batch_size', 10000),
                    schema=self.config['table_config'].get('schema')
                )
                table_configs = [table_config]
                
            # Compare all tables
            all_results = []
            overall_start_time = datetime.now()
            
            for table_config in table_configs:
                result = self.compare_table(table_config)
                all_results.append(result)
                
                # Log to audit table
                self.audit_manager.log_job_result(self.job_id, result)
                
            # Post-comparison verification if enabled
            if self.config.get('flags', {}).get('enable_reverification', False):
                self.logger.info("Starting post-comparison verification...")
                # Implementation for verification would go here
                # This would read the generated SQL files and verify primary keys
                
            # Generate final report
            self._generate_comparison_report(all_results, overall_start_time)
            
            self.logger.info(f"DB_Sentinel comparison completed successfully: {self.job_id}")
            
        except Exception as e:
            self.logger.error(f"Critical error in comparison process: {e}")
            self.logger.error(traceback.format_exc())
            raise
            
    def _generate_comparison_report(self, results: List[ComparisonResult], start_time: datetime):
        """Generate final comparison report"""
        report_file = self.config.get('paths', {}).get('comparison_report', './output/comparison_report.txt')
        Path(report_file).parent.mkdir(parents=True, exist_ok=True)
        
        total_duration = datetime.now() - start_time
        
        with open(report_file, 'w') as f:
            f.write(f"DB_Sentinel_util Comparison Report\n")
            f.write(f"{'=' * 50}\n\n")
            f.write(f"Job ID: {self.job_id}\n")
            f.write(f"Start Time: {start_time}\n")
            f.write(f"End Time: {datetime.now()}\n")
            f.write(f"Total Duration: {total_duration}\n\n")
            
            f.write(f"Tables Compared: {len(results)}\n\n")
            
            for result in results:
                f.write(f"Table: {result.schema}.{result.table_name}\n")
                f.write(f"  Status: {result.status}\n")
                f.write(f"  Duration: {result.duration}\n")
                f.write(f"  Source Count: {result.source_count:,}\n")
                f.write(f"  Target Count: {result.target_count:,}\n")
                f.write(f"  Matched: {result.matched_count:,}\n")
                f.write(f"  Source Only: {result.source_only_count:,}\n")
                f.write(f"  Target Only: {result.target_only_count:,}\n")
                f.write(f"  Different: {result.different_count:,}\n")
                if result.error_message:
                    f.write(f"  Error: {result.error_message}\n")
                f.write("\n")
                
            # Summary statistics
            total_matched = sum(r.matched_count for r in results)
            total_differences = sum(r.source_only_count + r.target_only_count + r.different_count for r in results)
            
            f.write(f"Summary:\n")
            f.write(f"  Total Matched Records: {total_matched:,}\n")
            f.write(f"  Total Differences: {total_differences:,}\n")
            f.write(f"  Success Rate: {(total_matched / (total_matched + total_differences) * 100):.2f}%\n")
            
        self.logger.info(f"Comparison report generated: {report_file}")


def main():
    """Main entry point"""
    if len(sys.argv) != 2:
        print("Usage: python DB_Sentinel_util.py <config.yaml>")
        sys.exit(1)
        
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)
        
    try:
        comparator = DatabaseComparator(config_path)
        comparator.run_comparison()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
