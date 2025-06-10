#!/usr/bin/env python3
"""
Oracle Database Table Comparison Tool
=====================================

A production-ready script for comparing tables between two Oracle databases
with advanced features including multi-threading, restart capability, and
comprehensive auditing.

Features:
- Row-level hashing for accurate comparison
- Multi-threaded processing for performance
- Configurable via YAML
- Restart/resume capability
- Progress tracking with tqdm
- SQL generation for synchronization
- Comprehensive audit logging
- Post-comparison verification

Author: Auto-generated
Version: 1.0.0
"""

import os
import sys
import yaml
import hashlib
import logging
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

import oracledb
import pandas as pd
from tqdm import tqdm


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    user: str
    password: str
    dsn: str


@dataclass
class TableConfig:
    """Table comparison configuration."""
    schema: str
    table_name: str
    primary_keys: List[str]
    batch_size: int
    max_threads: int


@dataclass
class BatchInfo:
    """Information about a processing batch."""
    batch_id: int
    start_row: int
    end_row: int
    status: str = 'pending'
    processed_time: Optional[datetime] = None


class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection = None
        self._lock = threading.Lock()
    
    def get_connection(self):
        """Get a thread-safe database connection."""
        with self._lock:
            if self._connection is None or not self._connection.ping():
                self._connection = oracledb.connect(
                    user=self.config.user,
                    password=self.config.password,
                    dsn=self.config.dsn
                )
            return self._connection
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        conn = self.get_connection()
        try:
            df = pd.read_sql(query, conn, params=params or {})
            return df
        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            raise
    
    def execute_non_query(self, query: str, params: Optional[Dict] = None):
        """Execute a non-query statement (INSERT, UPDATE, DELETE)."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params or {})
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Non-query execution failed: {e}")
            raise
        finally:
            cursor.close()
    
    def close(self):
        """Close the database connection."""
        with self._lock:
            if self._connection:
                self._connection.close()
                self._connection = None


class HashGenerator:
    """Generates consistent hashes for database rows."""
    
    @staticmethod
    def hash_row(row_data: pd.Series) -> str:
        """
        Generate a consistent hash for a database row.
        
        Args:
            row_data: Pandas Series containing row data
            
        Returns:
            str: MD5 hash of the row data
        """
        # Convert all values to strings and handle NaN/None values
        row_str = '|'.join([
            str(value) if pd.notna(value) else 'NULL' 
            for value in row_data.values
        ])
        
        # Generate MD5 hash
        return hashlib.md5(row_str.encode('utf-8')).hexdigest()
    
    @staticmethod
    def hash_dataframe(df: pd.DataFrame, primary_keys: List[str]) -> pd.DataFrame:
        """
        Add hash column to DataFrame.
        
        Args:
            df: DataFrame to hash
            primary_keys: List of primary key columns
            
        Returns:
            DataFrame with added hash column
        """
        if df.empty:
            return df
        
        # Sort by primary keys for consistent ordering
        df = df.sort_values(by=primary_keys).reset_index(drop=True)
        
        # Generate hash for each row
        df['row_hash'] = df.apply(HashGenerator.hash_row, axis=1)
        
        return df


class CheckpointManager:
    """Manages restart/resume functionality."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.metadata_table = 'TABLE_COMPARISON_METADATA'
        self._ensure_metadata_table()
    
    def _ensure_metadata_table(self):
        """Create metadata table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE {self.metadata_table} (
            schema_name VARCHAR2(128),
            table_name VARCHAR2(128),
            batch_id NUMBER,
            start_row NUMBER,
            end_row NUMBER,
            status VARCHAR2(20) DEFAULT 'pending',
            processed_time TIMESTAMP,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_comparison_metadata PRIMARY KEY (schema_name, table_name, batch_id)
        )
        """
        
        try:
            self.db_manager.execute_non_query(create_table_sql)
            logging.info(f"Created metadata table: {self.metadata_table}")
        except Exception as e:
            if "ORA-00955" in str(e):  # Table already exists
                logging.debug(f"Metadata table {self.metadata_table} already exists")
            else:
                logging.error(f"Failed to create metadata table: {e}")
                raise
    
    def save_checkpoint(self, schema: str, table: str, batch_info: BatchInfo):
        """Save checkpoint information."""
        upsert_sql = f"""
        MERGE INTO {self.metadata_table} m
        USING (SELECT :schema_name as schema_name, :table_name as table_name, 
                      :batch_id as batch_id FROM dual) s
        ON (m.schema_name = s.schema_name AND m.table_name = s.table_name 
            AND m.batch_id = s.batch_id)
        WHEN MATCHED THEN
            UPDATE SET status = :status, processed_time = :processed_time
        WHEN NOT MATCHED THEN
            INSERT (schema_name, table_name, batch_id, start_row, end_row, 
                   status, processed_time)
            VALUES (:schema_name, :table_name, :batch_id, :start_row, 
                   :end_row, :status, :processed_time)
        """
        
        params = {
            'schema_name': schema,
            'table_name': table,
            'batch_id': batch_info.batch_id,
            'start_row': batch_info.start_row,
            'end_row': batch_info.end_row,
            'status': batch_info.status,
            'processed_time': batch_info.processed_time
        }
        
        self.db_manager.execute_non_query(upsert_sql, params)
    
    def get_completed_batches(self, schema: str, table: str) -> List[int]:
        """Get list of completed batch IDs."""
        query = f"""
        SELECT batch_id FROM {self.metadata_table}
        WHERE schema_name = :schema_name AND table_name = :table_name 
        AND status = 'completed'
        ORDER BY batch_id
        """
        
        params = {'schema_name': schema, 'table_name': table}
        df = self.db_manager.execute_query(query, params)
        
        return df['BATCH_ID'].tolist() if not df.empty else []
    
    def clear_checkpoints(self, schema: str, table: str):
        """Clear all checkpoints for a table."""
        delete_sql = f"""
        DELETE FROM {self.metadata_table}
        WHERE schema_name = :schema_name AND table_name = :table_name
        """
        
        params = {'schema_name': schema, 'table_name': table}
        self.db_manager.execute_non_query(delete_sql, params)


class AuditManager:
    """Manages audit logging to database and file."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.audit_table = 'TABLE_COMPARISON_AUDIT'
        self._ensure_audit_table()
    
    def _ensure_audit_table(self):
        """Create audit table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE {self.audit_table} (
            audit_id NUMBER GENERATED ALWAYS AS IDENTITY,
            job_id VARCHAR2(100),
            schema_name VARCHAR2(128),
            table_name VARCHAR2(128),
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            source_row_count NUMBER,
            target_row_count NUMBER,
            mismatch_count NUMBER,
            insert_count NUMBER,
            update_count NUMBER,
            delete_count NUMBER,
            status VARCHAR2(20),
            error_message CLOB,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_comparison_audit PRIMARY KEY (audit_id)
        )
        """
        
        try:
            self.db_manager.execute_non_query(create_table_sql)
            logging.info(f"Created audit table: {self.audit_table}")
        except Exception as e:
            if "ORA-00955" in str(e):  # Table already exists
                logging.debug(f"Audit table {self.audit_table} already exists")
            else:
                logging.error(f"Failed to create audit table: {e}")
                raise
    
    def log_job_start(self, job_id: str, schema: str, table: str) -> int:
        """Log job start and return audit ID."""
        insert_sql = f"""
        INSERT INTO {self.audit_table} 
        (job_id, schema_name, table_name, start_time, status)
        VALUES (:job_id, :schema_name, :table_name, :start_time, 'running')
        """
        
        params = {
            'job_id': job_id,
            'schema_name': schema,
            'table_name': table,
            'start_time': datetime.now()
        }
        
        self.db_manager.execute_non_query(insert_sql, params)
        
        # Get the audit ID
        query = f"""
        SELECT audit_id FROM {self.audit_table}
        WHERE job_id = :job_id AND schema_name = :schema_name 
        AND table_name = :table_name
        ORDER BY created_time DESC
        FETCH FIRST 1 ROWS ONLY
        """
        
        df = self.db_manager.execute_query(query, {
            'job_id': job_id,
            'schema_name': schema,
            'table_name': table
        })
        
        return df['AUDIT_ID'].iloc[0] if not df.empty else None
    
    def log_job_completion(self, audit_id: int, counts: Dict[str, int], 
                          status: str = 'completed', error: str = None):
        """Log job completion with statistics."""
        update_sql = f"""
        UPDATE {self.audit_table}
        SET end_time = :end_time,
            source_row_count = :source_count,
            target_row_count = :target_count,
            mismatch_count = :mismatch_count,
            insert_count = :insert_count,
            update_count = :update_count,
            delete_count = :delete_count,
            status = :status,
            error_message = :error_message
        WHERE audit_id = :audit_id
        """
        
        params = {
            'audit_id': audit_id,
            'end_time': datetime.now(),
            'source_count': counts.get('source', 0),
            'target_count': counts.get('target', 0),
            'mismatch_count': counts.get('mismatches', 0),
            'insert_count': counts.get('inserts', 0),
            'update_count': counts.get('updates', 0),
            'delete_count': counts.get('deletes', 0),
            'status': status,
            'error_message': error
        }
        
        self.db_manager.execute_non_query(update_sql, params)


class SQLGenerator:
    """Generates SQL statements for synchronization."""
    
    def __init__(self, schema: str, table: str, primary_keys: List[str]):
        self.schema = schema
        self.table = table
        self.primary_keys = primary_keys
        self.source_statements = []
        self.target_statements = []
    
    def add_insert_statement(self, row_data: pd.Series, target: str = 'source'):
        """Add INSERT statement for missing row."""
        columns = list(row_data.index)
        # Remove hash column if present
        if 'row_hash' in columns:
            columns.remove('row_hash')
            row_data = row_data.drop('row_hash')
        
        column_list = ', '.join(columns)
        value_placeholders = ', '.join([':' + col for col in columns])
        
        sql = f"""
INSERT INTO {self.schema}.{self.table} ({column_list})
VALUES ({value_placeholders});
"""
        
        # Convert values for SQL
        values = {}
        for col in columns:
            value = row_data[col]
            if pd.isna(value):
                values[col] = 'NULL'
            elif isinstance(value, str):
                values[col] = f"'{value.replace("'", "''")}'"
            else:
                values[col] = str(value)
        
        # Replace placeholders with actual values
        for col in columns:
            sql = sql.replace(f':{col}', values[col])
        
        if target == 'source':
            self.source_statements.append(sql)
        else:
            self.target_statements.append(sql)
    
    def add_update_statement(self, source_row: pd.Series, target_row: pd.Series, 
                           target: str = 'target'):
        """Add UPDATE statement for differing row."""
        columns = list(source_row.index)
        # Remove hash column and primary keys
        if 'row_hash' in columns:
            columns.remove('row_hash')
        for pk in self.primary_keys:
            if pk in columns:
                columns.remove(pk)
        
        set_clause = []
        where_clause = []
        
        # Build SET clause
        for col in columns:
            value = source_row[col]
            if pd.isna(value):
                set_clause.append(f"{col} = NULL")
            elif isinstance(value, str):
                set_clause.append(f"{col} = '{value.replace("'", "''")}'")
            else:
                set_clause.append(f"{col} = {value}")
        
        # Build WHERE clause
        for pk in self.primary_keys:
            value = source_row[pk]
            if isinstance(value, str):
                where_clause.append(f"{pk} = '{value.replace("'", "''")}'")
            else:
                where_clause.append(f"{pk} = {value}")
        
        sql = f"""
UPDATE {self.schema}.{self.table}
SET {', '.join(set_clause)}
WHERE {' AND '.join(where_clause)};
"""
        
        if target == 'target':
            self.target_statements.append(sql)
        else:
            self.source_statements.append(sql)
    
    def add_delete_statement(self, row_data: pd.Series, target: str = 'target'):
        """Add DELETE statement for extra row."""
        where_clause = []
        
        for pk in self.primary_keys:
            value = row_data[pk]
            if isinstance(value, str):
                where_clause.append(f"{pk} = '{value.replace("'", "''")}'")
            else:
                where_clause.append(f"{pk} = {value}")
        
        sql = f"""
DELETE FROM {self.schema}.{self.table}
WHERE {' AND '.join(where_clause)};
"""
        
        if target == 'target':
            self.target_statements.append(sql)
        else:
            self.source_statements.append(sql)
    
    def write_sql_files(self, source_file: str, target_file: str):
        """Write SQL statements to files."""
        # Write source statements
        with open(source_file, 'w') as f:
            f.write(f"-- Source database synchronization statements\n")
            f.write(f"-- Generated on: {datetime.now()}\n")
            f.write(f"-- Schema: {self.schema}, Table: {self.table}\n\n")
            
            if self.source_statements:
                for stmt in self.source_statements:
                    f.write(stmt + '\n')
            else:
                f.write("-- No synchronization statements needed\n")
        
        # Write target statements
        with open(target_file, 'w') as f:
            f.write(f"-- Target database synchronization statements\n")
            f.write(f"-- Generated on: {datetime.now()}\n")
            f.write(f"-- Schema: {self.schema}, Table: {self.table}\n\n")
            
            if self.target_statements:
                for stmt in self.target_statements:
                    f.write(stmt + '\n')
            else:
                f.write("-- No synchronization statements needed\n")


class PrimaryKeyVerifier:
    """Verifies primary key constraints before INSERT operations."""
    
    def __init__(self, db_manager: DatabaseManager, schema: str, table: str, 
                 primary_keys: List[str]):
        self.db_manager = db_manager
        self.schema = schema
        self.table = table
        self.primary_keys = primary_keys
    
    def verify_insert_statements(self, sql_file: str) -> List[str]:
        """
        Verify INSERT statements and return only valid ones.
        
        Args:
            sql_file: Path to SQL file containing INSERT statements
            
        Returns:
            List of valid INSERT statements
        """
        valid_statements = []
        
        with open(sql_file, 'r') as f:
            content = f.read()
        
        # Extract INSERT statements
        statements = [stmt.strip() for stmt in content.split(';') 
                     if stmt.strip() and 'INSERT INTO' in stmt.upper()]
        
        for stmt in tqdm(statements, desc="Verifying INSERT statements"):
            if self._is_insert_valid(stmt):
                valid_statements.append(stmt)
            else:
                logging.warning(f"Skipping INSERT due to PK conflict: {stmt[:100]}...")
        
        return valid_statements
    
    def _is_insert_valid(self, insert_stmt: str) -> bool:
        """Check if INSERT statement would violate primary key constraint."""
        try:
            # Extract primary key values from INSERT statement
            pk_values = self._extract_pk_values(insert_stmt)
            if not pk_values:
                return True  # Cannot verify, assume valid
            
            # Check if primary key already exists
            where_conditions = []
            params = {}
            
            for i, (pk_col, value) in enumerate(pk_values.items()):
                param_name = f'pk_val_{i}'
                where_conditions.append(f"{pk_col} = :{param_name}")
                params[param_name] = value
            
            check_query = f"""
            SELECT COUNT(*) as cnt FROM {self.schema}.{self.table}
            WHERE {' AND '.join(where_conditions)}
            """
            
            df = self.db_manager.execute_query(check_query, params)
            return df['CNT'].iloc[0] == 0  # Valid if no existing row
            
        except Exception as e:
            logging.error(f"Error verifying INSERT statement: {e}")
            return False  # Conservative approach
    
    def _extract_pk_values(self, insert_stmt: str) -> Dict[str, Any]:
        """Extract primary key values from INSERT statement."""
        # This is a simplified extraction - in production, you might want
        # to use a proper SQL parser
        try:
            # Extract column list and values list
            upper_stmt = insert_stmt.upper()
            columns_start = upper_stmt.find('(') + 1
            columns_end = upper_stmt.find(')')
            values_start = upper_stmt.rfind('(') + 1
            values_end = upper_stmt.rfind(')')
            
            if columns_start <= 0 or columns_end <= 0 or values_start <= 0 or values_end <= 0:
                return {}
            
            columns_str = insert_stmt[columns_start:columns_end]
            values_str = insert_stmt[values_start:values_end]
            
            columns = [col.strip() for col in columns_str.split(',')]
            values = [val.strip().strip("'") for val in values_str.split(',')]
            
            if len(columns) != len(values):
                return {}
            
            # Extract primary key values
            pk_values = {}
            for pk_col in self.primary_keys:
                if pk_col in columns:
                    idx = columns.index(pk_col)
                    pk_values[pk_col] = values[idx]
            
            return pk_values
            
        except Exception:
            return {}


class TableComparator:
    """Main class for comparing tables between two databases."""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.job_id = f"comp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Initialize managers
        self.source_db = DatabaseManager(self.config['source_db'])
        self.target_db = DatabaseManager(self.config['target_db'])
        self.checkpoint_mgr = CheckpointManager(self.source_db)
        self.audit_mgr = AuditManager(self.source_db)
        
        # Initialize counters
        self.stats = {
            'source': 0,
            'target': 0,
            'mismatches': 0,
            'inserts': 0,
            'updates': 0,
            'deletes': 0
        }
        
        # Setup logging
        self._setup_logging()
        
        # Initialize SQL generator
        table_config = self.config['table_config']
        self.sql_generator = SQLGenerator(
            table_config['schema'],
            table_config['table_name'],
            table_config['primary_keys']
        )
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Validate required configuration
            required_keys = ['source_db', 'target_db', 'table_config', 'paths']
            for key in required_keys:
                if key not in config:
                    raise ValueError(f"Missing required configuration: {key}")
            
            # Convert to proper objects
            config['source_db'] = DatabaseConfig(**config['source_db'])
            config['target_db'] = DatabaseConfig(**config['target_db'])
            config['table_config'] = TableConfig(**config['table_config'])
            
            return config
            
        except Exception as e:
            print(f"Error loading configuration: {e}")
            sys.exit(1)
    
    def _setup_logging(self):
        """Setup logging configuration."""
        log_path = Path(self.config['paths']['audit_log'])
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def get_table_row_count(self, db_manager: DatabaseManager) -> int:
        """Get total row count for the table."""
        table_config = self.config['table_config']
        query = f"""
        SELECT COUNT(*) as row_count 
        FROM {table_config.schema}.{table_config.table_name}
        """
        
        df = db_manager.execute_query(query)
        return df['ROW_COUNT'].iloc[0]
    
    def fetch_data_batchwise(self, db_manager: DatabaseManager, 
                           batch_info: BatchInfo) -> pd.DataFrame:
        """
        Fetch data for a specific batch using ROWNUM-based pagination.
        
        Args:
            db_manager: Database manager instance
            batch_info: Batch information containing start and end rows
            
        Returns:
            DataFrame containing the batch data
        """
        table_config = self.config['table_config']
        
        # Build ORDER BY clause for consistent ordering
        order_by = ', '.join(table_config.primary_keys)
        
        # Oracle pagination using ROWNUM
        query = f"""
        SELECT * FROM (
            SELECT t.*, ROWNUM as rn FROM (
                SELECT * FROM {table_config.schema}.{table_config.table_name}
                ORDER BY {order_by}
            ) t
            WHERE ROWNUM <= {batch_info.end_row}
        )
        WHERE rn > {batch_info.start_row}
        """
        
        return db_manager.execute_query(query)
    
    def compare_hashes(self, source_df: pd.DataFrame, 
                      target_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Compare hashed data between source and target.
        
        Args:
            source_df: Source data with hashes
            target_df: Target data with hashes
            
        Returns:
            Dictionary containing comparison results
        """
        primary_keys = self.config['table_config'].primary_keys
        
        if source_df.empty and target_df.empty:
            return {'missing_in_target': pd.DataFrame(), 
                   'missing_in_source': pd.DataFrame(),
                   'hash_mismatches': pd.DataFrame()}
        
        # Create primary key combinations for joining
        def create_pk_key(df):
            if df.empty:
                return df.assign(pk_key='')
            return df.assign(pk_key=df[primary_keys].astype(str).agg('|'.join, axis=1))
        
        source_keyed = create_pk_key(source_df)
        target_keyed = create_pk_key(target_df)
        
        # Find missing records
        if not source_keyed.empty and not target_keyed.empty:
            missing_in_target = source_keyed[
                ~source_keyed['pk_key'].isin(target_keyed['pk_key'])
            ].drop('pk_key', axis=1)
            
            missing_in_source = target_keyed[
                ~target_keyed['pk_key'].isin(source_keyed['pk_key'])
            ].drop('pk_key', axis=1)
            
            # Find hash mismatches
            common_keys = set(source_keyed['pk_key']) & set(target_keyed['pk_key'])
            
            source_common = source_keyed[source_keyed['pk_key'].isin(common_keys)]
            target_common = target_keyed[target_keyed['pk_key'].isin(common_keys)]
            
            # Merge on primary key and compare hashes
            merged = source_common.merge(target_common, on='pk_key', suffixes=('_src', '_tgt'))
            hash_mismatches = merged[merged['row_hash_src'] != merged['row_hash_tgt']]
            
        elif not source_keyed.empty:
            missing_in_target = source_keyed.drop('pk_key', axis=1)
            missing_in_source = pd.DataFrame()
            hash_mismatches = pd.DataFrame()
        elif not target_keyed.empty:
            missing_in_target = pd.DataFrame()
            missing_in_source = target_keyed.drop('pk_key', axis=1)
            hash_mismatches = pd.DataFrame()
        else:
            missing_in_target = pd.DataFrame()
            missing_in_source = pd.DataFrame()
            hash_mismatches = pd.DataFrame()
        
        return {
            'missing_in_target': missing_in_target,
            'missing_in_source': missing_in_source,
            'hash_mismatches': hash_mismatches
        }
    
    def process_batch(self, batch_info: BatchInfo) -> Dict[str, int]:
        """
        Process a single batch of data.
        
        Args:
            batch_info: Information about the batch to process
            
        Returns:
            Dictionary with processing statistics
        """
        try:
            # Fetch data from both databases
            source_data = self.fetch_data_batchwise(self.source_db, batch_info)
            target_data = self.fetch_data_batchwise(self.target_db, batch_info)
            
            # Generate hashes
            source_hashed = HashGenerator.hash_dataframe(
                source_data, self.config['table_config'].primary_keys
            )
            target_hashed = HashGenerator.hash_dataframe(
                target_data, self.config['table_config'].primary_keys
            )
            
            # Compare hashes
            comparison_results = self.compare_hashes(source_hashed, target_hashed)
            
            # Generate SQL statements (thread-safe operation)
            batch_stats = {
                'inserts': 0,
                'updates': 0,
                'deletes': 0,
                'mismatches': 0
            }
            
            # Process missing records (need INSERT in target)
            for _, row in comparison_results['missing_in_target'].iterrows():
                self.sql_generator.add_insert_statement(row, 'target')
                batch_stats['inserts'] += 1
            
            # Process extra records (need DELETE from target)
            for _, row in comparison_results['missing_in_source'].iterrows():
                self.sql_generator.add_delete_statement(row, 'target')
                batch_stats['deletes'] += 1
            
            # Process hash mismatches (need UPDATE in target)
            for _, row in comparison_results['hash_mismatches'].iterrows():
                # Extract source and target data from merged row
                source_cols = [col for col in row.index if col.endswith('_src')]
                source_row = pd.Series({col.replace('_src', ''): row[col] for col in source_cols})
                
                target_cols = [col for col in row.index if col.endswith('_tgt')]
                target_row = pd.Series({col.replace('_tgt', ''): row[col] for col in target_cols})
                
                self.sql_generator.add_update_statement(source_row, target_row, 'target')
                batch_stats['updates'] += 1
            
            batch_stats['mismatches'] = (batch_stats['inserts'] + 
                                       batch_stats['updates'] + 
                                       batch_stats['deletes'])
            
            # Update checkpoint
            batch_info.status = 'completed'
            batch_info.processed_time = datetime.now()
            self.checkpoint_mgr.save_checkpoint(
                self.config['table_config'].schema,
                self.config['table_config'].table_name,
                batch_info
            )
            
            return batch_stats
            
        except Exception as e:
            logging.error(f"Error processing batch {batch_info.batch_id}: {e}")
            batch_info.status = 'failed'
            batch_info.processed_time = datetime.now()
            self.checkpoint_mgr.save_checkpoint(
                self.config['table_config'].schema,
                self.config['table_config'].table_name,
                batch_info
            )
            raise
    
    def generate_sql_files(self):
        """Generate SQL files for synchronization."""
        paths = self.config['paths']
        
        # Ensure output directory exists
        source_file = paths.get('source_sql_output', './output/source_sync_statements.sql')
        target_file = paths.get('target_sql_output', './output/target_sync_statements.sql')
        
        for file_path in [source_file, target_file]:
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.sql_generator.write_sql_files(source_file, target_file)
        logging.info(f"Generated SQL files: {source_file}, {target_file}")
        
        return source_file, target_file
    
    def verify_primary_keys(self, sql_files: List[str]):
        """
        Post-comparison verification of primary keys.
        
        Args:
            sql_files: List of SQL file paths to verify
        """
        if not self.config['flags'].get('enable_reverification', False):
            logging.info("Primary key verification disabled")
            return
        
        logging.info("Starting primary key verification...")
        
        verifier = PrimaryKeyVerifier(
            self.target_db,
            self.config['table_config'].schema,
            self.config['table_config'].table_name,
            self.config['table_config'].primary_keys
        )
        
        for sql_file in sql_files:
            if 'target' in sql_file:  # Only verify target INSERT statements
                valid_statements = verifier.verify_insert_statements(sql_file)
                
                # Write verified statements to new file
                verified_file = sql_file.replace('.sql', '_verified.sql')
                with open(verified_file, 'w') as f:
                    f.write(f"-- Verified INSERT statements\n")
                    f.write(f"-- Generated on: {datetime.now()}\n\n")
                    
                    for stmt in valid_statements:
                        f.write(stmt + ';\n\n')
                
                logging.info(f"Verified SQL file created: {verified_file}")
    
    def run_comparison(self):
        """Run the complete table comparison process."""
        start_time = datetime.now()
        table_config = self.config['table_config']
        
        logging.info(f"Starting comparison for {table_config.schema}.{table_config.table_name}")
        
        # Start audit logging
        audit_id = self.audit_mgr.log_job_start(
            self.job_id, table_config.schema, table_config.table_name
        )
        
        try:
            # Get row counts
            source_count = self.get_table_row_count(self.source_db)
            target_count = self.get_table_row_count(self.target_db)
            
            self.stats['source'] = source_count
            self.stats['target'] = target_count
            
            logging.info(f"Source rows: {source_count}, Target rows: {target_count}")
            
            # Generate batch information
            batch_size = table_config.batch_size
            total_batches = (max(source_count, target_count) + batch_size - 1) // batch_size
            
            batches = []
            for i in range(total_batches):
                start_row = i * batch_size
                end_row = min((i + 1) * batch_size, max(source_count, target_count))
                batches.append(BatchInfo(i, start_row, end_row))
            
            # Check for restart capability
            if self.config['flags'].get('enable_restart', False):
                completed_batches = self.checkpoint_mgr.get_completed_batches(
                    table_config.schema, table_config.table_name
                )
                
                # Filter out completed batches
                batches = [b for b in batches if b.batch_id not in completed_batches]
                logging.info(f"Resuming from checkpoint. {len(batches)} batches remaining.")
            else:
                # Clear any existing checkpoints
                self.checkpoint_mgr.clear_checkpoints(
                    table_config.schema, table_config.table_name
                )
            
            if not batches:
                logging.info("All batches already completed.")
                return
            
            # Process batches with threading
            max_threads = min(table_config.max_threads, len(batches))
            
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                # Submit all batch jobs
                future_to_batch = {
                    executor.submit(self.process_batch, batch): batch 
                    for batch in batches
                }
                
                # Process completed batches with progress bar
                with tqdm(total=len(batches), desc="Processing batches") as pbar:
                    for future in as_completed(future_to_batch):
                        batch = future_to_batch[future]
                        try:
                            batch_stats = future.result()
                            
                            # Update global statistics
                            for key in ['inserts', 'updates', 'deletes', 'mismatches']:
                                self.stats[key] += batch_stats[key]
                            
                            pbar.update(1)
                            pbar.set_postfix({
                                'Mismatches': self.stats['mismatches'],
                                'Inserts': self.stats['inserts'],
                                'Updates': self.stats['updates'],
                                'Deletes': self.stats['deletes']
                            })
                            
                        except Exception as e:
                            logging.error(f"Batch {batch.batch_id} failed: {e}")
                            pbar.update(1)
            
            # Generate SQL files
            source_file, target_file = self.generate_sql_files()
            
            # Verify primary keys if enabled
            self.verify_primary_keys([source_file, target_file])
            
            # Log completion
            self.audit_mgr.log_job_completion(audit_id, self.stats, 'completed')
            
            # Print summary
            duration = datetime.now() - start_time
            logging.info(f"Comparison completed in {duration}")
            logging.info(f"Statistics: {self.stats}")
            
        except Exception as e:
            logging.error(f"Comparison failed: {e}")
            self.audit_mgr.log_job_completion(audit_id, self.stats, 'failed', str(e))
            raise
        
        finally:
            # Cleanup connections
            self.source_db.close()
            self.target_db.close()


def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python table_comparator.py <config.yaml>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)
    
    try:
        comparator = TableComparator(config_path)
        comparator.run_comparison()
        
    except Exception as e:
        logging.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
