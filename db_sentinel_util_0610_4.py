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
import json
import hashlib
import logging
import traceback
import threading
import psutil
import re
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
    """Manages restart/resume functionality using database metadata table with advanced features"""
    
    def __init__(self, db_config: DatabaseConfig, logger: logging.Logger, restart_config: Dict[str, Any] = None):
        self.db_config = db_config
        self.logger = logger
        self.restart_config = restart_config or {}
        self.metadata_table = self.restart_config.get('metadata_table_name', 'DB_SENTINEL_CHECKPOINTS')
        
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
            duration_seconds NUMBER,
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
                        
                # Clean up old checkpoints if configured
                if self.restart_config.get('cleanup_old_checkpoints', True):
                    retention_days = self.restart_config.get('retention_days', 7)
                    self._cleanup_old_checkpoints(retention_days)
                    
        except Exception as e:
            self.logger.error(f"Error creating metadata table: {e}")
            raise
            
    def _cleanup_old_checkpoints(self, retention_days: int):
        """Clean up old checkpoint records"""
        cleanup_sql = f"""
        DELETE FROM {self.metadata_table} 
        WHERE processed_time < SYSTIMESTAMP - INTERVAL '{retention_days}' DAY
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(cleanup_sql)
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    self.logger.info(f"Cleaned up {deleted_count} old checkpoint records older than {retention_days} days")
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error cleaning up old checkpoints: {e}")
            
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
                       batch_start: int, batch_end: int, status: str = "COMPLETED",
                       duration_seconds: float = None):
        """Save checkpoint for a processed batch with timing information"""
        sql = f"""
        MERGE INTO {self.metadata_table} cp
        USING (SELECT ? job_id, ? schema_name, ? table_name, ? batch_start, ? batch_end FROM dual) src
        ON (cp.job_id = src.job_id AND cp.schema_name = src.schema_name 
            AND cp.table_name = src.table_name AND cp.batch_start = src.batch_start 
            AND cp.batch_end = src.batch_end)
        WHEN MATCHED THEN
            UPDATE SET status = ?, processed_time = CURRENT_TIMESTAMP, duration_seconds = ?
        WHEN NOT MATCHED THEN
            INSERT (job_id, schema_name, table_name, batch_start, batch_end, status, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, [job_id, schema, table, batch_start, batch_end, status, duration_seconds,
                                   job_id, schema, table, batch_start, batch_end, status, duration_seconds])
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
            
    def get_checkpoint_statistics(self, job_id: str) -> Dict[str, Any]:
        """Get statistics about checkpoint progress"""
        sql = f"""
        SELECT schema_name, table_name, 
               COUNT(*) as total_batches,
               SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed_batches,
               AVG(duration_seconds) as avg_duration,
               MIN(processed_time) as start_time,
               MAX(processed_time) as last_update
        FROM {self.metadata_table}
        WHERE job_id = ?
        GROUP BY schema_name, table_name
        ORDER BY schema_name, table_name
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, [job_id])
                results = []
                for row in cursor.fetchall():
                    results.append({
                        'schema': row[0],
                        'table': row[1],
                        'total_batches': row[2],
                        'completed_batches': row[3],
                        'avg_duration': row[4],
                        'start_time': row[5],
                        'last_update': row[6]
                    })
                return result
        
    def _process_single_table(self, table_config: TableConfig) -> Tuple[ComparisonResult, Dict[str, Any]]:
        """Process a single table and return result with statistics"""
        table_start_time = datetime.now()
        
        # Auto-adjust chunk size based on memory limits if enabled
        if self.config.get('advanced', {}).get('chunk_size_auto_adjust', False):
            table_config = self._auto_adjust_chunk_size(table_config)
        
        result = self.compare_table(table_config)
        table_end_time = datetime.now()
        
        # Calculate table-level statistics
        table_duration = table_end_time - table_start_time
        
        # Collect statistics
        table_stats = {
            'duration': table_duration,
            'avg_batch_time': None,  # This would be calculated from batch timings
            'total_memory': None     # This would be calculated from memory monitoring
        }
        
        # Log individual table completion
        self.logger.info(f"Table {result.schema}.{result.table_name} completed in {table_duration}")
        if result.error_message:
            self.logger.error(f"Table {result.schema}.{result.table_name} failed: {result.error_message}")
            
        return result, table_stats
        
    def _auto_adjust_chunk_size(self, table_config: TableConfig) -> TableConfig:
        """Auto-adjust chunk size based on memory limits and table size"""
        advanced_config = self.config.get('advanced', {})
        memory_limit_mb = advanced_config.get('memory_limit_mb', 1024)
        
        try:
            # Get an estimate of table size
            source_count = self.get_table_count(self.source_db, table_config)
            
            # Very simple heuristic - adjust chunk size based on table size
            if source_count > 1000000:  # Large table
                suggested_chunk = min(table_config.chunk_size, 25000)
            elif source_count > 100000:  # Medium table
                suggested_chunk = min(table_config.chunk_size, 50000)
            else:  # Small table
                suggested_chunk = table_config.chunk_size
                
            if suggested_chunk != table_config.chunk_size:
                self.logger.info(f"Auto-adjusted chunk size for {table_config.table_name}: {table_config.chunk_size} -> {suggested_chunk}")
                
                # Create new table config with adjusted chunk size
                return TableConfig(
                    table_name=table_config.table_name,
                    primary_key=table_config.primary_key,
                    chunk_size=suggested_chunk,
                    columns=table_config.columns,
                    where_clause=table_config.where_clause,
                    schema=table_config.schema
                )
                
        except Exception as e:
            self.logger.warning(f"Failed to auto-adjust chunk size: {e}")
            
        return table_configs
        except Exception as e:
            self.logger.error(f"Error getting checkpoint statistics: {e}")
            return []


class AuditManager:
    """Manages audit table operations for job tracking with advanced features"""
    
    def __init__(self, db_config: DatabaseConfig, logger: logging.Logger, audit_config: Dict[str, Any] = None):
        self.db_config = db_config
        self.logger = logger
        self.audit_config = audit_config or {}
        self.audit_table = self.audit_config.get('audit_table_name', 'DB_SENTINEL_AUDIT')
        self.batch_table = f"{self.audit_table}_BATCH_DETAILS"
        
    def ensure_audit_table(self):
        """Create audit table and batch details table if they don't exist"""
        # Main audit table
        create_audit_sql = f"""
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
            avg_batch_time_seconds NUMBER,
            total_memory_used_mb NUMBER,
            PRIMARY KEY (job_id, schema_name, table_name)
        )
        """
        
        # Batch details table (if batch logging is enabled)
        create_batch_sql = f"""
        CREATE TABLE {self.batch_table} (
            job_id VARCHAR2(100),
            schema_name VARCHAR2(128),
            table_name VARCHAR2(128),
            batch_start NUMBER,
            batch_end NUMBER,
            source_count NUMBER,
            target_count NUMBER,
            matched_count NUMBER,
            source_only_count NUMBER,
            target_only_count NUMBER,
            different_count NUMBER,
            batch_time_seconds NUMBER,
            memory_used_mb NUMBER,
            processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (job_id, schema_name, table_name, batch_start, batch_end)
        )
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create main audit table
                try:
                    cursor.execute(create_audit_sql)
                    conn.commit()
                    self.logger.info(f"Created audit table: {self.audit_table}")
                except oracledb.DatabaseError as e:
                    if "ORA-00955" in str(e):  # Table already exists
                        self.logger.debug(f"Audit table {self.audit_table} already exists")
                    else:
                        raise
                
                # Create batch details table if batch logging is enabled
                if self.audit_config.get('log_batch_details', False):
                    try:
                        cursor.execute(create_batch_sql)
                        conn.commit()
                        self.logger.info(f"Created batch details table: {self.batch_table}")
                    except oracledb.DatabaseError as e:
                        if "ORA-00955" in str(e):  # Table already exists
                            self.logger.debug(f"Batch details table {self.batch_table} already exists")
                        else:
                            raise
                            
                # Clean up old audit records if retention is configured
                retention_days = self.audit_config.get('retain_history_days', 0)
                if retention_days > 0:
                    self._cleanup_old_records(retention_days)
                    
        except Exception as e:
            self.logger.error(f"Error creating audit tables: {e}")
            raise
            
    def _cleanup_old_records(self, retention_days: int):
        """Clean up old audit records beyond retention period"""
        cleanup_sql = f"""
        DELETE FROM {self.audit_table} 
        WHERE start_time < SYSTIMESTAMP - INTERVAL '{retention_days}' DAY
        """
        
        batch_cleanup_sql = f"""
        DELETE FROM {self.batch_table} 
        WHERE processed_time < SYSTIMESTAMP - INTERVAL '{retention_days}' DAY
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Clean main audit table
                cursor.execute(cleanup_sql)
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    self.logger.info(f"Cleaned up {deleted_count} old audit records")
                
                # Clean batch details table if it exists
                if self.audit_config.get('log_batch_details', False):
                    cursor.execute(batch_cleanup_sql)
                    batch_deleted = cursor.rowcount
                    if batch_deleted > 0:
                        self.logger.info(f"Cleaned up {batch_deleted} old batch detail records")
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error cleaning up old audit records: {e}")
            
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
                
    def log_batch_result(self, job_id: str, schema: str, table_name: str, 
                        batch_start: int, batch_end: int, batch_result: Dict[str, Any]):
        """Log individual batch result if batch logging is enabled"""
        if not self.audit_config.get('log_batch_details', False):
            return
            
        sql = f"""
        INSERT INTO {self.batch_table} 
        (job_id, schema_name, table_name, batch_start, batch_end, 
         source_count, target_count, matched_count, source_only_count, 
         target_only_count, different_count, batch_time_seconds, memory_used_mb)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, [
                    job_id, schema, table_name, batch_start, batch_end,
                    batch_result.get('source_count', 0),
                    batch_result.get('target_count', 0),
                    batch_result.get('matched_count', 0),
                    batch_result.get('source_only_count', 0),
                    batch_result.get('target_only_count', 0),
                    batch_result.get('different_count', 0),
                    batch_result.get('batch_time_seconds'),
                    batch_result.get('memory_used_mb')
                ])
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error logging batch result: {e}")
                
    def log_job_result(self, job_id: str, result: ComparisonResult, avg_batch_time: float = None, 
                      total_memory: float = None):
        """Log job result to audit table with enhanced metrics"""
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
                duration_seconds = ?,
                avg_batch_time_seconds = ?,
                total_memory_used_mb = ?
        WHEN NOT MATCHED THEN
            INSERT (job_id, schema_name, table_name, start_time, end_time,
                   source_count, target_count, matched_count, source_only_count,
                   target_only_count, different_count, status, error_message, 
                   duration_seconds, avg_batch_time_seconds, total_memory_used_mb)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        duration_seconds = result.duration.total_seconds() if result.duration else None
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, [
                    job_id, result.schema, result.table_name, result.end_time,
                    result.source_count, result.target_count, result.matched_count,
                    result.source_only_count, result.target_only_count, result.different_count,
                    result.status, result.error_message, duration_seconds, avg_batch_time, total_memory,
                    job_id, result.schema, result.table_name, result.start_time, result.end_time,
                    result.source_count, result.target_count, result.matched_count,
                    result.source_only_count, result.target_only_count, result.different_count,
                    result.status, result.error_message, duration_seconds, avg_batch_time, total_memory
                ])
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error logging to audit table: {e}")


class DatabaseComparator:
    """Main class for comparing Oracle database tables"""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        
        # Create unique run identifier with timestamp
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.job_id = f"DBSENTINEL_{self.run_timestamp}"
        
        self.logger = self._setup_logging()
        
        # Initialize database configs
        self.source_db = DatabaseConfig(**self.config['source_db'])
        self.target_db = DatabaseConfig(**self.config['target_db'])
        
        # Initialize managers only if enabled in configuration
        self.checkpoint_manager = None
        self.audit_manager = None
        
        # Check feature flags
        flags = self.config.get('flags', {})
        
        if flags.get('enable_restart', False):
            restart_config = self.config.get('restart', {})
            self.checkpoint_manager = CheckpointManager(self.source_db, self.logger, restart_config)
            self.logger.info("Restart/resume functionality enabled")
        else:
            self.logger.info("Restart/resume functionality disabled")
            
        if flags.get('enable_audit_table', False):
            audit_config = self.config.get('audit', {})
            self.audit_manager = AuditManager(self.source_db, self.logger, audit_config)
            self.logger.info("Database audit table functionality enabled")
        else:
            self.logger.info("Database audit table functionality disabled")
        
        # Thread-safe locks
        self.sql_lock = threading.Lock()
        self.audit_lock = threading.Lock()
        
        # Initialize output files if SQL generation is enabled
        if flags.get('enable_sql_generation', True):
            self._setup_output_files()
            self.logger.info("SQL file generation enabled")
        else:
            self.logger.info("SQL file generation disabled")
            
        # Table-specific SQL file tracking
        self.table_sql_files = {}
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"Error loading config file {config_path}: {e}")
            sys.exit(1)
            
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration with timestamped audit files"""
        log_config = self.config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        base_log_file = self.config.get('paths', {}).get('audit_log', './logs/audit.log')
        
        # Create timestamped audit log file for each run
        log_path = Path(base_log_file)
        timestamped_log_file = log_path.parent / f"{log_path.stem}_{self.run_timestamp}{log_path.suffix}"
        
        # Create log directory if it doesn't exist
        timestamped_log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Advanced logging configuration
        max_file_size = log_config.get('max_file_size', '50MB')
        backup_count = log_config.get('backup_count', 3)
        log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Convert file size string to bytes
        if isinstance(max_file_size, str):
            size_map = {'KB': 1024, 'MB': 1024*1024, 'GB': 1024*1024*1024}
            size_value = int(''.join(filter(str.isdigit, max_file_size)))
            size_unit = ''.join(filter(str.isalpha, max_file_size)).upper()
            max_bytes = size_value * size_map.get(size_unit, 1024*1024)  # Default to MB
        else:
            max_bytes = max_file_size
        
        # Setup rotating file handler
        from logging.handlers import RotatingFileHandler
        
        # Create logger
        logger = logging.getLogger('DB_Sentinel')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Remove existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            timestamped_log_file, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_formatter = logging.Formatter(log_format)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_formatter = logging.Formatter(log_format)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # Store the actual log file path for reference
        self.actual_audit_log = str(timestamped_log_file)
        
        return logger
        
    def _setup_output_files(self):
        """Setup output directory structure for timestamped files"""
        paths = self.config.get('paths', {})
        
        # Get base paths from config
        base_source_sql = paths.get('source_sql_output', './output/source_sync_statements.sql')
        base_target_sql = paths.get('target_sql_output', './output/target_sync_statements.sql')
        
        # Create timestamped base directories
        source_path = Path(base_source_sql)
        target_path = Path(base_target_sql)
        
        # Create output directories with timestamp
        self.output_dir = source_path.parent / f"run_{self.run_timestamp}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Store base file patterns for table-specific files
        self.source_sql_pattern = self.output_dir / "source_sync_{table_name}_{timestamp}.sql"
        self.target_sql_pattern = self.output_dir / "target_sync_{table_name}_{timestamp}.sql"
        
        # Also create a combined file for all tables (legacy compatibility)
        self.source_sql_file = self.output_dir / f"source_sync_all_tables_{self.run_timestamp}.sql"
        self.target_sql_file = self.output_dir / f"target_sync_all_tables_{self.run_timestamp}.sql"
        
        # Initialize combined SQL files with headers
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        header = f"""-- DB_Sentinel_util Generated SQL Statements
-- Generated: {timestamp}
-- Job ID: {self.job_id}
-- Run Timestamp: {self.run_timestamp}
-- 
-- This file contains statements for ALL tables in this comparison run
-- Individual table-specific files are also created in the same directory
--

"""
        
        with open(self.source_sql_file, 'w') as f:
            f.write(header + "-- Source Database Sync Statements (All Tables)\n\n")
        with open(self.target_sql_file, 'w') as f:
            f.write(header + "-- Target Database Sync Statements (All Tables)\n\n")
            
        self.logger.info(f"Output directory created: {self.output_dir}")
        
    def _get_table_sql_files(self, table_config: TableConfig) -> Tuple[str, str]:
        """Get table-specific SQL file paths, creating them if needed"""
        table_name = table_config.table_name.upper()
        schema_name = (table_config.schema or "DEFAULT").upper()
        
        # Create table-specific file names
        table_key = f"{schema_name}.{table_name}"
        
        if table_key not in self.table_sql_files:
            # Generate safe filename (replace special characters)
            safe_table_name = re.sub(r'[^\w\-_]', '_', f"{schema_name}_{table_name}")
            
            source_file = self.output_dir / f"source_sync_{safe_table_name}_{self.run_timestamp}.sql"
            target_file = self.output_dir / f"target_sync_{safe_table_name}_{self.run_timestamp}.sql"
            
            # Create table-specific SQL files with headers
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            header = f"""-- DB_Sentinel_util Table-Specific SQL Statements
-- Table: {table_key}
-- Generated: {timestamp}
-- Job ID: {self.job_id}
-- Run Timestamp: {self.run_timestamp}
-- 
-- Primary Key Columns: {', '.join(table_config.primary_key)}
-- Chunk Size: {table_config.chunk_size:,}
{f"-- WHERE Clause: {table_config.where_clause}" if table_config.where_clause else ""}
{f"-- Specific Columns: {', '.join(table_config.columns)}" if table_config.columns else "-- All Columns"}
--

"""
            
            with open(source_file, 'w') as f:
                f.write(header + f"-- Source Database Sync Statements for {table_key}\n\n")
            with open(target_file, 'w') as f:
                f.write(header + f"-- Target Database Sync Statements for {table_key}\n\n")
                
            self.table_sql_files[table_key] = {
                'source': str(source_file),
                'target': str(target_file)
            }
            
            self.logger.info(f"Created table-specific SQL files for {table_key}")
            
        return self.table_sql_files[table_key]['source'], self.table_sql_files[table_key]['target']
            
    @contextmanager
    def get_connection(self, db_config: DatabaseConfig):
        """Get database connection context manager with timeout support"""
        conn = None
        try:
            # Get connection timeout from performance config
            timeout = self.config.get('performance', {}).get('batch_timeout', 300)
            
            conn = oracledb.connect(
                user=db_config.user,
                password=db_config.password,
                dsn=db_config.dsn
            )
            
            # Set query timeout if supported
            if hasattr(conn, 'cursor'):
                cursor = conn.cursor()
                # Oracle doesn't have a direct query timeout, but we can implement it at the application level
                
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
        """Fetch data from database in batches with timeout and monitoring support"""
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        where_clause = f" AND ({table_config.where_clause})" if table_config.where_clause else ""
        
        # Get performance configuration
        timeout = self.config.get('performance', {}).get('batch_timeout', 300)
        
        # Get actual column names if * is specified
        if not table_config.columns or table_config.columns == ["*"]:
            # Get all column names from table
            actual_columns = self._get_table_columns(db_config, table_config)
            column_list = ", ".join(actual_columns)
        else:
            column_list = ", ".join(table_config.columns)
        
        # Try ROW_NUMBER() approach first (most reliable)
        queries_to_try = [
            # Method 1: ROW_NUMBER() window function (Oracle 9i+)
            f"""
            SELECT {column_list}
            FROM (
                SELECT {column_list}, 
                       ROW_NUMBER() OVER (ORDER BY {', '.join(table_config.primary_key)}) as rn
                FROM {schema_prefix}{table_config.table_name}
                WHERE 1=1{where_clause}
            ) ranked_data
            WHERE rn > {batch_start} AND rn <= {batch_start + batch_size}
            """,
            
            # Method 2: OFFSET/FETCH for Oracle 12c+
            f"""
            SELECT {column_list}
            FROM {schema_prefix}{table_config.table_name}
            WHERE 1=1{where_clause}
            ORDER BY {', '.join(table_config.primary_key)}
            OFFSET {batch_start} ROWS FETCH NEXT {batch_size} ROWS ONLY
            """,
            
            # Method 3: Triple-nested ROWNUM (fallback)
            f"""
            SELECT {column_list.replace('*', 'a.*') if '*' in column_list else column_list} FROM (
                SELECT a.*, ROWNUM rnum FROM (
                    SELECT {column_list} FROM {schema_prefix}{table_config.table_name}
                    WHERE 1=1{where_clause}
                    ORDER BY {', '.join(table_config.primary_key)}
                ) a
                WHERE ROWNUM <= {batch_start + batch_size}
            )
            WHERE rnum > {batch_start}
            """
        ]
        
        for i, sql in enumerate(queries_to_try):
            try:
                fetch_start = datetime.now()
                
                with self.get_connection(db_config) as conn:
                    # For Oracle, we'll implement a simple timeout using pandas read_sql
                    df = pd.read_sql(sql, conn)
                    
                    fetch_duration = datetime.now() - fetch_start
                    
                    # Log detailed timing if enabled
                    if self.config.get('flags', {}).get('enable_detailed_logging', False):
                        method_names = ["ROW_NUMBER()", "OFFSET/FETCH", "ROWNUM"]
                        self.logger.debug(f"Batch fetch using {method_names[i]} completed in {fetch_duration.total_seconds():.2f}s, returned {len(df)} rows")
                    
                    # Check if fetch took too long
                    if fetch_duration.total_seconds() > timeout:
                        self.logger.warning(f"Batch fetch took {fetch_duration.total_seconds():.1f}s, exceeding timeout of {timeout}s")
                    
                    return df
                    
            except Exception as e:
                method_names = ["ROW_NUMBER()", "OFFSET/FETCH", "ROWNUM"]
                
                if i < len(queries_to_try) - 1:
                    self.logger.warning(f"{method_names[i]} approach failed: {e}, trying next method")
                else:
                    self.logger.error(f"All pagination methods failed. Last error with {method_names[i]}: {e}")
                    self.logger.error(f"Final SQL attempted: {sql}")
                    if self.config.get('flags', {}).get('enable_detailed_logging', False):
                        self.logger.error(traceback.format_exc())
                    raise Exception(f"Failed to fetch data after trying all methods. Last error: {e}")
                
        # This should never be reached, but just in case
        raise Exception("Unexpected error in fetch_data_batchwise")
            
    def hash_rows(self, df: pd.DataFrame, primary_keys: List[str]) -> Dict[str, str]:
        """
        Generate hash for each row based on all column values with configurable options
        
        Returns dictionary mapping primary key values to row hashes
        """
        if df.empty:
            return {}
            
        hashes = {}
        
        # Get hashing configuration
        hash_config = self.config.get('hashing', {})
        algorithm = hash_config.get('algorithm', 'md5').lower()
        ignore_case = hash_config.get('ignore_case', False)
        trim_whitespace = hash_config.get('trim_whitespace', True)
        null_replacement = hash_config.get('null_replacement', '__NULL__')
        
        # Select hash algorithm
        if algorithm == 'sha1':
            hash_func = hashlib.sha1
        elif algorithm == 'sha256':
            hash_func = hashlib.sha256
        elif algorithm == 'md5':
            hash_func = hashlib.md5
        else:
            self.logger.warning(f"Unknown hash algorithm '{algorithm}', defaulting to md5")
            hash_func = hashlib.md5
        
        for _, row in df.iterrows():
            # Create primary key tuple
            pk_values = tuple(str(row[pk]) for pk in primary_keys)
            pk_key = "|".join(str(v) for v in pk_values)
            
            # Create hash of all column values (excluding row number columns)
            row_data = row.drop(['rn', 'RN', 'rnum', 'RNUM'], errors='ignore')
            
            # Process each value according to configuration
            processed_values = []
            for value in row_data.values:
                if pd.isna(value) or value is None:
                    processed_value = null_replacement
                else:
                    processed_value = str(value)
                    if trim_whitespace:
                        processed_value = processed_value.strip()
                    if ignore_case:
                        processed_value = processed_value.lower()
                
                processed_values.append(processed_value)
            
            row_string = "|".join(processed_values)
            row_hash = hash_func(row_string.encode('utf-8')).hexdigest()
            
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
        """Generate INSERT and UPDATE SQL statements with table-specific files"""
        
        # Check if SQL generation is enabled
        if not self.config.get('flags', {}).get('enable_sql_generation', True):
            return
            
        sql_config = self.config.get('sql_generation', {})
        include_schema = sql_config.get('include_schema_prefix', True)
        include_comments = sql_config.get('include_comments', True)
        batch_statements = sql_config.get('batch_statements', False)
        batch_size = sql_config.get('statement_batch_size', 100)
        escape_chars = sql_config.get('escape_special_chars', True)
        
        schema_prefix = f"{table_config.schema}." if table_config.schema and include_schema else ""
        table_name = f"{schema_prefix}{table_config.table_name}"
        
        # Get table-specific SQL files
        table_source_file, table_target_file = self._get_table_sql_files(table_config)
        
        # Create lookup dictionaries for faster access
        source_lookup = {}
        target_lookup = {}
        
        if not source_data.empty:
            for _, row in source_data.iterrows():
                pk_key = "|".join(str(row[pk]) for pk in table_config.primary_key)
                clean_row = row.drop(['rn', 'RN', 'rnum', 'RNUM'], errors='ignore')
                source_lookup[pk_key] = clean_row
                
        if not target_data.empty:
            for _, row in target_data.iterrows():
                pk_key = "|".join(str(row[pk]) for pk in table_config.primary_key)
                clean_row = row.drop(['rn', 'RN', 'rnum', 'RNUM'], errors='ignore')
                target_lookup[pk_key] = clean_row
        
        def escape_value(value):
            """Escape special characters in SQL values"""
            if pd.isna(value) or value is None:
                return 'NULL'
            
            str_value = str(value)
            if escape_chars:
                str_value = str_value.replace("'", "''")
                
            return f"'{str_value}'"
        
        with self.sql_lock:
            insert_statements = []
            update_statements = []
            
            # Generate INSERT statements for source_only records (missing in target)
            for pk_key in comparison_result['source_only']:
                if pk_key in source_lookup:
                    row = source_lookup[pk_key]
                    columns = list(row.index)
                    values = [escape_value(v) for v in row.values]
                    
                    if include_comments:
                        comment = f"-- Missing in target: PK={pk_key}\n"
                        insert_statements.append(comment)
                    
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
                    insert_statements.append(insert_sql)
                    
            # Write target INSERT statements to both table-specific and combined files
            if insert_statements:
                section_header = ""
                if include_comments:
                    section_header = f"\n-- INSERT statements for {table_name} (records missing in target)\n"
                    section_header += f"-- Total: {len([s for s in insert_statements if 'INSERT' in s])} statements\n"
                    section_header += f"-- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                
                # Write to table-specific file
                with open(table_target_file, 'a') as f:
                    f.write(section_header)
                    self._write_statements_with_batching(f, insert_statements, batch_statements, batch_size)
                
                # Write to combined file
                with open(self.target_sql_file, 'a') as f:
                    f.write(section_header)
                    self._write_statements_with_batching(f, insert_statements, batch_statements, batch_size)
                        
            # Generate INSERT statements for target_only records (missing in source)
            source_insert_statements = []
            for pk_key in comparison_result['target_only']:
                if pk_key in target_lookup:
                    row = target_lookup[pk_key]
                    columns = list(row.index)
                    values = [escape_value(v) for v in row.values]
                    
                    if include_comments:
                        comment = f"-- Missing in source: PK={pk_key}\n"
                        source_insert_statements.append(comment)
                    
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
                    source_insert_statements.append(insert_sql)
                    
            # Write source INSERT statements
            if source_insert_statements:
                section_header = ""
                if include_comments:
                    section_header = f"\n-- INSERT statements for {table_name} (records missing in source)\n"
                    section_header += f"-- Total: {len([s for s in source_insert_statements if 'INSERT' in s])} statements\n"
                    section_header += f"-- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                
                # Write to table-specific file
                with open(table_source_file, 'a') as f:
                    f.write(section_header)
                    self._write_statements_with_batching(f, source_insert_statements, batch_statements, batch_size)
                
                # Write to combined file
                with open(self.source_sql_file, 'a') as f:
                    f.write(section_header)
                    self._write_statements_with_batching(f, source_insert_statements, batch_statements, batch_size)
                        
            # Generate UPDATE statements for different records
            for pk_key in comparison_result['different']:
                if pk_key in source_lookup and pk_key in target_lookup:
                    source_row = source_lookup[pk_key]
                    target_row = target_lookup[pk_key]
                    
                    set_clauses = []
                    for col in source_row.index:
                        if col not in table_config.primary_key:
                            if str(source_row[col]) != str(target_row[col]):
                                value = escape_value(source_row[col])
                                set_clauses.append(f"{col} = {value}")
                                
                    if set_clauses:
                        pk_conditions = []
                        for pk in table_config.primary_key:
                            value = escape_value(source_row[pk])
                            pk_conditions.append(f"{pk} = {value}")
                        
                        if include_comments:
                            comment = f"-- Different values: PK={pk_key}\n"
                            update_statements.append(comment)
                            
                        update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {' AND '.join(pk_conditions)};\n"
                        update_statements.append(update_sql)
                        
            # Write UPDATE statements
            if update_statements:
                section_header = ""
                if include_comments:
                    section_header = f"\n-- UPDATE statements for {table_name} (records with differences)\n"
                    section_header += f"-- Total: {len([s for s in update_statements if 'UPDATE' in s])} statements\n"
                    section_header += f"-- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                
                # Write to table-specific file
                with open(table_target_file, 'a') as f:
                    f.write(section_header)
                    self._write_statements_with_batching(f, update_statements, batch_statements, batch_size)
                
                # Write to combined file
                with open(self.target_sql_file, 'a') as f:
                    f.write(section_header)
                    self._write_statements_with_batching(f, update_statements, batch_statements, batch_size)
                    
    def _write_statements_with_batching(self, file_handle, statements: List[str], batch_statements: bool, batch_size: int):
        """Write SQL statements with optional batching and commit statements"""
        if batch_statements:
            for i in range(0, len(statements), batch_size):
                batch = statements[i:i + batch_size]
                for stmt in batch:
                    file_handle.write(stmt)
                if i + batch_size < len(statements):
                    file_handle.write("COMMIT;\n\n")
            file_handle.write("COMMIT;\n\n")
        else:
            for stmt in statements:
                file_handle.write(stmt)
            file_handle.write("\n")
                            
    def verify_primary_keys(self, table_config: TableConfig, insert_statements: List[str]) -> List[str]:
        """
        Comprehensive primary key verification with proper SQL parsing
        
        This method parses INSERT statements, extracts primary key values,
        and verifies they don't already exist in the target database.
        """
        if not insert_statements:
            return []
            
        verification_config = self.config.get('verification', {})
        batch_size = verification_config.get('batch_size', 1000)
        max_threads = verification_config.get('max_threads', 2)
        skip_existing = verification_config.get('skip_existing_keys', True)
        
        if not skip_existing:
            self.logger.info("Primary key verification disabled - returning all statements")
            return insert_statements
            
        self.logger.info(f"Starting comprehensive primary key verification for {len(insert_statements)} statements")
        
        # Parse statements to extract structured data
        parsed_statements = []
        for i, stmt in enumerate(insert_statements):
            try:
                parsed = self._parse_insert_statement(stmt, table_config)
                if parsed:
                    parsed['original_statement'] = stmt
                    parsed['statement_index'] = i
                    parsed_statements.append(parsed)
                else:
                    self.logger.warning(f"Failed to parse statement {i}: {stmt[:100]}...")
            except Exception as e:
                self.logger.error(f"Error parsing statement {i}: {e}")
                if self.config.get('flags', {}).get('enable_detailed_logging', False):
                    self.logger.error(f"Problematic statement: {stmt}")
        
        if not parsed_statements:
            self.logger.warning("No valid statements found after parsing")
            return []
            
        self.logger.info(f"Successfully parsed {len(parsed_statements)} statements")
        
        # Process statements in batches with optional threading
        valid_statements = []
        total_batches = (len(parsed_statements) + batch_size - 1) // batch_size
        
        if max_threads > 1 and total_batches > 1:
            valid_statements = self._verify_statements_parallel(parsed_statements, table_config, batch_size, max_threads)
        else:
            valid_statements = self._verify_statements_sequential(parsed_statements, table_config, batch_size)
            
        self.logger.info(f"Verification completed: {len(valid_statements)}/{len(insert_statements)} statements are safe to execute")
        
        # Generate verification report if detailed logging is enabled
        if self.config.get('flags', {}).get('enable_detailed_logging', False):
            self._generate_verification_report(insert_statements, valid_statements, table_config)
            
        return valid_statements
        
    def _parse_insert_statement(self, sql_statement: str, table_config: TableConfig) -> Optional[Dict[str, Any]]:
        """
        Parse INSERT statement to extract table name, columns, and values
        
        Handles various INSERT formats:
        - INSERT INTO table (col1, col2) VALUES (val1, val2)
        - INSERT INTO schema.table (col1, col2) VALUES (val1, val2)
        - Multiple value formats (strings, numbers, NULL, dates)
        """
        try:
            # Clean up the statement
            clean_sql = sql_statement.strip()
            if clean_sql.endswith(';'):
                clean_sql = clean_sql[:-1]
            
            # Regular expression to parse INSERT statement
            # This handles: INSERT INTO [schema.]table [(columns)] VALUES (values)
            insert_pattern = r'''
                INSERT\s+INTO\s+                          # INSERT INTO
                (?:(\w+)\.)?                               # Optional schema
                (\w+)                                      # Table name
                \s*\(\s*                                   # Opening parenthesis for columns
                ([^)]+)                                    # Column list
                \s*\)\s*                                   # Closing parenthesis for columns
                VALUES\s*\(\s*                             # VALUES clause
                (.+)                                       # Value list
                \s*\)                                      # Closing parenthesis for values
            '''
            
            match = re.search(insert_pattern, clean_sql, re.IGNORECASE | re.VERBOSE)
            if not match:
                self.logger.warning(f"Could not parse INSERT statement format: {clean_sql[:100]}...")
                return None
                
            schema_name = match.group(1)
            table_name = match.group(2)
            columns_str = match.group(3)
            values_str = match.group(4)
            
            # Verify this is for the correct table
            if table_name.upper() != table_config.table_name.upper():
                return None
                
            # Parse column names
            columns = [col.strip().strip('"').strip("'") for col in columns_str.split(',')]
            
            # Parse values - this is more complex due to quoted strings, NULLs, etc.
            values = self._parse_values_list(values_str)
            
            if len(columns) != len(values):
                self.logger.warning(f"Column/value count mismatch: {len(columns)} columns, {len(values)} values")
                return None
                
            # Create column-value mapping
            column_value_map = dict(zip(columns, values))
            
            # Extract primary key values
            pk_values = {}
            for pk_col in table_config.primary_key:
                pk_col_upper = pk_col.upper()
                found = False
                for col, val in column_value_map.items():
                    if col.upper() == pk_col_upper:
                        pk_values[pk_col] = val
                        found = True
                        break
                        
                if not found:
                    self.logger.warning(f"Primary key column '{pk_col}' not found in INSERT statement")
                    return None
                    
            return {
                'schema': schema_name,
                'table': table_name,
                'columns': columns,
                'values': values,
                'column_value_map': column_value_map,
                'primary_key_values': pk_values
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing INSERT statement: {e}")
            if self.config.get('flags', {}).get('enable_detailed_logging', False):
                self.logger.error(f"Statement: {sql_statement}")
            return None
            
    def _parse_values_list(self, values_str: str) -> List[str]:
        """
        Parse VALUES clause handling quoted strings, NULLs, and numbers
        
        Handles:
        - 'quoted strings'
        - "quoted strings"
        - NULL values
        - Numeric values
        - Date functions like TO_DATE(...)
        """
        values = []
        current_value = ""
        in_quote = False
        quote_char = None
        paren_depth = 0
        i = 0
        
        while i < len(values_str):
            char = values_str[i]
            
            if not in_quote:
                if char in ("'", '"'):
                    in_quote = True
                    quote_char = char
                    current_value += char
                elif char == '(':
                    paren_depth += 1
                    current_value += char
                elif char == ')':
                    paren_depth -= 1
                    current_value += char
                elif char == ',' and paren_depth == 0:
                    # End of current value
                    values.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            else:
                current_value += char
                if char == quote_char:
                    # Check for escaped quote
                    if i + 1 < len(values_str) and values_str[i + 1] == quote_char:
                        # Escaped quote, add the next character and skip it
                        i += 1
                        current_value += values_str[i]
                    else:
                        # End of quoted string
                        in_quote = False
                        quote_char = None
                        
            i += 1
            
        # Add the last value
        if current_value.strip():
            values.append(current_value.strip())
            
        return values
        
    def _verify_statements_sequential(self, parsed_statements: List[Dict], table_config: TableConfig, batch_size: int) -> List[str]:
        """Verify statements sequentially in batches"""
        valid_statements = []
        total_batches = (len(parsed_statements) + batch_size - 1) // batch_size
        
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        table_name = f"{schema_prefix}{table_config.table_name}"
        
        with tqdm(total=len(parsed_statements), desc="Verifying statements", unit="stmt") as pbar:
            try:
                with self.get_connection(self.target_db) as conn:
                    cursor = conn.cursor()
                    
                    for batch_start in range(0, len(parsed_statements), batch_size):
                        batch_end = min(batch_start + batch_size, len(parsed_statements))
                        batch = parsed_statements[batch_start:batch_end]
                        
                        batch_valid = self._verify_batch(batch, table_config, table_name, cursor)
                        valid_statements.extend(batch_valid)
                        
                        pbar.update(len(batch))
                        
                        if self.config.get('flags', {}).get('enable_detailed_logging', False):
                            self.logger.debug(f"Batch {batch_start//batch_size + 1}/{total_batches}: {len(batch_valid)}/{len(batch)} valid")
                            
            except Exception as e:
                self.logger.error(f"Error during sequential verification: {e}")
                raise
                
        return valid_statements
        
    def _verify_statements_parallel(self, parsed_statements: List[Dict], table_config: TableConfig, batch_size: int, max_threads: int) -> List[str]:
        """Verify statements in parallel batches"""
        valid_statements = []
        batches = []
        
        # Create batches
        for batch_start in range(0, len(parsed_statements), batch_size):
            batch_end = min(batch_start + batch_size, len(parsed_statements))
            batches.append(parsed_statements[batch_start:batch_end])
            
        schema_prefix = f"{table_config.schema}." if table_config.schema else ""
        table_name = f"{schema_prefix}{table_config.table_name}"
        
        with tqdm(total=len(parsed_statements), desc="Verifying statements (parallel)", unit="stmt") as pbar:
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                # Submit all batch verification jobs
                future_to_batch = {
                    executor.submit(self._verify_batch_with_connection, batch, table_config, table_name): batch
                    for batch in batches
                }
                
                # Collect results
                for future in as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    try:
                        batch_valid = future.result()
                        valid_statements.extend(batch_valid)
                        pbar.update(len(batch))
                    except Exception as e:
                        self.logger.error(f"Parallel verification batch failed: {e}")
                        
        return valid_statements
        
    def _verify_batch_with_connection(self, batch: List[Dict], table_config: TableConfig, table_name: str) -> List[str]:
        """Verify a batch of statements with its own database connection"""
        try:
            with self.get_connection(self.target_db) as conn:
                cursor = conn.cursor()
                return self._verify_batch(batch, table_config, table_name, cursor)
        except Exception as e:
            self.logger.error(f"Error in batch verification: {e}")
            return []
            
    def _verify_batch(self, batch: List[Dict], table_config: TableConfig, table_name: str, cursor) -> List[str]:
        """Verify a single batch of parsed statements"""
        valid_statements = []
        
        for parsed_stmt in batch:
            try:
                pk_values = parsed_stmt['primary_key_values']
                
                # Build WHERE clause for primary key existence check
                pk_conditions = []
                bind_variables = []
                
                for pk_col in table_config.primary_key:
                    pk_value = pk_values[pk_col]
                    
                    if pk_value.upper() == 'NULL':
                        pk_conditions.append(f"{pk_col} IS NULL")
                    else:
                        pk_conditions.append(f"{pk_col} = ?")
                        # Remove quotes if present
                        clean_value = pk_value.strip("'\"")
                        bind_variables.append(clean_value)
                
                # Execute existence check
                check_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {' AND '.join(pk_conditions)}"
                
                if bind_variables:
                    cursor.execute(check_sql, bind_variables)
                else:
                    cursor.execute(check_sql)
                    
                count = cursor.fetchone()[0]
                
                if count == 0:
                    # Record doesn't exist, safe to insert
                    valid_statements.append(parsed_stmt['original_statement'])
                else:
                    # Record exists, skip
                    pk_display = ', '.join([f"{k}={v}" for k, v in pk_values.items()])
                    if self.config.get('flags', {}).get('enable_detailed_logging', False):
                        self.logger.debug(f"Skipping INSERT - PK exists: {pk_display}")
                        
            except Exception as e:
                self.logger.error(f"Error verifying statement: {e}")
                if self.config.get('flags', {}).get('enable_detailed_logging', False):
                    self.logger.error(f"Statement: {parsed_stmt.get('original_statement', 'Unknown')[:100]}...")
                    
        return valid_statements
        
    def _generate_verification_report(self, original_statements: List[str], valid_statements: List[str], table_config: TableConfig):
        """Generate detailed verification report"""
        report_path = self.config.get('paths', {}).get('comparison_report', './output/comparison_report.txt')
        verification_report_path = report_path.replace('.txt', '_verification.txt')
        
        skipped_count = len(original_statements) - len(valid_statements)
        success_rate = (len(valid_statements) / len(original_statements)) * 100 if original_statements else 0
        
        with open(verification_report_path, 'w') as f:
            f.write(f"DB_Sentinel_util Verification Report\n")
            f.write(f"{'=' * 50}\n\n")
            f.write(f"Table: {table_config.schema or 'DEFAULT'}.{table_config.table_name}\n")
            f.write(f"Verification Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write(f"Statement Analysis:\n")
            f.write(f"  - Total Statements: {len(original_statements)}\n")
            f.write(f"  - Valid Statements: {len(valid_statements)}\n")
            f.write(f"  - Skipped (PK exists): {skipped_count}\n")
            f.write(f"  - Success Rate: {success_rate:.2f}%\n\n")
            
            f.write(f"Primary Key Configuration:\n")
            f.write(f"  - Columns: {', '.join(table_config.primary_key)}\n")
            f.write(f"  - Skip Existing: {self.config.get('verification', {}).get('skip_existing_keys', True)}\n\n")
            
            if skipped_count > 0:
                f.write(f"⚠️  {skipped_count} statements were skipped due to existing primary keys.\n")
                f.write(f"   This is normal and prevents constraint violations.\n\n")
                
            if success_rate == 100:
                f.write(f"✅ All statements are safe to execute!\n")
            elif success_rate >= 90:
                f.write(f"✅ High success rate - most statements are safe.\n")
            else:
                f.write(f"⚠️  Lower success rate - many records already exist.\n")
                
        self.logger.info(f"Verification report generated: {verification_report_path}")
        
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
        """Compare a single batch of table data with advanced monitoring"""
        batch_start_time = datetime.now()
        initial_memory = None
        
        # Memory monitoring if enabled
        monitoring_config = self.config.get('monitoring', {})
        show_memory = monitoring_config.get('show_memory_usage', False)
        show_timing = monitoring_config.get('show_timing_details', False)
        
        if show_memory:
            try:
                process = psutil.Process()
                initial_memory = process.memory_info().rss / 1024 / 1024  # MB
            except:
                pass
        
        try:
            # Fetch data from both databases
            fetch_start = datetime.now()
            source_data = self.fetch_data_batchwise(self.source_db, table_config, batch_start, batch_size)
            source_fetch_time = datetime.now() - fetch_start
            
            fetch_start = datetime.now()
            target_data = self.fetch_data_batchwise(self.target_db, table_config, batch_start, batch_size)
            target_fetch_time = datetime.now() - fetch_start
            
            # Generate hashes
            hash_start = datetime.now()
            source_hashes = self.hash_rows(source_data, table_config.primary_key)
            target_hashes = self.hash_rows(target_data, table_config.primary_key)
            hash_time = datetime.now() - hash_start
            
            # Compare hashes
            compare_start = datetime.now()
            comparison_result = self.compare_hashes(source_hashes, target_hashes)
            compare_time = datetime.now() - compare_start
            
            # Generate SQL statements if differences found and SQL generation is enabled
            sql_start = datetime.now()
            if (comparison_result['source_only'] or comparison_result['target_only'] or 
                comparison_result['different']) and self.config.get('flags', {}).get('enable_sql_generation', True):
                self.generate_sql_statements(table_config, comparison_result, source_data, target_data)
            sql_time = datetime.now() - sql_start
            
            batch_end_time = datetime.now()
            total_time = batch_end_time - batch_start_time
            
            # Memory monitoring
            final_memory = None
            memory_used = None
            if show_memory and initial_memory:
                try:
                    process = psutil.Process()
                    final_memory = process.memory_info().rss / 1024 / 1024  # MB
                    memory_used = final_memory - initial_memory
                except:
                    pass
            
            # Detailed logging if enabled
            if self.config.get('flags', {}).get('enable_detailed_logging', False) or show_timing:
                self.logger.debug(f"Batch {batch_start}-{batch_start + batch_size} timing details:")
                self.logger.debug(f"  Source fetch: {source_fetch_time.total_seconds():.2f}s")
                self.logger.debug(f"  Target fetch: {target_fetch_time.total_seconds():.2f}s")
                self.logger.debug(f"  Hashing: {hash_time.total_seconds():.2f}s")
                self.logger.debug(f"  Comparison: {compare_time.total_seconds():.2f}s")
                self.logger.debug(f"  SQL generation: {sql_time.total_seconds():.2f}s")
                self.logger.debug(f"  Total time: {total_time.total_seconds():.2f}s")
                
                if show_memory and memory_used is not None:
                    self.logger.debug(f"  Memory used: {memory_used:.2f} MB")
                
            result = {
                'source_count': len(source_hashes),
                'target_count': len(target_hashes),
                'matched_count': len(comparison_result['matched']),
                'source_only_count': len(comparison_result['source_only']),
                'target_only_count': len(comparison_result['target_only']),
                'different_count': len(comparison_result['different']),
                'batch_time_seconds': total_time.total_seconds()
            }
            
            if show_memory and memory_used is not None:
                result['memory_used_mb'] = memory_used
                
            return result
            
        except Exception as e:
            self.logger.error(f"Error comparing batch {batch_start}-{batch_start + batch_size}: {e}")
            if self.config.get('flags', {}).get('enable_detailed_logging', False):
                self.logger.error(traceback.format_exc())
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
            if self.checkpoint_manager and self.config.get('flags', {}).get('enable_restart', False):
                processed_batches = self.checkpoint_manager.get_processed_batches(
                    self.job_id, result.schema, result.table_name
                )
                
            # Create batch ranges, excluding already processed ones
            batch_ranges = []
            for i in range(total_batches):
                batch_start = i * table_config.chunk_size
                batch_end = min(batch_start + table_config.chunk_size, max(source_total, target_total))
                
                # Check if this batch was already processed (only if restart is enabled)
                is_processed = False
                if processed_batches:
                    is_processed = any(
                        start <= batch_start < end for start, end in processed_batches
                    )
                
                if not is_processed:
                    batch_ranges.append((batch_start, table_config.chunk_size))
                    
            if processed_batches:
                self.logger.info(f"Resuming from checkpoint: {len(batch_ranges)} batches remaining")
                
            # Multi-threaded batch processing
            max_threads = self.config.get('performance', {}).get('max_threads', 4)
            
            # Check if progress tracking is enabled
            show_progress = self.config.get('flags', {}).get('enable_progress_tracking', True)
            progress_update_freq = self.config.get('monitoring', {}).get('progress_update_frequency', 1)
            
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                # Submit all batch jobs
                future_to_batch = {
                    executor.submit(self.compare_table_batch, table_config, batch_start, batch_size): 
                    (batch_start, batch_size)
                    for batch_start, batch_size in batch_ranges
                }
                
                # Process completed batches with optional progress bar
                progress_bar = None
                if show_progress and batch_ranges:
                    progress_bar = tqdm(
                        total=len(batch_ranges), 
                        desc=f"Comparing {result.table_name}",
                        unit="batch",
                        ncols=100
                    )
                
                batch_counter = 0
                total_batch_time = 0
                total_memory_used = 0
                
                try:
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
                            
                            # Aggregate timing and memory stats
                            if 'batch_time_seconds' in batch_result:
                                total_batch_time += batch_result['batch_time_seconds']
                            if 'memory_used_mb' in batch_result:
                                total_memory_used += batch_result['memory_used_mb']
                            
                            batch_counter += 1
                            
                            # Save checkpoint if restart functionality is enabled
                            if self.checkpoint_manager and self.config.get('flags', {}).get('enable_restart', False):
                                checkpoint_config = self.config.get('restart', {})
                                save_frequency = checkpoint_config.get('save_frequency', 10)
                                
                                if batch_counter % save_frequency == 0:
                                    batch_duration = batch_result.get('batch_time_seconds', 0)
                                    self.checkpoint_manager.save_checkpoint(
                                        self.job_id, result.schema, result.table_name,
                                        batch_start, batch_start + batch_size, "COMPLETED",
                                        batch_duration
                                    )
                                    
                            # Log batch details to audit table if enabled
                            if (self.audit_manager and 
                                self.config.get('flags', {}).get('enable_audit_table', False) and
                                self.config.get('audit', {}).get('log_batch_details', False)):
                                self.audit_manager.log_batch_result(
                                    self.job_id, result.schema, result.table_name,
                                    batch_start, batch_start + batch_size, batch_result
                                )
                            
                            # Update progress bar based on frequency
                            if progress_bar and batch_counter % progress_update_freq == 0:
                                # Calculate average time per batch
                                avg_time = total_batch_time / batch_counter if batch_counter > 0 else 0
                                
                                # Update progress bar description with stats
                                stats = f"Avg: {avg_time:.1f}s/batch"
                                if total_memory_used > 0:
                                    avg_memory = total_memory_used / batch_counter
                                    stats += f", Mem: {avg_memory:.1f}MB"
                                
                                progress_bar.set_description(f"Comparing {result.table_name} ({stats})")
                                progress_bar.update(progress_update_freq)
                                
                        except Exception as e:
                            self.logger.error(f"Batch {batch_start}-{batch_start + batch_size} failed: {e}")
                            result.error_message = str(e)
                            
                finally:
                    if progress_bar:
                        progress_bar.close()
                        
                # Log final statistics if detailed logging is enabled
                if self.config.get('flags', {}).get('enable_detailed_logging', False):
                    avg_batch_time = total_batch_time / batch_counter if batch_counter > 0 else 0
                    self.logger.info(f"Table {result.table_name} batch statistics:")
                    self.logger.info(f"  Total batches processed: {batch_counter}")
                    self.logger.info(f"  Average time per batch: {avg_batch_time:.2f}s")
                    if total_memory_used > 0:
                        avg_memory = total_memory_used / batch_counter
                        self.logger.info(f"  Average memory per batch: {avg_memory:.2f}MB")
                            
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
            
            # Initialize database tables only if features are enabled
            flags = self.config.get('flags', {})
            
            if flags.get('enable_restart', False) and self.checkpoint_manager:
                self.checkpoint_manager.ensure_metadata_table()
                self.logger.info("Restart metadata table initialized")
                
            if flags.get('enable_audit_table', False) and self.audit_manager:
                self.audit_manager.ensure_audit_table()
                self.logger.info("Audit table initialized")
            
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
                
            # Compare all tables (with optional parallel processing)
            all_results = []
            overall_start_time = datetime.now()
            
            # Check if parallel table processing is enabled
            parallel_tables = self.config.get('advanced', {}).get('parallel_table_processing', False)
            
            if parallel_tables and len(table_configs) > 1:
                self.logger.info(f"Processing {len(table_configs)} tables in parallel")
                
                # Process tables in parallel
                max_table_threads = min(len(table_configs), max(1, self.config.get('performance', {}).get('max_threads', 4) // 2))
                
                with ThreadPoolExecutor(max_workers=max_table_threads) as table_executor:
                    # Submit all table jobs
                    future_to_table = {
                        table_executor.submit(self._process_single_table, table_config): table_config
                        for table_config in table_configs
                    }
                    
                    # Collect results
                    for future in as_completed(future_to_table):
                        table_config = future_to_table[future]
                        try:
                            result, table_stats = future.result()
                            all_results.append(result)
                            
                            # Log to audit table if enabled
                            if flags.get('enable_audit_table', False) and self.audit_manager:
                                self.audit_manager.log_job_result(
                                    self.job_id, result, 
                                    table_stats.get('avg_batch_time'), 
                                    table_stats.get('total_memory')
                                )
                                
                        except Exception as e:
                            self.logger.error(f"Parallel processing failed for table {table_config.table_name}: {e}")
                            
            else:
                # Process tables sequentially
                for table_config in table_configs:
                    result, table_stats = self._process_single_table(table_config)
                    all_results.append(result)
                    
                    # Log to audit table only if audit functionality is enabled
                    if flags.get('enable_audit_table', False) and self.audit_manager:
                        self.audit_manager.log_job_result(
                            self.job_id, result, 
                            table_stats.get('avg_batch_time'), 
                            table_stats.get('total_memory')
                        )
                
            # Post-comparison verification if enabled
            if flags.get('enable_reverification', False):
                self.logger.info("Starting post-comparison verification...")
                try:
                    self._run_verification_process(table_configs)
                except Exception as e:
                    self.logger.error(f"Verification process failed: {e}")
                    if flags.get('enable_detailed_logging', False):
                        self.logger.error(traceback.format_exc())
                        
            # Generate final report
            self._generate_comparison_report(all_results, overall_start_time)
            
            # Final statistics
            total_duration = datetime.now() - overall_start_time
            total_records_processed = sum(r.source_count + r.target_count for r in all_results)
            total_differences = sum(r.source_only_count + r.target_only_count + r.different_count for r in all_results)
            
            self.logger.info(f"DB_Sentinel comparison completed successfully: {self.job_id}")
            self.logger.info(f"Total duration: {total_duration}")
            self.logger.info(f"Total records processed: {total_records_processed:,}")
            self.logger.info(f"Total differences found: {total_differences:,}")
            
            return all_results
            
        except Exception as e:
            self.logger.error(f"Critical error in comparison process: {e}")
            self.logger.error(traceback.format_exc())
            raise
            
    def _run_verification_process(self, table_configs: List[TableConfig]):
        """
        Comprehensive post-comparison verification process
        
        This method reads generated SQL files, extracts INSERT statements,
        verifies primary keys, and creates verified SQL files ready for execution.
        """
        verification_config = self.config.get('verification', {})
        create_verified_files = verification_config.get('create_verified_files', True)
        
        self.logger.info("Starting comprehensive post-comparison verification process")
        
        verification_summary = {
            'tables_processed': 0,
            'total_statements': 0,
            'total_valid': 0,
            'total_skipped': 0,
            'verification_start': datetime.now()
        }
        
        for table_config in table_configs:
            try:
                self.logger.info(f"Verifying SQL statements for table {table_config.schema or 'DEFAULT'}.{table_config.table_name}")
                
                # Process both source and target SQL files
                table_results = {}
                
                for sql_type, sql_file in [('target', self.target_sql_file), ('source', self.source_sql_file)]:
                    if not os.path.exists(sql_file):
                        self.logger.warning(f"SQL file not found: {sql_file}")
                        continue
                        
                    # Extract INSERT statements for this table
                    table_inserts = self._extract_table_inserts(sql_file, table_config)
                    
                    if not table_inserts:
                        self.logger.info(f"No INSERT statements found for {table_config.table_name} in {sql_type} file")
                        continue
                        
                    self.logger.info(f"Found {len(table_inserts)} INSERT statements in {sql_type} file")
                    
                    # Verify the statements
                    verified_statements = self.verify_primary_keys(table_config, table_inserts)
                    
                    # Store results
                    table_results[sql_type] = {
                        'original_count': len(table_inserts),
                        'verified_count': len(verified_statements),
                        'skipped_count': len(table_inserts) - len(verified_statements),
                        'verified_statements': verified_statements,
                        'original_statements': table_inserts
                    }
                    
                    # Create verified SQL file if enabled
                    if create_verified_files and verified_statements:
                        verified_file = self._create_verified_sql_file(sql_file, table_config, verified_statements, sql_type)
                        self.logger.info(f"Created verified SQL file: {verified_file}")
                        
                    # Update summary
                    verification_summary['total_statements'] += len(table_inserts)
                    verification_summary['total_valid'] += len(verified_statements)
                    verification_summary['total_skipped'] += len(table_inserts) - len(verified_statements)
                
                # Log table-level results
                self._log_table_verification_results(table_config, table_results)
                verification_summary['tables_processed'] += 1
                
            except Exception as e:
                self.logger.error(f"Error verifying table {table_config.table_name}: {e}")
                if self.config.get('flags', {}).get('enable_detailed_logging', False):
                    self.logger.error(traceback.format_exc())
                    
        # Generate final verification summary
        verification_summary['verification_end'] = datetime.now()
        verification_summary['total_duration'] = verification_summary['verification_end'] - verification_summary['verification_start']
        
        self._generate_verification_summary(verification_summary)
        
    def _extract_table_inserts(self, sql_file: str, table_config: TableConfig) -> List[str]:
        """
        Extract INSERT statements for a specific table from SQL file
        
        Handles multi-line statements and comments properly.
        """
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Remove comments and normalize whitespace
            content = self._clean_sql_content(content)
            
            # Find INSERT statements for this table
            table_pattern = f"{table_config.schema}.{table_config.table_name}" if table_config.schema else table_config.table_name
            
            # More flexible pattern matching for INSERT statements
            # Handles: INSERT INTO [schema.]table or INSERT INTO "schema"."table"
            insert_patterns = [
                rf'INSERT\s+INTO\s+{re.escape(table_pattern)}\s*\([^)]+\)\s*VALUES\s*\([^)]+\)\s*;?',
                rf'INSERT\s+INTO\s+"{re.escape(table_config.schema or "")}"\s*\.\s*"{re.escape(table_config.table_name)}"\s*\([^)]+\)\s*VALUES\s*\([^)]+\)\s*;?',
                rf'INSERT\s+INTO\s+{re.escape(table_config.table_name)}\s*\([^)]+\)\s*VALUES\s*\([^)]+\)\s*;?'
            ]
            
            insert_statements = []
            
            for pattern in insert_patterns:
                matches = re.finditer(pattern, content, re.IGNORECASE | re.MULTILINE | re.DOTALL)
                for match in matches:
                    stmt = match.group(0).strip()
                    if stmt and stmt not in insert_statements:
                        insert_statements.append(stmt)
                        
            # If no pattern matches, try a more general approach
            if not insert_statements:
                insert_statements = self._extract_inserts_by_line_parsing(content, table_config)
                
            return insert_statements
            
        except Exception as e:
            self.logger.error(f"Error extracting INSERT statements from {sql_file}: {e}")
            return []
            
    def _clean_sql_content(self, content: str) -> str:
        """Clean SQL content by removing comments and normalizing whitespace"""
        # Remove SQL comments (-- style)
        content = re.sub(r'--.*
            
    def _generate_comparison_report(self, results: List[ComparisonResult], start_time: datetime):
        """Generate comprehensive comparison report with advanced metrics"""
        report_file = self.config.get('paths', {}).get('comparison_report', './output/comparison_report.txt')
        Path(report_file).parent.mkdir(parents=True, exist_ok=True)
        
        total_duration = datetime.now() - start_time
        
        # Calculate summary statistics
        total_tables = len(results)
        successful_tables = len([r for r in results if r.status == "COMPLETED"])
        failed_tables = len([r for r in results if r.status == "FAILED"])
        total_source_records = sum(r.source_count for r in results)
        total_target_records = sum(r.target_count for r in results)
        total_matched = sum(r.matched_count for r in results)
        total_source_only = sum(r.source_only_count for r in results)
        total_target_only = sum(r.target_only_count for r in results)
        total_different = sum(r.different_count for r in results)
        total_differences = total_source_only + total_target_only + total_different
        
        # Calculate success rate
        total_processed = total_matched + total_differences
        success_rate = (total_matched / total_processed * 100) if total_processed > 0 else 0
        
        with open(report_file, 'w') as f:
            # Header
            f.write("DB_Sentinel_util Comprehensive Comparison Report\n")
            f.write("=" * 80 + "\n\n")
            
            # Job Information
            f.write("JOB INFORMATION\n")
            f.write("-" * 40 + "\n")
            f.write(f"Job ID: {self.job_id}\n")
            f.write(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Duration: {total_duration}\n")
            f.write(f"Configuration Features:\n")
            
            flags = self.config.get('flags', {})
            f.write(f"  - Restart/Resume: {'Enabled' if flags.get('enable_restart', False) else 'Disabled'}\n")
            f.write(f"  - Audit Table: {'Enabled' if flags.get('enable_audit_table', False) else 'Disabled'}\n")
            f.write(f"  - Verification: {'Enabled' if flags.get('enable_reverification', False) else 'Disabled'}\n")
            f.write(f"  - SQL Generation: {'Enabled' if flags.get('enable_sql_generation', True) else 'Disabled'}\n")
            f.write(f"  - Progress Tracking: {'Enabled' if flags.get('enable_progress_tracking', True) else 'Disabled'}\n\n")
            
            # Summary Statistics
            f.write("SUMMARY STATISTICS\n")
            f.write("-" * 40 + "\n")
            f.write(f"Tables Processed: {total_tables}\n")
            f.write(f"  - Successful: {successful_tables}\n")
            f.write(f"  - Failed: {failed_tables}\n\n")
            
            f.write("Record Counts:\n")
            f.write(f"  - Total Source Records: {total_source_records:,}\n")
            f.write(f"  - Total Target Records: {total_target_records:,}\n")
            f.write(f"  - Total Matched Records: {total_matched:,}\n")
            f.write(f"  - Records Only in Source: {total_source_only:,}\n")
            f.write(f"  - Records Only in Target: {total_target_only:,}\n")
            f.write(f"  - Records with Differences: {total_different:,}\n")
            f.write(f"  - Total Differences Found: {total_differences:,}\n\n")
            
            f.write(f"Data Quality Metrics:\n")
            f.write(f"  - Match Rate: {success_rate:.2f}%\n")
            f.write(f"  - Difference Rate: {(100 - success_rate):.2f}%\n\n")
            
            # Performance Metrics
            if total_duration.total_seconds() > 0:
                records_per_second = total_processed / total_duration.total_seconds()
                f.write("PERFORMANCE METRICS\n")
                f.write("-" * 40 + "\n")
                f.write(f"Processing Rate: {records_per_second:,.0f} records/second\n")
                f.write(f"Average Duration per Table: {total_duration / total_tables}\n")
                
                # Thread information
                max_threads = self.config.get('performance', {}).get('max_threads', 4)
                f.write(f"Max Threads Used: {max_threads}\n\n")
            
            # Detailed Table Results
            f.write("DETAILED TABLE RESULTS\n")
            f.write("-" * 40 + "\n")
            
            for i, result in enumerate(results, 1):
                f.write(f"{i}. Table: {result.schema}.{result.table_name}\n")
                f.write(f"   Status: {result.status}\n")
                f.write(f"   Duration: {result.duration if result.duration else 'N/A'}\n")
                f.write(f"   Source Count: {result.source_count:,}\n")
                f.write(f"   Target Count: {result.target_count:,}\n")
                f.write(f"   Matched: {result.matched_count:,}\n")
                f.write(f"   Source Only: {result.source_only_count:,}\n")
                f.write(f"   Target Only: {result.target_only_count:,}\n")
                f.write(f"   Different: {result.different_count:,}\n")
                
                # Calculate table-specific metrics
                table_total = result.matched_count + result.source_only_count + result.target_only_count + result.different_count
                if table_total > 0:
                    table_success_rate = (result.matched_count / table_total) * 100
                    f.write(f"   Match Rate: {table_success_rate:.2f}%\n")
                
                if result.duration:
                    table_rate = table_total / result.duration.total_seconds() if result.duration.total_seconds() > 0 else 0
                    f.write(f"   Processing Rate: {table_rate:,.0f} records/second\n")
                
                if result.error_message:
                    f.write(f"   Error: {result.error_message}\n")
                f.write("\n")
            
            # Configuration Summary
            f.write("CONFIGURATION SUMMARY\n")
            f.write("-" * 40 + "\n")
            f.write(f"Hashing Algorithm: {self.config.get('hashing', {}).get('algorithm', 'md5').upper()}\n")
            f.write(f"Chunk Sizes Used: {list(set(tc.chunk_size for tc in [TableConfig(table_name=t['table_name'], primary_key=t['primary_key'], chunk_size=t.get('chunk_size', 10000)) for t in self.config.get('tables', [])]))}\n")
            
            # Output Files
            f.write("\nOUTPUT FILES\n")
            f.write("-" * 40 + "\n")
            if flags.get('enable_sql_generation', True):
                f.write(f"Source SQL File: {self.source_sql_file}\n")
                f.write(f"Target SQL File: {self.target_sql_file}\n")
            f.write(f"Audit Log: {self.config.get('paths', {}).get('audit_log', './logs/audit.log')}\n")
            f.write(f"This Report: {report_file}\n\n")
            
            # Recommendations
            f.write("RECOMMENDATIONS\n")
            f.write("-" * 40 + "\n")
            
            if failed_tables > 0:
                f.write("⚠️  Some tables failed to process. Check the audit log for details.\n")
            
            if total_differences > 1000:
                f.write("⚠️  Large number of differences found. Consider investigating data quality.\n")
            
            if success_rate < 95:
                f.write("⚠️  Low match rate detected. Review data synchronization processes.\n")
            
            if total_differences == 0:
                f.write("✅ All tables are perfectly synchronized!\n")
            elif success_rate > 99:
                f.write("✅ Excellent data quality - very few differences found.\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("Report generated by DB_Sentinel_util\n")
            f.write(f"Generation time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
        self.logger.info(f"Comprehensive comparison report generated: {report_file}")
        
        # Also create a JSON summary for programmatic access
        json_report = {
            'job_id': self.job_id,
            'start_time': start_time.isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': total_duration.total_seconds(),
            'summary': {
                'total_tables': total_tables,
                'successful_tables': successful_tables,
                'failed_tables': failed_tables,
                'total_source_records': total_source_records,
                'total_target_records': total_target_records,
                'total_matched': total_matched,
                'total_differences': total_differences,
                'success_rate': success_rate
            },
            'table_results': [
                {
                    'schema': r.schema,
                    'table_name': r.table_name,
                    'status': r.status,
                    'duration_seconds': r.duration.total_seconds() if r.duration else None,
                    'source_count': r.source_count,
                    'target_count': r.target_count,
                    'matched_count': r.matched_count,
                    'source_only_count': r.source_only_count,
                    'target_only_count': r.target_only_count,
                    'different_count': r.different_count,
                    'error_message': r.error_message
                }
                for r in results
            ]
        }
        
        import json
        json_report_file = report_file.replace('.txt', '.json')
        with open(json_report_file, 'w') as f:
            json.dump(json_report, f, indent=2)
        
        self.logger.info(f"JSON summary report generated: {json_report_file}")


def main():
    """Main entry point with enhanced command line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='DB_Sentinel_util - Advanced Oracle Database Table Comparison Utility',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python DB_Sentinel_util.py config.yaml
  python DB_Sentinel_util.py config.yaml --dry-run
  python DB_Sentinel_util.py config.yaml --check-config
  python DB_Sentinel_util.py config.yaml --show-features
        """
    )
    
    parser.add_argument('config', help='Path to YAML configuration file')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Validate configuration and show what would be processed')
    parser.add_argument('--check-config', action='store_true',
                       help='Validate configuration file only')
    parser.add_argument('--show-features', action='store_true',
                       help='Show enabled/disabled features')
    parser.add_argument('--version', action='version', version='DB_Sentinel_util 1.0.0')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        print(f"❌ Configuration file not found: {args.config}")
        sys.exit(1)
        
    try:
        # Load and validate configuration
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
            
        # Validate required sections
        required_sections = ['source_db', 'target_db']
        for section in required_sections:
            if section not in config:
                print(f"❌ Missing required configuration section: {section}")
                sys.exit(1)
                
        if args.check_config:
            print("✅ Configuration file is valid")
            return
            
        if args.show_features:
            _show_features(config)
            return
            
        if args.dry_run:
            _dry_run(config)
            return
            
        # Run the actual comparison
        comparator = DatabaseComparator(args.config)
        results = comparator.run_comparison()
        
        # Print summary
        print(f"\n🎉 Comparison completed successfully!")
        print(f"📊 Summary:")
        print(f"   - Tables processed: {len(results)}")
        print(f"   - Successful: {len([r for r in results if r.status == 'COMPLETED'])}")
        print(f"   - Failed: {len([r for r in results if r.status == 'FAILED'])}")
        
        total_differences = sum(r.source_only_count + r.target_only_count + r.different_count for r in results)
        print(f"   - Total differences: {total_differences:,}")
        
        if total_differences == 0:
            print("✅ All tables are perfectly synchronized!")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)


def _show_features(config: Dict[str, Any]):
    """Show enabled/disabled features"""
    print("🛡️  DB_Sentinel_util Feature Status")
    print("=" * 50)
    
    flags = config.get('flags', {})
    
    features = [
        ('Restart/Resume Functionality', flags.get('enable_restart', False)),
        ('Database Audit Tables', flags.get('enable_audit_table', False)),
        ('Post-Comparison Verification', flags.get('enable_reverification', False)),
        ('Progress Tracking', flags.get('enable_progress_tracking', True)),
        ('SQL File Generation', flags.get('enable_sql_generation', True)),
        ('Detailed Logging', flags.get('enable_detailed_logging', False)),
    ]
    
    # Advanced features
    advanced = config.get('advanced', {})
    monitoring = config.get('monitoring', {})
    
    advanced_features = [
        ('Parallel Table Processing', advanced.get('parallel_table_processing', False)),
        ('Auto Chunk Size Adjustment', advanced.get('chunk_size_auto_adjust', False)),
        ('Memory Usage Monitoring', monitoring.get('show_memory_usage', False)),
        ('Timing Details', monitoring.get('show_timing_details', False)),
    ]
    
    print("Core Features:")
    for feature, enabled in features:
        status = "✅ Enabled " if enabled else "❌ Disabled"
        print(f"  {status} - {feature}")
        
    print("\nAdvanced Features:")
    for feature, enabled in advanced_features:
        status = "✅ Enabled " if enabled else "❌ Disabled"
        print(f"  {status} - {feature}")
        
    # Configuration summary
    print(f"\nConfiguration Summary:")
    print(f"  - Tables to compare: {len(config.get('tables', []))}")
    print(f"  - Max threads: {config.get('performance', {}).get('max_threads', 4)}")
    print(f"  - Hashing algorithm: {config.get('hashing', {}).get('algorithm', 'md5').upper()}")
    
    if flags.get('enable_audit_table', False):
        audit_config = config.get('audit', {})
        print(f"  - Audit table: {audit_config.get('audit_table_name', 'DB_SENTINEL_AUDIT')}")
        print(f"  - Batch details logging: {'Yes' if audit_config.get('log_batch_details', False) else 'No'}")
        
    if flags.get('enable_restart', False):
        restart_config = config.get('restart', {})
        print(f"  - Checkpoint table: {restart_config.get('metadata_table_name', 'DB_SENTINEL_CHECKPOINTS')}")
        print(f"  - Checkpoint frequency: Every {restart_config.get('save_frequency', 10)} batches")


def _dry_run(config: Dict[str, Any]):
    """Perform a dry run to show what would be processed"""
    print("🧪 DB_Sentinel_util Dry Run")
    print("=" * 50)
    
    # Create timestamp for this dry run
    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Show database connections (without passwords)
    source_db = config['source_db'].copy()
    target_db = config['target_db'].copy()
    source_db['password'] = '***'
    target_db['password'] = '***'
    
    print(f"Source Database: {source_db['user']}@{source_db['dsn']}")
    print(f"Target Database: {target_db['user']}@{target_db['dsn']}")
    print()
    
    # Show tables that would be processed
    tables = config.get('tables', [])
    if not tables:
        print("❌ No tables configured for comparison")
        return
        
    print(f"Tables to be compared ({len(tables)}):")
    for i, table in enumerate(tables, 1):
        schema = table.get('schema') or config.get('schema', 'DEFAULT')
        print(f"  {i}. {schema}.{table['table_name']}")
        print(f"     - Primary Key: {table['primary_key']}")
        print(f"     - Chunk Size: {table.get('chunk_size', 10000):,}")
        
        if table.get('columns'):
            print(f"     - Columns: {len(table['columns'])} specified")
        else:
            print(f"     - Columns: All columns")
            
        if table.get('where_clause'):
            print(f"     - Filter: {table['where_clause']}")
        print()
        
    # Show output file structure with timestamps
    paths = config.get('paths', {})
    base_output_dir = Path(paths.get('source_sql_output', './output/source_sync_statements.sql')).parent
    run_output_dir = base_output_dir / f"run_{run_timestamp}"
    
    print("Output File Structure:")
    print(f"  📁 Run Directory: {run_output_dir}")
    print(f"     📝 Audit Log: audit_{run_timestamp}.log")
    print(f"     📊 Comparison Report: comparison_report_{run_timestamp}.txt")
    print(f"     📊 JSON Report: comparison_report_{run_timestamp}.json")
    print()
    
    print("  📄 Combined SQL Files (All Tables):")
    print(f"     📝 source_sync_all_tables_{run_timestamp}.sql")
    print(f"     📝 target_sync_all_tables_{run_timestamp}.sql")
    print()
    
    print("  📄 Table-Specific SQL Files:")
    for table in tables:
        schema = table.get('schema') or config.get('schema', 'DEFAULT')
        safe_name = re.sub(r'[^\w\-_]', '_', f"{schema}_{table['table_name']}")
        print(f"     🔹 {schema}.{table['table_name']}:")
        print(f"        📝 source_sync_{safe_name}_{run_timestamp}.sql")
        print(f"        📝 target_sync_{safe_name}_{run_timestamp}.sql")
        
        # Show verification files if enabled
        if config.get('flags', {}).get('enable_reverification', False):
            print(f"        ✅ source_sync_{safe_name}_verified_{run_timestamp}.sql")
            print(f"        ✅ target_sync_{safe_name}_verified_{run_timestamp}.sql")
    print()
        
    # Show features that would be used
    _show_features(config)
    
    print(f"\n📅 This run would use timestamp: {run_timestamp}")
    print("⚠️  This was a dry run. No actual comparison was performed.")
    print("   Remove --dry-run to execute the comparison.")


if __name__ == "__main__":
    main()
, '', content, flags=re.MULTILINE)
        
        # Remove /* */ style comments
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
        
        # Normalize whitespace but preserve structure
        content = re.sub(r'\s+', ' ', content)
        
        return content.strip()
        
    def _extract_inserts_by_line_parsing(self, content: str, table_config: TableConfig) -> List[str]:
        """
        Fallback method to extract INSERT statements by parsing line by line
        
        Used when regex patterns don't match due to complex formatting.
        """
        lines = content.split('\n')
        insert_statements = []
        current_statement = ""
        in_insert = False
        
        table_identifiers = [
            table_config.table_name.upper(),
            f"{table_config.schema}.{table_config.table_name}".upper() if table_config.schema else None,
            f'"{table_config.schema}"."{table_config.table_name}"'.upper() if table_config.schema else None
        ]
        table_identifiers = [t for t in table_identifiers if t]
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('--'):
                continue
                
            if line.upper().startswith('INSERT INTO'):
                # Check if this INSERT is for our table
                line_upper = line.upper()
                for table_id in table_identifiers:
                    if table_id in line_upper:
                        in_insert = True
                        current_statement = line
                        break
                        
            elif in_insert:
                current_statement += " " + line
                
                # Check if statement is complete
                if line.endswith(';') or (current_statement.count('(') == current_statement.count(')')):
                    insert_statements.append(current_statement.strip())
                    current_statement = ""
                    in_insert = False
                    
        return insert_statements
        
    def _create_verified_sql_file(self, original_file: str, table_config: TableConfig, 
                                 verified_statements: List[str], sql_type: str) -> str:
        """
        Create a new SQL file containing only verified statements in the run directory
        
        Includes header comments and summary information.
        """
        # Generate verified file path in the run directory
        table_name = table_config.table_name.upper()
        schema_name = (table_config.schema or "DEFAULT").upper()
        safe_table_name = re.sub(r'[^\w\-_]', '_', f"{schema_name}_{table_name}")
        
        verified_file = self.output_dir / f"{sql_type}_sync_{safe_table_name}_verified_{self.run_timestamp}.sql"
        
        # Create header
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        header = f"""-- DB_Sentinel_util Verified SQL Statements
-- Original file: {Path(original_file).name}
-- Verification time: {timestamp}
-- Job ID: {self.job_id}
-- Run Timestamp: {self.run_timestamp}
-- Table: {table_config.schema or 'DEFAULT'}.{table_config.table_name}
-- 
-- VERIFICATION SUMMARY:
-- Total statements verified: {len(verified_statements)}
-- These statements have been verified to not cause primary key constraint violations
-- Safe to execute in production
--
-- Primary Key Columns: {', '.join(table_config.primary_key)}
--

"""
        
        try:
            with open(verified_file, 'w', encoding='utf-8') as f:
                f.write(header)
                
                if verified_statements:
                    f.write(f"-- Verified {sql_type.upper()} statements for {table_config.table_name}\n")
                    f.write(f"-- Generated: {timestamp}\n\n")
                    
                    # Group statements for better organization
                    sql_config = self.config.get('sql_generation', {})
                    batch_statements = sql_config.get('batch_statements', False)
                    batch_size = sql_config.get('statement_batch_size', 100)
                    
                    if batch_statements:
                        for i in range(0, len(verified_statements), batch_size):
                            batch = verified_statements[i:i + batch_size]
                            f.write(f"-- Batch {i//batch_size + 1}\n")
                            f.writelines([stmt + '\n' for stmt in batch])
                            f.write("COMMIT;\n\n")
                    else:
                        f.writelines([stmt + '\n' for stmt in verified_statements])
                        
                else:
                    f.write("-- No verified statements found\n")
                    f.write("-- All statements were skipped due to existing primary keys\n")
                    
                f.write(f"\n-- End of verified statements\n")
                f.write(f"-- File generated by DB_Sentinel_util at {timestamp}\n")
                
            return str(verified_file)
            
        except Exception as e:
            self.logger.error(f"Error creating verified SQL file: {e}")
            return ""
            
    def _log_table_verification_results(self, table_config: TableConfig, table_results: Dict[str, Dict]):
        """Log detailed verification results for a table"""
        table_name = f"{table_config.schema or 'DEFAULT'}.{table_config.table_name}"
        
        self.logger.info(f"Verification results for {table_name}:")
        
        for sql_type, results in table_results.items():
            original_count = results['original_count']
            verified_count = results['verified_count']
            skipped_count = results['skipped_count']
            success_rate = (verified_count / original_count * 100) if original_count > 0 else 0
            
            self.logger.info(f"  {sql_type.upper()} file:")
            self.logger.info(f"    - Original statements: {original_count}")
            self.logger.info(f"    - Verified statements: {verified_count}")
            self.logger.info(f"    - Skipped statements: {skipped_count}")
            self.logger.info(f"    - Success rate: {success_rate:.1f}%")
            
            if skipped_count > 0:
                self.logger.info(f"    - {skipped_count} statements skipped (primary keys already exist)")
                
    def _generate_verification_summary(self, summary: Dict[str, Any]):
        """Generate comprehensive verification summary report"""
        report_path = self.config.get('paths', {}).get('comparison_report', './output/comparison_report.txt')
        verification_summary_path = report_path.replace('.txt', '_verification_summary.txt')
        
        total_statements = summary['total_statements']
        total_valid = summary['total_valid']
        total_skipped = summary['total_skipped']
        overall_success_rate = (total_valid / total_statements * 100) if total_statements > 0 else 0
        
        try:
            with open(verification_summary_path, 'w', encoding='utf-8') as f:
                f.write("DB_Sentinel_util Verification Summary Report\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"Job ID: {self.job_id}\n")
                f.write(f"Verification Start: {summary['verification_start'].strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Verification End: {summary['verification_end'].strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Duration: {summary['total_duration']}\n\n")
                
                f.write("VERIFICATION SUMMARY\n")
                f.write("-" * 30 + "\n")
                f.write(f"Tables Processed: {summary['tables_processed']}\n")
                f.write(f"Total Statements Analyzed: {total_statements:,}\n")
                f.write(f"Valid Statements: {total_valid:,}\n")
                f.write(f"Skipped Statements: {total_skipped:,}\n")
                f.write(f"Overall Success Rate: {overall_success_rate:.2f}%\n\n")
                
                f.write("INTERPRETATION\n")
                f.write("-" * 30 + "\n")
                
                if total_statements == 0:
                    f.write("ℹ️  No statements found for verification.\n")
                elif total_skipped == 0:
                    f.write("✅ Perfect! All statements are safe to execute.\n")
                    f.write("   No primary key conflicts detected.\n")
                elif overall_success_rate >= 95:
                    f.write("✅ Excellent! Very few conflicts detected.\n")
                    f.write(f"   Only {total_skipped} statements skipped due to existing keys.\n")
                elif overall_success_rate >= 80:
                    f.write("⚠️  Good verification rate with some conflicts.\n")
                    f.write(f"   {total_skipped} statements skipped - this is normal in incremental syncs.\n")
                else:
                    f.write("⚠️  High number of conflicts detected.\n")
                    f.write(f"   {total_skipped} statements skipped - consider investigating data freshness.\n")
                    
                f.write("\nNEXT STEPS\n")
                f.write("-" * 30 + "\n")
                f.write("1. Review verified SQL files (filename_verified.sql)\n")
                f.write("2. Execute verified statements in your target database\n")
                f.write("3. Monitor execution for any remaining issues\n")
                f.write("4. Check audit logs for detailed verification information\n\n")
                
                f.write("CONFIGURATION USED\n")
                f.write("-" * 30 + "\n")
                verification_config = self.config.get('verification', {})
                f.write(f"Batch Size: {verification_config.get('batch_size', 1000)}\n")
                f.write(f"Max Threads: {verification_config.get('max_threads', 2)}\n")
                f.write(f"Skip Existing Keys: {verification_config.get('skip_existing_keys', True)}\n")
                f.write(f"Create Verified Files: {verification_config.get('create_verified_files', True)}\n")
                
            self.logger.info(f"Verification summary report generated: {verification_summary_path}")
            
        except Exception as e:
            self.logger.error(f"Error generating verification summary: {e}")
            
    def _generate_comparison_report(self, results: List[ComparisonResult], start_time: datetime):
        """Generate comprehensive comparison report with advanced metrics"""
        report_file = self.config.get('paths', {}).get('comparison_report', './output/comparison_report.txt')
        Path(report_file).parent.mkdir(parents=True, exist_ok=True)
        
        total_duration = datetime.now() - start_time
        
        # Calculate summary statistics
        total_tables = len(results)
        successful_tables = len([r for r in results if r.status == "COMPLETED"])
        failed_tables = len([r for r in results if r.status == "FAILED"])
        total_source_records = sum(r.source_count for r in results)
        total_target_records = sum(r.target_count for r in results)
        total_matched = sum(r.matched_count for r in results)
        total_source_only = sum(r.source_only_count for r in results)
        total_target_only = sum(r.target_only_count for r in results)
        total_different = sum(r.different_count for r in results)
        total_differences = total_source_only + total_target_only + total_different
        
        # Calculate success rate
        total_processed = total_matched + total_differences
        success_rate = (total_matched / total_processed * 100) if total_processed > 0 else 0
        
        with open(report_file, 'w') as f:
            # Header
            f.write("DB_Sentinel_util Comprehensive Comparison Report\n")
            f.write("=" * 80 + "\n\n")
            
            # Job Information
            f.write("JOB INFORMATION\n")
            f.write("-" * 40 + "\n")
            f.write(f"Job ID: {self.job_id}\n")
            f.write(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Duration: {total_duration}\n")
            f.write(f"Configuration Features:\n")
            
            flags = self.config.get('flags', {})
            f.write(f"  - Restart/Resume: {'Enabled' if flags.get('enable_restart', False) else 'Disabled'}\n")
            f.write(f"  - Audit Table: {'Enabled' if flags.get('enable_audit_table', False) else 'Disabled'}\n")
            f.write(f"  - Verification: {'Enabled' if flags.get('enable_reverification', False) else 'Disabled'}\n")
            f.write(f"  - SQL Generation: {'Enabled' if flags.get('enable_sql_generation', True) else 'Disabled'}\n")
            f.write(f"  - Progress Tracking: {'Enabled' if flags.get('enable_progress_tracking', True) else 'Disabled'}\n\n")
            
            # Summary Statistics
            f.write("SUMMARY STATISTICS\n")
            f.write("-" * 40 + "\n")
            f.write(f"Tables Processed: {total_tables}\n")
            f.write(f"  - Successful: {successful_tables}\n")
            f.write(f"  - Failed: {failed_tables}\n\n")
            
            f.write("Record Counts:\n")
            f.write(f"  - Total Source Records: {total_source_records:,}\n")
            f.write(f"  - Total Target Records: {total_target_records:,}\n")
            f.write(f"  - Total Matched Records: {total_matched:,}\n")
            f.write(f"  - Records Only in Source: {total_source_only:,}\n")
            f.write(f"  - Records Only in Target: {total_target_only:,}\n")
            f.write(f"  - Records with Differences: {total_different:,}\n")
            f.write(f"  - Total Differences Found: {total_differences:,}\n\n")
            
            f.write(f"Data Quality Metrics:\n")
            f.write(f"  - Match Rate: {success_rate:.2f}%\n")
            f.write(f"  - Difference Rate: {(100 - success_rate):.2f}%\n\n")
            
            # Performance Metrics
            if total_duration.total_seconds() > 0:
                records_per_second = total_processed / total_duration.total_seconds()
                f.write("PERFORMANCE METRICS\n")
                f.write("-" * 40 + "\n")
                f.write(f"Processing Rate: {records_per_second:,.0f} records/second\n")
                f.write(f"Average Duration per Table: {total_duration / total_tables}\n")
                
                # Thread information
                max_threads = self.config.get('performance', {}).get('max_threads', 4)
                f.write(f"Max Threads Used: {max_threads}\n\n")
            
            # Detailed Table Results
            f.write("DETAILED TABLE RESULTS\n")
            f.write("-" * 40 + "\n")
            
            for i, result in enumerate(results, 1):
                f.write(f"{i}. Table: {result.schema}.{result.table_name}\n")
                f.write(f"   Status: {result.status}\n")
                f.write(f"   Duration: {result.duration if result.duration else 'N/A'}\n")
                f.write(f"   Source Count: {result.source_count:,}\n")
                f.write(f"   Target Count: {result.target_count:,}\n")
                f.write(f"   Matched: {result.matched_count:,}\n")
                f.write(f"   Source Only: {result.source_only_count:,}\n")
                f.write(f"   Target Only: {result.target_only_count:,}\n")
                f.write(f"   Different: {result.different_count:,}\n")
                
                # Calculate table-specific metrics
                table_total = result.matched_count + result.source_only_count + result.target_only_count + result.different_count
                if table_total > 0:
                    table_success_rate = (result.matched_count / table_total) * 100
                    f.write(f"   Match Rate: {table_success_rate:.2f}%\n")
                
                if result.duration:
                    table_rate = table_total / result.duration.total_seconds() if result.duration.total_seconds() > 0 else 0
                    f.write(f"   Processing Rate: {table_rate:,.0f} records/second\n")
                
                if result.error_message:
                    f.write(f"   Error: {result.error_message}\n")
                f.write("\n")
            
            # Configuration Summary
            f.write("CONFIGURATION SUMMARY\n")
            f.write("-" * 40 + "\n")
            f.write(f"Hashing Algorithm: {self.config.get('hashing', {}).get('algorithm', 'md5').upper()}\n")
            f.write(f"Chunk Sizes Used: {list(set(tc.chunk_size for tc in [TableConfig(table_name=t['table_name'], primary_key=t['primary_key'], chunk_size=t.get('chunk_size', 10000)) for t in self.config.get('tables', [])]))}\n")
            
            # Output Files
            f.write("\nOUTPUT FILES\n")
            f.write("-" * 40 + "\n")
            if flags.get('enable_sql_generation', True):
                f.write(f"Source SQL File: {self.source_sql_file}\n")
                f.write(f"Target SQL File: {self.target_sql_file}\n")
            f.write(f"Audit Log: {self.config.get('paths', {}).get('audit_log', './logs/audit.log')}\n")
            f.write(f"This Report: {report_file}\n\n")
            
            # Recommendations
            f.write("RECOMMENDATIONS\n")
            f.write("-" * 40 + "\n")
            
            if failed_tables > 0:
                f.write("⚠️  Some tables failed to process. Check the audit log for details.\n")
            
            if total_differences > 1000:
                f.write("⚠️  Large number of differences found. Consider investigating data quality.\n")
            
            if success_rate < 95:
                f.write("⚠️  Low match rate detected. Review data synchronization processes.\n")
            
            if total_differences == 0:
                f.write("✅ All tables are perfectly synchronized!\n")
            elif success_rate > 99:
                f.write("✅ Excellent data quality - very few differences found.\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("Report generated by DB_Sentinel_util\n")
            f.write(f"Generation time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
        self.logger.info(f"Comprehensive comparison report generated: {report_file}")
        
        # Also create a JSON summary for programmatic access
        json_report = {
            'job_id': self.job_id,
            'start_time': start_time.isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': total_duration.total_seconds(),
            'summary': {
                'total_tables': total_tables,
                'successful_tables': successful_tables,
                'failed_tables': failed_tables,
                'total_source_records': total_source_records,
                'total_target_records': total_target_records,
                'total_matched': total_matched,
                'total_differences': total_differences,
                'success_rate': success_rate
            },
            'table_results': [
                {
                    'schema': r.schema,
                    'table_name': r.table_name,
                    'status': r.status,
                    'duration_seconds': r.duration.total_seconds() if r.duration else None,
                    'source_count': r.source_count,
                    'target_count': r.target_count,
                    'matched_count': r.matched_count,
                    'source_only_count': r.source_only_count,
                    'target_only_count': r.target_only_count,
                    'different_count': r.different_count,
                    'error_message': r.error_message
                }
                for r in results
            ]
        }
        
        import json
        json_report_file = report_file.replace('.txt', '.json')
        with open(json_report_file, 'w') as f:
            json.dump(json_report, f, indent=2)
        
        self.logger.info(f"JSON summary report generated: {json_report_file}")


def main():
    """Main entry point with enhanced command line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='DB_Sentinel_util - Advanced Oracle Database Table Comparison Utility',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python DB_Sentinel_util.py config.yaml
  python DB_Sentinel_util.py config.yaml --dry-run
  python DB_Sentinel_util.py config.yaml --check-config
  python DB_Sentinel_util.py config.yaml --show-features
        """
    )
    
    parser.add_argument('config', help='Path to YAML configuration file')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Validate configuration and show what would be processed')
    parser.add_argument('--check-config', action='store_true',
                       help='Validate configuration file only')
    parser.add_argument('--show-features', action='store_true',
                       help='Show enabled/disabled features')
    parser.add_argument('--version', action='version', version='DB_Sentinel_util 1.0.0')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        print(f"❌ Configuration file not found: {args.config}")
        sys.exit(1)
        
    try:
        # Load and validate configuration
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
            
        # Validate required sections
        required_sections = ['source_db', 'target_db']
        for section in required_sections:
            if section not in config:
                print(f"❌ Missing required configuration section: {section}")
                sys.exit(1)
                
        if args.check_config:
            print("✅ Configuration file is valid")
            return
            
        if args.show_features:
            _show_features(config)
            return
            
        if args.dry_run:
            _dry_run(config)
            return
            
        # Run the actual comparison
        comparator = DatabaseComparator(args.config)
        results = comparator.run_comparison()
        
        # Print summary
        print(f"\n🎉 Comparison completed successfully!")
        print(f"📊 Summary:")
        print(f"   - Tables processed: {len(results)}")
        print(f"   - Successful: {len([r for r in results if r.status == 'COMPLETED'])}")
        print(f"   - Failed: {len([r for r in results if r.status == 'FAILED'])}")
        
        total_differences = sum(r.source_only_count + r.target_only_count + r.different_count for r in results)
        print(f"   - Total differences: {total_differences:,}")
        
        if total_differences == 0:
            print("✅ All tables are perfectly synchronized!")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)


def _show_features(config: Dict[str, Any]):
    """Show enabled/disabled features"""
    print("🛡️  DB_Sentinel_util Feature Status")
    print("=" * 50)
    
    flags = config.get('flags', {})
    
    features = [
        ('Restart/Resume Functionality', flags.get('enable_restart', False)),
        ('Database Audit Tables', flags.get('enable_audit_table', False)),
        ('Post-Comparison Verification', flags.get('enable_reverification', False)),
        ('Progress Tracking', flags.get('enable_progress_tracking', True)),
        ('SQL File Generation', flags.get('enable_sql_generation', True)),
        ('Detailed Logging', flags.get('enable_detailed_logging', False)),
    ]
    
    # Advanced features
    advanced = config.get('advanced', {})
    monitoring = config.get('monitoring', {})
    
    advanced_features = [
        ('Parallel Table Processing', advanced.get('parallel_table_processing', False)),
        ('Auto Chunk Size Adjustment', advanced.get('chunk_size_auto_adjust', False)),
        ('Memory Usage Monitoring', monitoring.get('show_memory_usage', False)),
        ('Timing Details', monitoring.get('show_timing_details', False)),
    ]
    
    print("Core Features:")
    for feature, enabled in features:
        status = "✅ Enabled " if enabled else "❌ Disabled"
        print(f"  {status} - {feature}")
        
    print("\nAdvanced Features:")
    for feature, enabled in advanced_features:
        status = "✅ Enabled " if enabled else "❌ Disabled"
        print(f"  {status} - {feature}")
        
    # Configuration summary
    print(f"\nConfiguration Summary:")
    print(f"  - Tables to compare: {len(config.get('tables', []))}")
    print(f"  - Max threads: {config.get('performance', {}).get('max_threads', 4)}")
    print(f"  - Hashing algorithm: {config.get('hashing', {}).get('algorithm', 'md5').upper()}")
    
    if flags.get('enable_audit_table', False):
        audit_config = config.get('audit', {})
        print(f"  - Audit table: {audit_config.get('audit_table_name', 'DB_SENTINEL_AUDIT')}")
        print(f"  - Batch details logging: {'Yes' if audit_config.get('log_batch_details', False) else 'No'}")
        
    if flags.get('enable_restart', False):
        restart_config = config.get('restart', {})
        print(f"  - Checkpoint table: {restart_config.get('metadata_table_name', 'DB_SENTINEL_CHECKPOINTS')}")
        print(f"  - Checkpoint frequency: Every {restart_config.get('save_frequency', 10)} batches")


def _dry_run(config: Dict[str, Any]):
    """Perform a dry run to show what would be processed"""
    print("🧪 DB_Sentinel_util Dry Run")
    print("=" * 50)
    
    # Show database connections (without passwords)
    source_db = config['source_db'].copy()
    target_db = config['target_db'].copy()
    source_db['password'] = '***'
    target_db['password'] = '***'
    
    print(f"Source Database: {source_db['user']}@{source_db['dsn']}")
    print(f"Target Database: {target_db['user']}@{target_db['dsn']}")
    print()
    
    # Show tables that would be processed
    tables = config.get('tables', [])
    if not tables:
        print("❌ No tables configured for comparison")
        return
        
    print(f"Tables to be compared ({len(tables)}):")
    for i, table in enumerate(tables, 1):
        schema = table.get('schema') or config.get('schema', 'DEFAULT')
        print(f"  {i}. {schema}.{table['table_name']}")
        print(f"     - Primary Key: {table['primary_key']}")
        print(f"     - Chunk Size: {table.get('chunk_size', 10000):,}")
        
        if table.get('columns'):
            print(f"     - Columns: {len(table['columns'])} specified")
        else:
            print(f"     - Columns: All columns")
            
        if table.get('where_clause'):
            print(f"     - Filter: {table['where_clause']}")
        print()
        
    # Show output files
    paths = config.get('paths', {})
    print("Output Files:")
    print(f"  - Audit Log: {paths.get('audit_log', './logs/audit.log')}")
    print(f"  - Source SQL: {paths.get('source_sql_output', './output/source_sync.sql')}")
    print(f"  - Target SQL: {paths.get('target_sql_output', './output/target_sync.sql')}")
    print(f"  - Report: {paths.get('comparison_report', './output/comparison_report.txt')}")
    print()
    
    # Show features that would be used
    _show_features(config)
    
    print("\n⚠️  This was a dry run. No actual comparison was performed.")
    print("   Remove --dry-run to execute the comparison.")


if __name__ == "__main__":
    main()
