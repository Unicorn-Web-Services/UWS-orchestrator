#!/usr/bin/env python3
"""
Database Migration Script
Adds missing columns to existing tables for enhanced DB service functionality
"""

import os
import sqlite3
import structlog
from sqlalchemy import create_engine, text
from database import DATABASE_URL, engine

logger = structlog.get_logger()

def check_column_exists(cursor, table_name, column_name):
    """Check if a column exists in a table"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns

def migrate_db_services_table():
    """Add missing columns to db_services table"""
    logger.info("Starting database migration for db_services table")
    
    # Connect directly to SQLite database
    if DATABASE_URL.startswith('sqlite:///'):
        db_path = DATABASE_URL.replace('sqlite:///', '')
        
        # Handle relative paths
        if not os.path.isabs(db_path):
            db_path = os.path.join(os.getcwd(), db_path)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='db_services'")
            if not cursor.fetchone():
                logger.info("db_services table does not exist, creating with new schema")
                # Let SQLAlchemy create the table with all columns
                from database import Base
                Base.metadata.create_all(bind=engine)
                logger.info("Database tables created successfully")
                return
            
            # List of columns to add with their definitions
            columns_to_add = [
                ("max_cpu_percent", "INTEGER DEFAULT 90"),
                ("max_ram_mb", "INTEGER DEFAULT 2048"), 
                ("max_disk_gb", "INTEGER DEFAULT 10"),
                ("database_name", "VARCHAR DEFAULT 'main'"),
                ("instance_name", "VARCHAR")
            ]
            
            migrations_applied = 0
            
            for column_name, column_def in columns_to_add:
                if not check_column_exists(cursor, "db_services", column_name):
                    logger.info(f"Adding column {column_name} to db_services table")
                    cursor.execute(f"ALTER TABLE db_services ADD COLUMN {column_name} {column_def}")
                    migrations_applied += 1
                else:
                    logger.info(f"Column {column_name} already exists, skipping")
            
            # Commit changes
            conn.commit()
            
            if migrations_applied > 0:
                logger.info(f"Migration completed: {migrations_applied} columns added to db_services table")
            else:
                logger.info("No migration needed: all columns already exist")
                
        except Exception as e:
            logger.error(f"Migration failed: {str(e)}")
            conn.rollback()
            raise
        finally:
            conn.close()
            
    else:
        # For non-SQLite databases, use SQLAlchemy
        logger.info("Using SQLAlchemy for non-SQLite database migration")
        try:
            with engine.connect() as conn:
                # Check if columns exist and add them if they don't
                columns_to_add = [
                    ("max_cpu_percent", "INTEGER DEFAULT 90"),
                    ("max_ram_mb", "INTEGER DEFAULT 2048"), 
                    ("max_disk_gb", "INTEGER DEFAULT 10"),
                    ("database_name", "VARCHAR(255) DEFAULT 'main'"),
                    ("instance_name", "VARCHAR(255)")
                ]
                
                for column_name, column_def in columns_to_add:
                    try:
                        # Try to add the column - this will fail if it already exists
                        conn.execute(text(f"ALTER TABLE db_services ADD COLUMN {column_name} {column_def}"))
                        logger.info(f"Added column {column_name} to db_services table")
                    except Exception as e:
                        if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                            logger.info(f"Column {column_name} already exists, skipping")
                        else:
                            raise e
                
                conn.commit()
                logger.info("Migration completed successfully")
                
        except Exception as e:
            logger.error(f"Migration failed: {str(e)}")
            raise

def migrate_other_service_tables():
    """Add resource limit columns to other service tables if needed"""
    service_tables = [
        "bucket_services",
        "nosql_services", 
        "queue_services",
        "secrets_services"
    ]
    
    if DATABASE_URL.startswith('sqlite:///'):
        db_path = DATABASE_URL.replace('sqlite:///', '')
        if not os.path.isabs(db_path):
            db_path = os.path.join(os.getcwd(), db_path)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            for table_name in service_tables:
                # Check if table exists
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                if cursor.fetchone():
                    # Add is_healthy column if it doesn't exist (for consistency)
                    if not check_column_exists(cursor, table_name, "is_healthy"):
                        logger.info(f"Adding is_healthy column to {table_name} table")
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN is_healthy BOOLEAN DEFAULT FALSE")
            
            conn.commit()
            logger.info("Other service tables migration completed")
            
        except Exception as e:
            logger.error(f"Other tables migration failed: {str(e)}")
            conn.rollback()
            raise
        finally:
            conn.close()

def main():
    """Run all migrations"""
    logger.info("Starting database migration")
    
    try:
        # Migrate db_services table
        migrate_db_services_table()
        
        # Migrate other service tables
        migrate_other_service_tables()
        
        logger.info("All database migrations completed successfully")
        
    except Exception as e:
        logger.error(f"Database migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()