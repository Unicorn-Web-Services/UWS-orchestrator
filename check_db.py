#!/usr/bin/env python3
"""
Check and update existing database services with default values
"""

import sqlite3
import os
import structlog
from database import DATABASE_URL

logger = structlog.get_logger()

def check_and_update_existing_services():
    """Check existing services and update them with default values if needed"""
    
    if DATABASE_URL.startswith('sqlite:///'):
        db_path = DATABASE_URL.replace('sqlite:///', '')
        if not os.path.isabs(db_path):
            db_path = os.path.join(os.getcwd(), db_path)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # Check if there are any existing services
            cursor.execute("SELECT COUNT(*) FROM db_services")
            count = cursor.fetchone()[0]
            
            if count == 0:
                logger.info("No existing db_services found")
                return
            
            logger.info(f"Found {count} existing db_services")
            
            # Get current services to see their state
            cursor.execute("""
                SELECT service_id, max_cpu_percent, max_ram_mb, max_disk_gb, 
                       database_name, instance_name, status, is_healthy 
                FROM db_services
            """)
            
            services = cursor.fetchall()
            
            for service in services:
                service_id, cpu, ram, disk, db_name, instance_name, status, healthy = service
                logger.info(f"Service {service_id}: CPU={cpu}, RAM={ram}, Disk={disk}, DB={db_name}, Name={instance_name}, Status={status}, Healthy={healthy}")
            
            # Update any services with NULL values to defaults
            update_queries = [
                "UPDATE db_services SET max_cpu_percent = 90 WHERE max_cpu_percent IS NULL",
                "UPDATE db_services SET max_ram_mb = 2048 WHERE max_ram_mb IS NULL", 
                "UPDATE db_services SET max_disk_gb = 10 WHERE max_disk_gb IS NULL",
                "UPDATE db_services SET database_name = 'main' WHERE database_name IS NULL"
            ]
            
            updates_made = 0
            for query in update_queries:
                result = cursor.execute(query)
                if result.rowcount > 0:
                    logger.info(f"Updated {result.rowcount} services with query: {query}")
                    updates_made += result.rowcount
            
            if updates_made > 0:
                conn.commit()
                logger.info(f"Updated {updates_made} service records with default values")
            else:
                logger.info("All services already have proper values")
                
            # Show final state
            logger.info("Final state of db_services:")
            cursor.execute("""
                SELECT service_id, max_cpu_percent, max_ram_mb, max_disk_gb, 
                       database_name, instance_name, status, is_healthy 
                FROM db_services
            """)
            
            services = cursor.fetchall()
            for service in services:
                service_id, cpu, ram, disk, db_name, instance_name, status, healthy = service
                logger.info(f"Service {service_id}: CPU={cpu}, RAM={ram}, Disk={disk}, DB={db_name}, Name={instance_name}")
                
        except Exception as e:
            logger.error(f"Error checking database: {str(e)}")
            raise
        finally:
            conn.close()

if __name__ == "__main__":
    check_and_update_existing_services()