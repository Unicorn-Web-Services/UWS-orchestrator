import httpx
import structlog
from datetime import datetime
from fastapi import HTTPException, Request, Depends, UploadFile, File
from fastapi.responses import Response
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from database import (
    get_db,
    BucketService,
    DBService,
    NoSQLService,
    QueueService,
    SecretsService,
    Container,
    Node,
)
from auth import get_auth_headers

logger = structlog.get_logger()

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)


# Bucket Service Management
@limiter.limit("30/minute")
async def list_bucket_services(request: Request, db: Session = Depends(get_db)):
    """List all bucket services"""
    try:
        bucket_services = db.query(BucketService).all()
        return {
            "bucket_services": [
                {
                    "service_id": service.service_id,
                    "container_id": service.container_id,
                    "node_id": service.node_id,
                    "ip_address": service.ip_address,
                    "port": service.port,
                    "status": service.status,
                    "is_healthy": service.is_healthy,
                    "created_at": service.created_at.isoformat(),
                    "service_url": f"http://{service.ip_address}:{service.port}",
                }
                for service in bucket_services
            ]
        }
    except Exception as e:
        logger.error("Failed to list bucket services", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to list bucket services: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_bucket_service(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get specific bucket service information"""
    try:
        bucket_service = (
            db.query(BucketService)
            .filter(BucketService.service_id == service_id)
            .first()
        )

        if not bucket_service:
            raise HTTPException(status_code=404, detail="Bucket service not found")

        return {
            "service_id": bucket_service.service_id,
            "container_id": bucket_service.container_id,
            "node_id": bucket_service.node_id,
            "ip_address": bucket_service.ip_address,
            "port": bucket_service.port,
            "status": bucket_service.status,
            "is_healthy": bucket_service.is_healthy,
            "created_at": bucket_service.created_at.isoformat(),
            "service_url": f"http://{bucket_service.ip_address}:{bucket_service.port}",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get bucket service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get bucket service: {str(e)}"
        )


@limiter.limit("30/minute")
async def list_bucket_files(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """List files in a bucket service"""
    try:
        bucket_service = (
            db.query(BucketService)
            .filter(BucketService.service_id == service_id)
            .first()
        )

        # Forward request to the bucket service
        service_url = f"http://{bucket_service.ip_address}:{bucket_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/data/files",
                timeout=30.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to list files from bucket service",
                    service_id=service_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to list files from bucket service",
                )

            return resp.json()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to list bucket files", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to list bucket files: {str(e)}"
        )


@limiter.limit("20/minute")
async def upload_to_bucket(
    service_id: str,
    file: UploadFile = File(...),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Upload file to bucket service"""
    try:
        bucket_service = (
            db.query(BucketService)
            .filter(BucketService.service_id == service_id)
            .first()
        )

        if not bucket_service:
            raise HTTPException(status_code=404, detail="Bucket service not found")

        if not bucket_service.is_healthy:
            raise HTTPException(status_code=503, detail="Bucket service is not healthy")

        # Forward request to the bucket service
        service_url = f"http://{bucket_service.ip_address}:{bucket_service.port}"
        async with httpx.AsyncClient() as client:
            files = {"file": (file.filename, file.file, file.content_type)}
            resp = await client.post(
                f"{service_url}/data/upload",
                files=files,
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to upload file to bucket service",
                    service_id=service_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to upload file to bucket service",
                )

            return resp.json()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to upload to bucket", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to upload to bucket: {str(e)}"
        )


@limiter.limit("30/minute")
async def download_from_bucket(
    service_id: str,
    filename: str,
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Download file from bucket service"""
    try:
        bucket_service = (
            db.query(BucketService)
            .filter(BucketService.service_id == service_id)
            .first()
        )

        if not bucket_service:
            raise HTTPException(status_code=404, detail="Bucket service not found")

        if not bucket_service.is_healthy:
            raise HTTPException(status_code=503, detail="Bucket service is not healthy")

        # Forward request to the bucket service
        service_url = f"http://{bucket_service.ip_address}:{bucket_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/data/download/{filename}",
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to download file from bucket service",
                    service_id=service_id,
                    filename=filename,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to download file from bucket service",
                )

            # Return the file as a response
            return Response(
                content=resp.content,
                media_type=resp.headers.get("content-type", "application/octet-stream"),
                headers={"Content-Disposition": f"attachment; filename={filename}"},
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to download from bucket", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to download from bucket: {str(e)}"
        )


@limiter.limit("20/minute")
async def delete_from_bucket(
    service_id: str,
    filename: str,
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Delete file from bucket service"""
    try:
        bucket_service = (
            db.query(BucketService)
            .filter(BucketService.service_id == service_id)
            .first()
        )

        if not bucket_service:
            raise HTTPException(status_code=404, detail="Bucket service not found")

        if not bucket_service.is_healthy:
            raise HTTPException(status_code=503, detail="Bucket service is not healthy")

        # Forward request to the bucket service
        service_url = f"http://{bucket_service.ip_address}:{bucket_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.delete(
                f"{service_url}/data/delete/{filename}",
                timeout=30.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to delete file from bucket service",
                    service_id=service_id,
                    filename=filename,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to delete file from bucket service",
                )

            return resp.json()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete from bucket", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to delete from bucket: {str(e)}"
        )


@limiter.limit("30/minute")
async def check_bucket_service_health(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Check health of bucket service"""
    try:
        bucket_service = (
            db.query(BucketService)
            .filter(BucketService.service_id == service_id)
            .first()
        )

        if not bucket_service:
            raise HTTPException(status_code=404, detail="Bucket service not found")

        # Check health by calling the bucket service health endpoint
        service_url = f"http://{bucket_service.ip_address}:{bucket_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/health",
                timeout=10.0,
            )

            is_healthy = resp.status_code == 200

            # Update health status in database
            bucket_service.is_healthy = is_healthy
            bucket_service.last_health_check = datetime.utcnow()
            db.commit()

            return {
                "service_id": service_id,
                "is_healthy": is_healthy,
                "last_check": bucket_service.last_health_check.isoformat(),
                "service_url": service_url,
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to check bucket service health", error=str(e))
        # Mark as unhealthy if we can't reach it
        bucket_service.is_healthy = False
        bucket_service.last_health_check = datetime.utcnow()
        db.commit()

        raise HTTPException(
            status_code=500, detail=f"Failed to check bucket service health: {str(e)}"
        )


# DB Service Management
@limiter.limit("30/minute")
async def list_db_services(request: Request, db: Session = Depends(get_db)):
    """List all DB services"""
    try:
        db_services = db.query(DBService).all()
        return {
            "db_services": [
                {
                    "service_id": service.service_id,
                    "container_id": service.container_id,
                    "node_id": service.node_id,
                    "ip_address": service.ip_address,
                    "port": service.port,
                    "status": service.status,
                    "is_healthy": service.is_healthy,
                    "created_at": service.created_at.isoformat(),
                    "service_url": f"http://{service.ip_address}:{service.port}",
                    "max_cpu_percent": service.max_cpu_percent,
                    "max_ram_mb": service.max_ram_mb,
                    "max_disk_gb": service.max_disk_gb,
                    "database_name": service.database_name,
                    "instance_name": service.instance_name,
                }
                for service in db_services
            ]
        }
    except Exception as e:
        logger.error("Failed to list DB services", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to list DB services: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_db_service(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get specific DB service information"""
    try:
        db_service = (
            db.query(DBService).filter(DBService.service_id == service_id).first()
        )

        if not db_service:
            raise HTTPException(status_code=404, detail="DB service not found")

        return {
            "service_id": db_service.service_id,
            "container_id": db_service.container_id,
            "node_id": db_service.node_id,
            "ip_address": db_service.ip_address,
            "port": db_service.port,
            "status": db_service.status,
            "is_healthy": db_service.is_healthy,
            "created_at": db_service.created_at.isoformat(),
            "service_url": f"http://{db_service.ip_address}:{db_service.port}",
            "max_cpu_percent": db_service.max_cpu_percent,
            "max_ram_mb": db_service.max_ram_mb,
            "max_disk_gb": db_service.max_disk_gb,
            "database_name": db_service.database_name,
            "instance_name": db_service.instance_name,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get DB service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get DB service: {str(e)}"
        )


@limiter.limit("30/minute")
async def check_db_service_health(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Check health of DB service"""
    try:
        db_service = (
            db.query(DBService).filter(DBService.service_id == service_id).first()
        )

        if not db_service:
            raise HTTPException(status_code=404, detail="DB service not found")

        # Check health by calling the DB service health endpoint
        service_url = f"http://{db_service.ip_address}:{db_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/health",
                timeout=10.0,
            )

            is_healthy = resp.status_code == 200

            # Update health status in database
            db_service.is_healthy = is_healthy
            db_service.last_health_check = datetime.utcnow()
            db.commit()

            return {
                "service_id": service_id,
                "is_healthy": is_healthy,
                "last_check": db_service.last_health_check.isoformat(),
                "service_url": service_url,
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to check DB service health", error=str(e))
        # Mark as unhealthy if we can't reach it
        db_service.is_healthy = False
        db_service.last_health_check = datetime.utcnow()
        db.commit()

        raise HTTPException(
            status_code=500, detail=f"Failed to check DB service health: {str(e)}"
        )


@limiter.limit("30/minute")
async def execute_db_sql_query(
    service_id: str,
    query_request: dict,
    request: Request,
    db: Session = Depends(get_db),
):
    """Execute SQL query on specific DB service"""
    try:
        db_service = (
            db.query(DBService).filter(DBService.service_id == service_id).first()
        )

        if not db_service:
            raise HTTPException(status_code=404, detail="DB service not found")

        if not db_service.is_healthy:
            raise HTTPException(status_code=503, detail="DB service is not healthy")

        # Forward SQL query to the DB service
        service_url = f"http://{db_service.ip_address}:{db_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{service_url}/sql/query",
                json=query_request,
                headers={"x-signature": "mysecretkey123"},
                timeout=30.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"SQL query failed: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "query_result": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to execute SQL query", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to execute SQL query: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_db_tables(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get list of tables from specific DB service"""
    try:
        db_service = (
            db.query(DBService).filter(DBService.service_id == service_id).first()
        )

        if not db_service:
            raise HTTPException(status_code=404, detail="DB service not found")

        if not db_service.is_healthy:
            raise HTTPException(status_code=503, detail="DB service is not healthy")

        # Get tables from the DB service
        service_url = f"http://{db_service.ip_address}:{db_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/sql/tables",
                headers={"x-signature": "mysecretkey123"},
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to get tables: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "tables": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get DB tables", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get DB tables: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_db_table_schema(
    service_id: str, table_name: str, request: Request, db: Session = Depends(get_db)
):
    """Get schema for a specific table from DB service"""
    try:
        db_service = (
            db.query(DBService).filter(DBService.service_id == service_id).first()
        )

        if not db_service:
            raise HTTPException(status_code=404, detail="DB service not found")

        if not db_service.is_healthy:
            raise HTTPException(status_code=503, detail="DB service is not healthy")

        # Get table schema from the DB service
        service_url = f"http://{db_service.ip_address}:{db_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/sql/schema/{table_name}",
                headers={"x-signature": "mysecretkey123"},
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to get table schema: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "schema": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get table schema", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get table schema: {str(e)}"
        )


@limiter.limit("30/minute")
async def update_db_service_config(
    service_id: str,
    config_update: dict,
    request: Request,
    db: Session = Depends(get_db),
):
    """Update DB service configuration (resource limits, etc.)"""
    try:
        db_service = (
            db.query(DBService).filter(DBService.service_id == service_id).first()
        )

        if not db_service:
            raise HTTPException(status_code=404, detail="DB service not found")

        # Update local database record
        if "max_cpu_percent" in config_update:
            db_service.max_cpu_percent = config_update["max_cpu_percent"]
        if "max_ram_mb" in config_update:
            db_service.max_ram_mb = config_update["max_ram_mb"]
        if "max_disk_gb" in config_update:
            db_service.max_disk_gb = config_update["max_disk_gb"]
        if "instance_name" in config_update:
            db_service.instance_name = config_update["instance_name"]

        db.commit()

        # Forward resource limits update to the DB service
        if any(
            key in config_update
            for key in ["max_cpu_percent", "max_ram_mb", "max_disk_gb"]
        ):
            resource_limits = {
                "max_cpu_percent": config_update.get(
                    "max_cpu_percent", db_service.max_cpu_percent
                ),
                "max_ram_mb": config_update.get("max_ram_mb", db_service.max_ram_mb),
                "max_disk_gb": config_update.get("max_disk_gb", db_service.max_disk_gb),
            }

            service_url = f"http://{db_service.ip_address}:{db_service.port}"
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{service_url}/config/resource-limits",
                    json=resource_limits,
                    headers={"x-signature": "mysecretkey123"},
                    timeout=10.0,
                )

        return {
            "service_id": service_id,
            "message": "Configuration updated successfully",
            "updated_config": {
                "max_cpu_percent": db_service.max_cpu_percent,
                "max_ram_mb": db_service.max_ram_mb,
                "max_disk_gb": db_service.max_disk_gb,
                "instance_name": db_service.instance_name,
            },
            "timestamp": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to update DB service config", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to update DB service config: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_db_service_stats(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get database statistics from specific DB service"""
    try:
        db_service = (
            db.query(DBService).filter(DBService.service_id == service_id).first()
        )

        if not db_service:
            raise HTTPException(status_code=404, detail="DB service not found")

        if not db_service.is_healthy:
            raise HTTPException(status_code=503, detail="DB service is not healthy")

        # Get statistics from the DB service
        service_url = f"http://{db_service.ip_address}:{db_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/stats",
                headers={"x-signature": "mysecretkey123"},
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to get statistics: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "statistics": result,
                "service_config": {
                    "max_cpu_percent": db_service.max_cpu_percent,
                    "max_ram_mb": db_service.max_ram_mb,
                    "max_disk_gb": db_service.max_disk_gb,
                    "instance_name": db_service.instance_name,
                    "database_name": db_service.database_name,
                },
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get DB service stats", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get DB service stats: {str(e)}"
        )


# NoSQL Service Management
@limiter.limit("30/minute")
async def list_nosql_services(request: Request, db: Session = Depends(get_db)):
    """List all NoSQL services"""
    try:
        nosql_services = db.query(NoSQLService).all()
        return {
            "nosql_services": [
                {
                    "service_id": service.service_id,
                    "container_id": service.container_id,
                    "node_id": service.node_id,
                    "ip_address": service.ip_address,
                    "port": service.port,
                    "status": service.status,
                    "is_healthy": service.is_healthy,
                    "created_at": service.created_at.isoformat(),
                    "service_url": f"http://{service.ip_address}:{service.port}",
                }
                for service in nosql_services
            ]
        }
    except Exception as e:
        logger.error("Failed to list NoSQL services", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to list NoSQL services: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_nosql_service(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get specific NoSQL service information"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        return {
            "service_id": nosql_service.service_id,
            "container_id": nosql_service.container_id,
            "node_id": nosql_service.node_id,
            "ip_address": nosql_service.ip_address,
            "port": nosql_service.port,
            "status": nosql_service.status,
            "is_healthy": nosql_service.is_healthy,
            "created_at": nosql_service.created_at.isoformat(),
            "service_url": f"http://{nosql_service.ip_address}:{nosql_service.port}",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get NoSQL service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get NoSQL service: {str(e)}"
        )


@limiter.limit("30/minute")
async def check_nosql_service_health(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Check health of NoSQL service"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        # Check health by calling the NoSQL service health endpoint
        service_url = f"http://{nosql_service.ip_address}:{nosql_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/health",
                timeout=10.0,
            )

            is_healthy = resp.status_code == 200

            # Update health status in database
            nosql_service.is_healthy = is_healthy
            nosql_service.last_health_check = datetime.utcnow()
            db.commit()

            return {
                "service_id": service_id,
                "is_healthy": is_healthy,
                "last_check": nosql_service.last_health_check.isoformat(),
                "service_url": service_url,
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to check NoSQL service health", error=str(e))
        # Mark as unhealthy if we can't reach it
        nosql_service.is_healthy = False
        nosql_service.last_health_check = datetime.utcnow()
        db.commit()

        raise HTTPException(
            status_code=500, detail=f"Failed to check NoSQL service health: {str(e)}"
        )


@limiter.limit("30/minute")
async def remove_nosql_service(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """Remove NoSQL service"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        # Delete from database
        db.delete(nosql_service)
        db.commit()

        return {"message": "NoSQL service removed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to remove NoSQL service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to remove NoSQL service: {str(e)}"
        )


# NoSQL Collection Management
@limiter.limit("30/minute")
async def get_nosql_collections(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get list of collections from specific NoSQL service"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        if not nosql_service.is_healthy:
            raise HTTPException(status_code=503, detail="NoSQL service is not healthy")

        # Get collections from the NoSQL service
        service_url = f"http://{nosql_service.ip_address}:{nosql_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/nosql/collections",
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to get collections: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "collections": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get NoSQL collections", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get NoSQL collections: {str(e)}"
        )


@limiter.limit("30/minute")
async def create_nosql_collection(
    service_id: str,
    collection_name: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Create collection in specific NoSQL service"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        if not nosql_service.is_healthy:
            raise HTTPException(status_code=503, detail="NoSQL service is not healthy")

        # Create collection in the NoSQL service
        service_url = f"http://{nosql_service.ip_address}:{nosql_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{service_url}/nosql/create_collection/{collection_name}",
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to create collection: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "collection_name": collection_name,
                "result": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to create NoSQL collection", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to create NoSQL collection: {str(e)}"
        )


@limiter.limit("30/minute")
async def save_nosql_entity(
    service_id: str,
    collection_name: str,
    entity_data: dict,
    request: Request,
    db: Session = Depends(get_db),
):
    """Save entity to specific NoSQL collection"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        if not nosql_service.is_healthy:
            raise HTTPException(status_code=503, detail="NoSQL service is not healthy")

        # Save entity to the NoSQL service
        service_url = f"http://{nosql_service.ip_address}:{nosql_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{service_url}/nosql/{collection_name}/save_json",
                json=entity_data,
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to save entity: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "collection_name": collection_name,
                "result": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to save NoSQL entity", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to save NoSQL entity: {str(e)}"
        )


@limiter.limit("30/minute")
async def query_nosql_collection(
    service_id: str,
    collection_name: str,
    field: str,
    value: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Query specific field in NoSQL collection"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        if not nosql_service.is_healthy:
            raise HTTPException(status_code=503, detail="NoSQL service is not healthy")

        # Query the NoSQL service
        service_url = f"http://{nosql_service.ip_address}:{nosql_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/nosql/{collection_name}/query",
                params={"field": field, "value": value},
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to query collection: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "collection_name": collection_name,
                "query_result": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to query NoSQL collection", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to query NoSQL collection: {str(e)}"
        )


@limiter.limit("30/minute")
async def scan_nosql_collection(
    service_id: str,
    collection_name: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Scan (list all documents) in NoSQL collection"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        if not nosql_service.is_healthy:
            raise HTTPException(status_code=503, detail="NoSQL service is not healthy")

        # Scan the NoSQL service
        service_url = f"http://{nosql_service.ip_address}:{nosql_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/nosql/{collection_name}/scan",
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to scan collection: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "collection_name": collection_name,
                "documents": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to scan NoSQL collection", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to scan NoSQL collection: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_nosql_entity(
    service_id: str,
    collection_name: str,
    entity_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Get specific entity from NoSQL collection"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        if not nosql_service.is_healthy:
            raise HTTPException(status_code=503, detail="NoSQL service is not healthy")

        # Get entity from the NoSQL service
        service_url = f"http://{nosql_service.ip_address}:{nosql_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/nosql/{collection_name}/get/{entity_id}",
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to get entity: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "collection_name": collection_name,
                "entity": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get NoSQL entity", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get NoSQL entity: {str(e)}"
        )


@limiter.limit("30/minute")
async def update_nosql_entity(
    service_id: str,
    collection_name: str,
    entity_id: str,
    update_data: dict,
    request: Request,
    db: Session = Depends(get_db),
):
    """Update entity in NoSQL collection"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        if not nosql_service.is_healthy:
            raise HTTPException(status_code=503, detail="NoSQL service is not healthy")

        # Update entity in the NoSQL service
        service_url = f"http://{nosql_service.ip_address}:{nosql_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{service_url}/nosql/{collection_name}/update/{entity_id}",
                json=update_data,
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to update entity: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "collection_name": collection_name,
                "entity_id": entity_id,
                "result": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to update NoSQL entity", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to update NoSQL entity: {str(e)}"
        )


@limiter.limit("30/minute")
async def delete_nosql_entity(
    service_id: str,
    collection_name: str,
    entity_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Delete entity from NoSQL collection"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        if not nosql_service.is_healthy:
            raise HTTPException(status_code=503, detail="NoSQL service is not healthy")

        # Delete entity from the NoSQL service
        service_url = f"http://{nosql_service.ip_address}:{nosql_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.delete(
                f"{service_url}/nosql/{collection_name}/delete/{entity_id}",
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to delete entity: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "collection_name": collection_name,
                "entity_id": entity_id,
                "result": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete NoSQL entity", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to delete NoSQL entity: {str(e)}"
        )


# Queue Service Management
@limiter.limit("30/minute")
async def list_queue_services(request: Request, db: Session = Depends(get_db)):
    """List all Queue services"""
    try:
        queue_services = db.query(QueueService).all()
        return {
            "queue_services": [
                {
                    "service_id": service.service_id,
                    "container_id": service.container_id,
                    "node_id": service.node_id,
                    "ip_address": service.ip_address,
                    "port": service.port,
                    "status": service.status,
                    "is_healthy": service.is_healthy,
                    "created_at": service.created_at.isoformat(),
                    "service_url": f"http://{service.ip_address}:{service.port}",
                }
                for service in queue_services
            ]
        }
    except Exception as e:
        logger.error("Failed to list Queue services", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to list Queue services: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_queue_service(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get specific Queue service information"""
    try:
        queue_service = (
            db.query(QueueService).filter(QueueService.service_id == service_id).first()
        )

        if not queue_service:
            raise HTTPException(status_code=404, detail="Queue service not found")

        return {
            "service_id": queue_service.service_id,
            "container_id": queue_service.container_id,
            "node_id": queue_service.node_id,
            "ip_address": queue_service.ip_address,
            "port": queue_service.port,
            "status": queue_service.status,
            "is_healthy": queue_service.is_healthy,
            "created_at": queue_service.created_at.isoformat(),
            "service_url": f"http://{queue_service.ip_address}:{queue_service.port}",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get Queue service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get Queue service: {str(e)}"
        )


@limiter.limit("30/minute")
async def check_queue_service_health(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Check health of Queue service"""
    try:
        queue_service = (
            db.query(QueueService).filter(QueueService.service_id == service_id).first()
        )

        if not queue_service:
            raise HTTPException(status_code=404, detail="Queue service not found")

        # Check health by calling the Queue service health endpoint
        service_url = f"http://{queue_service.ip_address}:{queue_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/health",
                timeout=10.0,
            )

            is_healthy = resp.status_code == 200

            # Update health status in database
            queue_service.is_healthy = is_healthy
            queue_service.last_health_check = datetime.utcnow()
            db.commit()

            return {
                "service_id": service_id,
                "is_healthy": is_healthy,
                "last_check": queue_service.last_health_check.isoformat(),
                "service_url": service_url,
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to check Queue service health", error=str(e))
        # Mark as unhealthy if we can't reach it
        queue_service.is_healthy = False
        queue_service.last_health_check = datetime.utcnow()
        db.commit()

        raise HTTPException(
            status_code=500, detail=f"Failed to check Queue service health: {str(e)}"
        )


# Secrets Service Management
@limiter.limit("30/minute")
async def list_secrets_services(request: Request, db: Session = Depends(get_db)):
    """List all Secrets services"""
    try:
        secrets_services = db.query(SecretsService).all()
        return {
            "secrets_services": [
                {
                    "service_id": service.service_id,
                    "container_id": service.container_id,
                    "node_id": service.node_id,
                    "ip_address": service.ip_address,
                    "port": service.port,
                    "status": service.status,
                    "is_healthy": service.is_healthy,
                    "created_at": service.created_at.isoformat(),
                    "service_url": f"http://{service.ip_address}:{service.port}",
                }
                for service in secrets_services
            ]
        }
    except Exception as e:
        logger.error("Failed to list Secrets services", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to list Secrets services: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_secrets_service(
    service_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get specific Secrets service information"""
    try:
        secrets_service = (
            db.query(SecretsService)
            .filter(SecretsService.service_id == service_id)
            .first()
        )

        if not secrets_service:
            raise HTTPException(status_code=404, detail="Secrets service not found")

        return {
            "service_id": secrets_service.service_id,
            "container_id": secrets_service.container_id,
            "node_id": secrets_service.node_id,
            "ip_address": secrets_service.ip_address,
            "port": secrets_service.port,
            "status": secrets_service.status,
            "is_healthy": secrets_service.is_healthy,
            "created_at": secrets_service.created_at.isoformat(),
            "service_url": f"http://{secrets_service.ip_address}:{secrets_service.port}",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get Secrets service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get Secrets service: {str(e)}"
        )


@limiter.limit("30/minute")
async def check_secrets_service_health(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Check health of Secrets service"""
    try:
        secrets_service = (
            db.query(SecretsService)
            .filter(SecretsService.service_id == service_id)
            .first()
        )

        if not secrets_service:
            raise HTTPException(status_code=404, detail="Secrets service not found")

        # Check health by calling the Secrets service health endpoint
        service_url = f"http://{secrets_service.ip_address}:{secrets_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/health",
                timeout=10.0,
            )

            is_healthy = resp.status_code == 200

            # Update health status in database
            secrets_service.is_healthy = is_healthy
            secrets_service.last_health_check = datetime.utcnow()
            db.commit()

            return {
                "service_id": service_id,
                "is_healthy": is_healthy,
                "last_check": secrets_service.last_health_check.isoformat(),
                "service_url": service_url,
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to check Secrets service health", error=str(e))
        # Mark as unhealthy if we can't reach it
        secrets_service.is_healthy = False
        secrets_service.last_health_check = datetime.utcnow()
        db.commit()

        raise HTTPException(
            status_code=500, detail=f"Failed to check Secrets service health: {str(e)}"
        )


# Service Management Endpoints (Create/Remove)
@limiter.limit("10/minute")
async def remove_bucket_service(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Remove a bucket service and its associated container"""
    try:
        bucket_service = (
            db.query(BucketService)
            .filter(BucketService.service_id == service_id)
            .first()
        )

        if not bucket_service:
            raise HTTPException(status_code=404, detail="Bucket service not found")

        # Get associated container and node
        container = (
            db.query(Container)
            .filter(Container.container_id == bucket_service.container_id)
            .first()
        )
        if not container:
            logger.warning(f"Container not found for bucket service {service_id}")

        node = None
        if container:
            node = db.query(Node).filter(Node.node_id == container.node_id).first()

        # Try to stop and remove the container
        if node and node.is_healthy and container:
            try:
                async with httpx.AsyncClient() as client:
                    # Stop the container
                    stop_resp = await client.post(
                        f"{node.url}/containers/{container.container_id}/stop",
                        headers=get_auth_headers(),
                        timeout=30.0,
                    )

                    # Remove the container (if the node supports it)
                    remove_resp = await client.delete(
                        f"{node.url}/containers/{container.container_id}",
                        headers=get_auth_headers(),
                        timeout=30.0,
                    )

                    logger.info(
                        f"Container operations completed for bucket service {service_id}",
                        stop_status=stop_resp.status_code,
                        remove_status=remove_resp.status_code,
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to clean up container for bucket service {service_id}",
                    error=str(e),
                )

        # Remove from database
        if container:
            db.delete(container)
        db.delete(bucket_service)
        db.commit()

        logger.info(f"Bucket service removed", service_id=service_id)
        return {"message": f"Bucket service {service_id} removed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to remove bucket service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to remove bucket service: {str(e)}"
        )


@limiter.limit("10/minute")
async def remove_db_service(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Remove a database service and its associated container"""
    try:
        db_service = (
            db.query(DBService).filter(DBService.service_id == service_id).first()
        )

        if not db_service:
            raise HTTPException(status_code=404, detail="Database service not found")

        # Get associated container and node
        container = (
            db.query(Container)
            .filter(Container.container_id == db_service.container_id)
            .first()
        )
        node = None
        if container:
            node = db.query(Node).filter(Node.node_id == container.node_id).first()

        # Try to stop and remove the container
        if node and node.is_healthy and container:
            try:
                async with httpx.AsyncClient() as client:
                    # Stop the container
                    stop_resp = await client.post(
                        f"{node.url}/containers/{container.container_id}/stop",
                        headers=get_auth_headers(),
                        timeout=30.0,
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to clean up container for DB service {service_id}",
                    error=str(e),
                )

        # Remove from database
        if container:
            db.delete(container)
        db.delete(db_service)
        db.commit()

        logger.info(f"Database service removed", service_id=service_id)
        return {"message": f"Database service {service_id} removed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to remove database service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to remove database service: {str(e)}"
        )


@limiter.limit("10/minute")
async def remove_nosql_service(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Remove a NoSQL service and its associated container"""
    try:
        nosql_service = (
            db.query(NoSQLService).filter(NoSQLService.service_id == service_id).first()
        )

        if not nosql_service:
            raise HTTPException(status_code=404, detail="NoSQL service not found")

        # Get associated container and node
        container = (
            db.query(Container)
            .filter(Container.container_id == nosql_service.container_id)
            .first()
        )
        node = None
        if container:
            node = db.query(Node).filter(Node.node_id == container.node_id).first()

        # Try to stop and remove the container
        if node and node.is_healthy and container:
            try:
                async with httpx.AsyncClient() as client:
                    stop_resp = await client.post(
                        f"{node.url}/containers/{container.container_id}/stop",
                        headers=get_auth_headers(),
                        timeout=30.0,
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to clean up container for NoSQL service {service_id}",
                    error=str(e),
                )

        # Remove from database
        if container:
            db.delete(container)
        db.delete(nosql_service)
        db.commit()

        logger.info(f"NoSQL service removed", service_id=service_id)
        return {"message": f"NoSQL service {service_id} removed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to remove NoSQL service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to remove NoSQL service: {str(e)}"
        )


@limiter.limit("10/minute")
async def remove_queue_service(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Remove a queue service and its associated container"""
    try:
        queue_service = (
            db.query(QueueService).filter(QueueService.service_id == service_id).first()
        )

        if not queue_service:
            raise HTTPException(status_code=404, detail="Queue service not found")

        # Get associated container and node
        container = (
            db.query(Container)
            .filter(Container.container_id == queue_service.container_id)
            .first()
        )
        node = None
        if container:
            node = db.query(Node).filter(Node.node_id == container.node_id).first()

        # Try to stop and remove the container
        if node and node.is_healthy and container:
            try:
                async with httpx.AsyncClient() as client:
                    stop_resp = await client.post(
                        f"{node.url}/containers/{container.container_id}/stop",
                        headers=get_auth_headers(),
                        timeout=30.0,
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to clean up container for Queue service {service_id}",
                    error=str(e),
                )

        # Remove from database
        if container:
            db.delete(container)
        db.delete(queue_service)
        db.commit()

        logger.info(f"Queue service removed", service_id=service_id)
        return {"message": f"Queue service {service_id} removed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to remove queue service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to remove queue service: {str(e)}"
        )


@limiter.limit("10/minute")
async def remove_secrets_service(
    service_id: str, request: Request = None, db: Session = Depends(get_db)
):
    """Remove a secrets service and its associated container"""
    try:
        secrets_service = (
            db.query(SecretsService)
            .filter(SecretsService.service_id == service_id)
            .first()
        )

        if not secrets_service:
            raise HTTPException(status_code=404, detail="Secrets service not found")

        # Get associated container and node
        container = (
            db.query(Container)
            .filter(Container.container_id == secrets_service.container_id)
            .first()
        )
        node = None
        if container:
            node = db.query(Node).filter(Node.node_id == container.node_id).first()

        # Try to stop and remove the container
        if node and node.is_healthy and container:
            try:
                async with httpx.AsyncClient() as client:
                    stop_resp = await client.post(
                        f"{node.url}/containers/{container.container_id}/stop",
                        headers=get_auth_headers(),
                        timeout=30.0,
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to clean up container for Secrets service {service_id}",
                    error=str(e),
                )

        # Remove from database
        if container:
            db.delete(container)
        db.delete(secrets_service)
        db.commit()

        logger.info(f"Secrets service removed", service_id=service_id)
        return {"message": f"Secrets service {service_id} removed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to remove secrets service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to remove secrets service: {str(e)}"
        )


# Queue Operations
@limiter.limit("30/minute")
async def add_queue_message(
    service_id: str,
    message_data: dict,
    request: Request,
    db: Session = Depends(get_db),
):
    """Add a message to the queue"""
    try:
        queue_service = (
            db.query(QueueService).filter(QueueService.service_id == service_id).first()
        )

        if not queue_service:
            raise HTTPException(status_code=404, detail="Queue service not found")

        if not queue_service.is_healthy:
            raise HTTPException(status_code=503, detail="Queue service is not healthy")

        # Add message to the queue service
        service_url = f"http://{queue_service.ip_address}:{queue_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{service_url}/queue/add",
                json=message_data,
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to add message: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "message_id": result.get("id"),
                "message": result.get("message"),
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to add queue message", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to add queue message: {str(e)}"
        )


@limiter.limit("30/minute")
async def read_queue_messages(
    service_id: str,
    limit: int = 10,
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Read messages from the queue"""
    try:
        queue_service = (
            db.query(QueueService).filter(QueueService.service_id == service_id).first()
        )

        if not queue_service:
            raise HTTPException(status_code=404, detail="Queue service not found")

        if not queue_service.is_healthy:
            raise HTTPException(status_code=503, detail="Queue service is not healthy")

        # Read messages from the queue service
        service_url = f"http://{queue_service.ip_address}:{queue_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/queue/read?limit={limit}",
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to read messages: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "messages": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to read queue messages", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to read queue messages: {str(e)}"
        )


@limiter.limit("30/minute")
async def delete_queue_message(
    service_id: str,
    message_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Delete a message from the queue"""
    try:
        queue_service = (
            db.query(QueueService).filter(QueueService.service_id == service_id).first()
        )

        if not queue_service:
            raise HTTPException(status_code=404, detail="Queue service not found")

        if not queue_service.is_healthy:
            raise HTTPException(status_code=503, detail="Queue service is not healthy")

        # Delete message from the queue service
        service_url = f"http://{queue_service.ip_address}:{queue_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.delete(
                f"{service_url}/queue/{message_id}",
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to delete message: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "message_id": message_id,
                "deleted": result.get("deleted", False),
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete queue message", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to delete queue message: {str(e)}"
        )


# Secrets Operations
@limiter.limit("30/minute")
async def create_secret(
    service_id: str,
    secret_data: dict,
    request: Request,
    db: Session = Depends(get_db),
):
    """Create a new secret"""
    try:
        secrets_service = (
            db.query(SecretsService)
            .filter(SecretsService.service_id == service_id)
            .first()
        )

        if not secrets_service:
            raise HTTPException(status_code=404, detail="Secrets service not found")

        if not secrets_service.is_healthy:
            raise HTTPException(
                status_code=503, detail="Secrets service is not healthy"
            )

        # Create secret in the secrets service
        service_url = f"http://{secrets_service.ip_address}:{secrets_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{service_url}/secrets/store",
                json=secret_data,
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to create secret: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "secret_name": secret_data.get("name"),
                "created": True,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to create secret", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to create secret: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_secret(
    service_id: str,
    secret_name: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Get a secret by name"""
    try:
        secrets_service = (
            db.query(SecretsService)
            .filter(SecretsService.service_id == service_id)
            .first()
        )

        if not secrets_service:
            raise HTTPException(status_code=404, detail="Secrets service not found")

        if not secrets_service.is_healthy:
            raise HTTPException(
                status_code=503, detail="Secrets service is not healthy"
            )

        # Get secret from the secrets service
        service_url = f"http://{secrets_service.ip_address}:{secrets_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/secrets/{secret_name}",
                timeout=10.0,
            )

            if resp.status_code == 404:
                return {
                    "service_id": service_id,
                    "secret": None,
                    "timestamp": datetime.utcnow().isoformat(),
                }

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to get secret: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "secret": result,
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get secret", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get secret: {str(e)}")


@limiter.limit("30/minute")
async def list_secrets(
    service_id: str,
    request: Request = None,
    db: Session = Depends(get_db),
):
    """List all secrets"""
    try:
        secrets_service = (
            db.query(SecretsService)
            .filter(SecretsService.service_id == service_id)
            .first()
        )

        if not secrets_service:
            raise HTTPException(status_code=404, detail="Secrets service not found")

        if not secrets_service.is_healthy:
            raise HTTPException(
                status_code=503, detail="Secrets service is not healthy"
            )

        # List secrets from the secrets service
        service_url = f"http://{secrets_service.ip_address}:{secrets_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{service_url}/secrets",
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to list secrets: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "secrets": result.get("secrets", []),
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to list secrets", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to list secrets: {str(e)}")


@limiter.limit("30/minute")
async def delete_secret(
    service_id: str,
    secret_name: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Delete a secret by name"""
    try:
        secrets_service = (
            db.query(SecretsService)
            .filter(SecretsService.service_id == service_id)
            .first()
        )

        if not secrets_service:
            raise HTTPException(status_code=404, detail="Secrets service not found")

        if not secrets_service.is_healthy:
            raise HTTPException(
                status_code=503, detail="Secrets service is not healthy"
            )

        # Delete secret from the secrets service
        service_url = f"http://{secrets_service.ip_address}:{secrets_service.port}"
        async with httpx.AsyncClient() as client:
            resp = await client.delete(
                f"{service_url}/secrets/{secret_name}",
                timeout=10.0,
            )

            if resp.status_code != 200:
                raise HTTPException(
                    status_code=resp.status_code,
                    detail=f"Failed to delete secret: {resp.text}",
                )

            result = resp.json()
            return {
                "service_id": service_id,
                "secret_name": secret_name,
                "deleted": result.get("deleted", False),
                "timestamp": datetime.utcnow().isoformat(),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete secret", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to delete secret: {str(e)}"
        )
