# Health Check and Service Management Improvements

## Overview
This document outlines the improvements made to address health check issues and add service management capabilities to the UWS Orchestrator.

## Issues Addressed

### 1. ✅ **Health Check for Bucket Containers Not Working**
**Problem**: Health checks only monitored nodes, not individual services like bucket containers.

**Solution**: 
- Created `service_health.py` module with comprehensive service health monitoring
- Implemented `check_service_health()` for individual service health verification
- Added `health_check_services()` for periodic monitoring of all service types
- Integrated service health check loop into the main orchestrator startup

### 2. ✅ **Automatic Restart Functionality**
**Problem**: No automatic restart when services go down.

**Solution**:
- Implemented `restart_failed_service()` function for automatic service recovery
- Added auto-restart logic in the service health check cycle
- Service restarts are attempted when health checks fail
- Proper logging and error handling for restart operations

### 3. ✅ **Missing Management Controls for Bucket Instances**
**Problem**: No way to create or remove bucket service instances from the UI.

**Solution**:
- Added `remove_bucket_service()` and similar functions for all service types
- Created DELETE endpoints for service removal: `/bucket-services/{service_id}`
- Updated API service with `removeBucketService()` method
- Enhanced UI with "Launch New Service" and "Remove" buttons
- Improved service status display with health indicators

## New Features Added

### 🔄 **Enhanced Service Health Monitoring**
- **File**: `service_health.py`
- **Functionality**: 
  - Monitors all service types (Bucket, DB, NoSQL, Queue, Secrets)
  - Checks service health every 30 seconds
  - Updates Prometheus metrics for service status
  - Automatic restart attempts for failed services

### 🚀 **Service Management Endpoints**
- **Endpoints Added**:
  - `DELETE /bucket-services/{service_id}` - Remove bucket service
  - `DELETE /db-services/{service_id}` - Remove database service
  - `DELETE /nosql-services/{service_id}` - Remove NoSQL service
  - `DELETE /queue-services/{service_id}` - Remove queue service
  - `DELETE /secrets-services/{service_id}` - Remove secrets service

### 📊 **Improved Metrics**
- **File**: `metrics.py` (consolidated metrics)
- **New Metrics**:
  - `ACTIVE_BUCKET_SERVICES` - Number of healthy bucket services
  - `ACTIVE_DB_SERVICES` - Number of healthy database services
  - `ACTIVE_NOSQL_SERVICES` - Number of healthy NoSQL services
  - `ACTIVE_QUEUE_SERVICES` - Number of healthy queue services
  - `ACTIVE_SECRETS_SERVICES` - Number of healthy secrets services

### 🎯 **Enhanced UI for Bucket Management**
- **File**: `uws/src/app/buckets/page.tsx`
- **Improvements**:
  - "Launch New Service" button for creating additional instances
  - "Remove" button for each service instance
  - Better status indicators (green/red dots for health)
  - Active service highlighting
  - Creation date display
  - Loading states for all operations

## Health Check Flow

```
1. Node Health Check (every 10 seconds)
   ├── Checks all registered nodes
   ├── Updates node health status
   └── Updates ACTIVE_NODES metric

2. Service Health Check (every 30 seconds)
   ├── Checks all service instances
   ├── Updates service health status
   ├── Attempts auto-restart for failed services
   └── Updates service-specific metrics
```

## Auto-Restart Logic

```
1. Service Health Check Fails
   ├── Mark service as unhealthy
   ├── Attempt container restart via node API
   ├── Update service status based on restart result
   └── Log all operations for monitoring
```

## Usage Examples

### Creating a New Bucket Service
```bash
# Via API
POST /launchBucket

# Via UI
Click "Launch New Service" button in Buckets page
```

### Removing a Bucket Service
```bash
# Via API
DELETE /bucket-services/{service_id}

# Via UI
Click "Remove" button next to service in Buckets page
```

### Monitoring Service Health
```bash
# Check specific service
GET /bucket-services/{service_id}/health

# View all services
GET /bucket-services
```

## Files Modified/Created

### New Files
- `UWS-orchestrator/service_health.py` - Service health monitoring system
- `UWS-orchestrator/metrics.py` - Consolidated Prometheus metrics

### Modified Files
- `UWS-orchestrator/orchestrator.py` - Added service health check loop and new endpoints
- `UWS-orchestrator/service_manager.py` - Added remove service endpoints
- `UWS-orchestrator/health.py` - Updated to use shared metrics
- `UWS-orchestrator/container_manager.py` - Updated to use shared metrics
- `UWS-orchestrator/service_launcher.py` - Updated to use shared metrics
- `UWS-orchestrator/websocket_proxy.py` - Updated to use shared metrics
- `uws/src/lib/api.ts` - Added removeBucketService API method
- `uws/src/app/buckets/page.tsx` - Enhanced UI with management controls

## Benefits

### 🔧 **Reliability**
- Services automatically restart when they fail
- Comprehensive health monitoring reduces downtime
- Early detection of service issues

### 📈 **Observability**
- Detailed metrics for all service types
- Health status tracking with timestamps
- Comprehensive logging for troubleshooting

### 👥 **Usability**
- Easy service management from the UI
- Clear visual indicators for service status
- Streamlined service creation and removal

### 🏗️ **Scalability**
- Support for multiple service instances
- Load balancing capabilities (foundation laid)
- Easy horizontal scaling

## Monitoring and Alerts

The new system provides metrics that can be used with monitoring tools:

```prometheus
# Example Prometheus queries
active_bucket_services         # Number of healthy bucket services
service_health_check_duration  # Time taken for health checks
service_restart_attempts_total # Number of restart attempts
```

## Next Steps

1. **Advanced Load Balancing**: Implement intelligent service selection
2. **Service Templates**: Pre-configured service deployment options
3. **Resource Monitoring**: CPU/Memory usage tracking per service
4. **Backup/Recovery**: Automated data backup for stateful services
5. **Multi-Node Deployment**: Service distribution across multiple nodes