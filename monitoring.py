"""
Monitoring Module for UWS Orchestrator

Handles real-time monitoring, metrics collection, alerts, and customizable dashboards.
Supports mathematical functions (MIN, MAX, SUM, AVG, p95) and automatic alerts.
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import structlog
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc
from database import get_db, Node, Container, MonitoringMetric, Alert, AlertRule

logger = structlog.get_logger()

class MetricType(Enum):
    CPU_USAGE = "cpu_usage"
    MEMORY_USAGE = "memory_usage"
    DISK_USAGE = "disk_usage"
    NETWORK_TRAFFIC = "network_traffic"
    REQUEST_COUNT = "request_count"
    RESPONSE_TIME = "response_time"
    ERROR_RATE = "error_rate"
    CONTAINER_COUNT = "container_count"
    ACTIVE_SERVICES = "active_services"

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

class AlertStatus(Enum):
    ACTIVE = "active"
    RESOLVED = "resolved"
    ACKNOWLEDGED = "acknowledged"

@dataclass
class MetricData:
    service_id: str
    metric_type: str
    value: float
    timestamp: datetime
    labels: Dict[str, str] = None

@dataclass
class AlertData:
    id: str
    name: str
    service_id: str
    severity: AlertSeverity
    status: AlertStatus
    message: str
    threshold: str
    current_value: float
    triggered_at: datetime
    resolved_at: Optional[datetime] = None

@dataclass
class DashboardWidget:
    id: str
    name: str
    type: str  # chart, metric, alert
    config: Dict[str, Any]
    position: Dict[str, int]  # x, y, width, height

@dataclass
class Dashboard:
    id: str
    name: str
    description: str
    widgets: List[DashboardWidget]
    created_at: datetime
    updated_at: datetime

class MonitoringService:
    def __init__(self):
        self.metrics_buffer: List[MetricData] = []
        self.active_alerts: Dict[str, AlertData] = {}
        self.alert_rules: Dict[str, Dict] = {}
        self.dashboards: Dict[str, Dashboard] = {}
        self.metric_processors: Dict[str, Callable] = {
            "MIN": self._min_processor,
            "MAX": self._max_processor,
            "SUM": self._sum_processor,
            "AVG": self._avg_processor,
            "P95": self._p95_processor,
            "COUNT": self._count_processor,
        }
        
    async def start_monitoring(self):
        """Start the monitoring service"""
        logger.info("Starting monitoring service")
        asyncio.create_task(self._metrics_collection_loop())
        asyncio.create_task(self._alert_check_loop())
        asyncio.create_task(self._metrics_aggregation_loop())
    
    async def _metrics_collection_loop(self):
        """Collect metrics from all services"""
        while True:
            try:
                await self._collect_service_metrics()
                await asyncio.sleep(30)  # Collect every 30 seconds
            except Exception as e:
                logger.error("Error in metrics collection loop", error=str(e))
                await asyncio.sleep(60)
    
    async def _collect_service_metrics(self):
        """Collect metrics from all active services"""
        db = next(get_db())
        try:
            # Collect container metrics
            containers = db.query(Container).filter(Container.status == "running").all()
            for container in containers:
                await self._collect_container_metrics(container)
            
            # Collect service-specific metrics
            await self._collect_bucket_metrics(db)
            await self._collect_db_metrics(db)
            await self._collect_nosql_metrics(db)
            await self._collect_queue_metrics(db)
            await self._collect_secrets_metrics(db)
            
        finally:
            db.close()
    
    async def _collect_container_metrics(self, container: Container):
        """Collect metrics for a specific container"""
        try:
            # Simulate container metrics collection
            metrics = [
                MetricData(
                    service_id=container.container_id,
                    metric_type=MetricType.CPU_USAGE.value,
                    value=float(time.time() % 100),  # Simulated CPU usage
                    timestamp=datetime.utcnow(),
                    labels={"container_id": container.container_id, "service": "container"}
                ),
                MetricData(
                    service_id=container.container_id,
                    metric_type=MetricType.MEMORY_USAGE.value,
                    value=float(time.time() % 80),  # Simulated memory usage
                    timestamp=datetime.utcnow(),
                    labels={"container_id": container.container_id, "service": "container"}
                )
            ]
            
            for metric in metrics:
                await self._store_metric(metric)
                
        except Exception as e:
            logger.error("Error collecting container metrics", 
                        container_id=container.container_id, error=str(e))
    
    async def _collect_bucket_metrics(self, db: Session):
        """Collect metrics for bucket services"""
        # Implementation for bucket service metrics
        pass
    
    async def _collect_db_metrics(self, db: Session):
        """Collect metrics for database services"""
        # Implementation for database service metrics
        pass
    
    async def _collect_nosql_metrics(self, db: Session):
        """Collect metrics for NoSQL services"""
        # Implementation for NoSQL service metrics
        pass
    
    async def _collect_queue_metrics(self, db: Session):
        """Collect metrics for queue services"""
        # Implementation for queue service metrics
        pass
    
    async def _collect_secrets_metrics(self, db: Session):
        """Collect metrics for secrets services"""
        # Implementation for secrets service metrics
        pass
    
    async def _store_metric(self, metric: MetricData):
        """Store a metric in the database"""
        db = next(get_db())
        try:
            db_metric = MonitoringMetric(
                service_id=metric.service_id,
                metric_type=metric.metric_type,
                value=metric.value,
                timestamp=metric.timestamp,
                labels=json.dumps(metric.labels or {})
            )
            db.add(db_metric)
            db.commit()
            
            # Add to buffer for real-time processing
            self.metrics_buffer.append(metric)
            
        except Exception as e:
            logger.error("Error storing metric", error=str(e))
            db.rollback()
        finally:
            db.close()
    
    async def _alert_check_loop(self):
        """Check for alert conditions"""
        while True:
            try:
                await self._check_alerts()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error("Error in alert check loop", error=str(e))
                await asyncio.sleep(120)
    
    async def _check_alerts(self):
        """Check all alert rules and trigger alerts if needed"""
        db = next(get_db())
        try:
            alert_rules = db.query(AlertRule).filter(AlertRule.is_active == True).all()
            
            for rule in alert_rules:
                await self._evaluate_alert_rule(rule)
                
        finally:
            db.close()
    
    async def _evaluate_alert_rule(self, rule: AlertRule):
        """Evaluate a single alert rule"""
        try:
            # Get recent metrics for the service
            db = next(get_db())
            recent_metrics = db.query(MonitoringMetric).filter(
                and_(
                    MonitoringMetric.service_id == rule.service_id,
                    MonitoringMetric.metric_type == rule.metric_type,
                    MonitoringMetric.timestamp >= datetime.utcnow() - timedelta(minutes=5)
                )
            ).order_by(desc(MonitoringMetric.timestamp)).limit(10).all()
            
            if not recent_metrics:
                return
            
            # Calculate current value based on aggregation function
            values = [m.value for m in recent_metrics]
            current_value = self._apply_aggregation_function(rule.aggregation_function, values)
            
            # Check if alert should be triggered
            should_trigger = self._evaluate_condition(
                current_value, rule.operator, rule.threshold_value
            )
            
            if should_trigger:
                await self._trigger_alert(rule, current_value)
            else:
                await self._resolve_alert(rule.id)
                
        except Exception as e:
            logger.error("Error evaluating alert rule", rule_id=rule.id, error=str(e))
        finally:
            db.close()
    
    def _apply_aggregation_function(self, func: str, values: List[float]) -> float:
        """Apply aggregation function to a list of values"""
        processor = self.metric_processors.get(func.upper())
        if processor:
            return processor(values)
        return values[-1] if values else 0.0
    
    def _evaluate_condition(self, value: float, operator: str, threshold: float) -> bool:
        """Evaluate if a condition is met"""
        if operator == ">":
            return value > threshold
        elif operator == ">=":
            return value >= threshold
        elif operator == "<":
            return value < threshold
        elif operator == "<=":
            return value <= threshold
        elif operator == "==":
            return value == threshold
        elif operator == "!=":
            return value != threshold
        return False
    
    async def _trigger_alert(self, rule: AlertRule, current_value: float):
        """Trigger a new alert"""
        alert_id = f"alert_{rule.id}_{int(time.time())}"
        
        alert_data = AlertData(
            id=alert_id,
            name=rule.name,
            service_id=rule.service_id,
            severity=AlertSeverity(rule.severity),
            status=AlertStatus.ACTIVE,
            message=f"{rule.metric_type} {rule.operator} {rule.threshold_value} (current: {current_value:.2f})",
            threshold=f"{rule.metric_type} {rule.operator} {rule.threshold_value}",
            current_value=current_value,
            triggered_at=datetime.utcnow()
        )
        
        # Store alert in database
        db = next(get_db())
        try:
            db_alert = Alert(
                alert_id=alert_id,
                rule_id=rule.id,
                service_id=rule.service_id,
                severity=rule.severity,
                status=AlertStatus.ACTIVE.value,
                message=alert_data.message,
                current_value=current_value,
                triggered_at=datetime.utcnow()
            )
            db.add(db_alert)
            db.commit()
            
            # Add to active alerts
            self.active_alerts[alert_id] = alert_data
            
            # Send notifications
            await self._send_alert_notifications(alert_data)
            
            logger.info("Alert triggered", alert_id=alert_id, rule_name=rule.name)
            
        except Exception as e:
            logger.error("Error triggering alert", error=str(e))
            db.rollback()
        finally:
            db.close()
    
    async def _resolve_alert(self, rule_id: str):
        """Resolve an alert when conditions return to normal"""
        db = next(get_db())
        try:
            active_alerts = db.query(Alert).filter(
                and_(
                    Alert.rule_id == rule_id,
                    Alert.status == AlertStatus.ACTIVE.value
                )
            ).all()
            
            for alert in active_alerts:
                alert.status = AlertStatus.RESOLVED.value
                alert.resolved_at = datetime.utcnow()
                
                # Remove from active alerts
                if alert.alert_id in self.active_alerts:
                    del self.active_alerts[alert.alert_id]
            
            db.commit()
            
        except Exception as e:
            logger.error("Error resolving alert", error=str(e))
            db.rollback()
        finally:
            db.close()
    
    async def _send_alert_notifications(self, alert: AlertData):
        """Send alert notifications via email, UI, etc."""
        # Email notification
        await self._send_email_alert(alert)
        
        # UI notification (WebSocket)
        await self._send_ui_alert(alert)
        
        # Log notification
        logger.warning("Alert notification sent", 
                      alert_id=alert.id, 
                      severity=alert.severity.value,
                      message=alert.message)
    
    async def _send_email_alert(self, alert: AlertData):
        """Send email alert notification"""
        # TODO: Implement email notification
        logger.info("Email alert would be sent", alert_id=alert.id)
    
    async def _send_ui_alert(self, alert: AlertData):
        """Send UI alert notification via WebSocket"""
        # TODO: Implement WebSocket notification
        logger.info("UI alert would be sent", alert_id=alert.id)
    
    async def _metrics_aggregation_loop(self):
        """Aggregate metrics for dashboards and historical data"""
        while True:
            try:
                await self._aggregate_metrics()
                await asyncio.sleep(300)  # Aggregate every 5 minutes
            except Exception as e:
                logger.error("Error in metrics aggregation loop", error=str(e))
                await asyncio.sleep(600)
    
    async def _aggregate_metrics(self):
        """Aggregate metrics for different time periods"""
        # Implementation for metrics aggregation
        pass
    
    # Aggregation function processors
    def _min_processor(self, values: List[float]) -> float:
        return min(values) if values else 0.0
    
    def _max_processor(self, values: List[float]) -> float:
        return max(values) if values else 0.0
    
    def _sum_processor(self, values: List[float]) -> float:
        return sum(values) if values else 0.0
    
    def _avg_processor(self, values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0
    
    def _p95_processor(self, values: List[float]) -> float:
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * 0.95)
        return sorted_values[index]
    
    def _count_processor(self, values: List[float]) -> float:
        return len(values)
    
    # Public API methods
    async def get_service_metrics(self, service_id: str, metric_type: str = None, 
                                 start_time: datetime = None, end_time: datetime = None) -> List[MetricData]:
        """Get metrics for a specific service"""
        db = next(get_db())
        try:
            query = db.query(MonitoringMetric).filter(MonitoringMetric.service_id == service_id)
            
            if metric_type:
                query = query.filter(MonitoringMetric.metric_type == metric_type)
            
            if start_time:
                query = query.filter(MonitoringMetric.timestamp >= start_time)
            
            if end_time:
                query = query.filter(MonitoringMetric.timestamp <= end_time)
            
            metrics = query.order_by(MonitoringMetric.timestamp).all()
            
            return [
                MetricData(
                    service_id=m.service_id,
                    metric_type=m.metric_type,
                    value=m.value,
                    timestamp=m.timestamp,
                    labels=json.loads(m.labels) if m.labels else {}
                )
                for m in metrics
            ]
            
        finally:
            db.close()
    
    async def get_active_alerts(self) -> List[AlertData]:
        """Get all active alerts"""
        return list(self.active_alerts.values())
    
    async def create_alert_rule(self, rule_data: Dict[str, Any]) -> str:
        """Create a new alert rule"""
        db = next(get_db())
        try:
            rule = AlertRule(
                name=rule_data["name"],
                service_id=rule_data["service_id"],
                metric_type=rule_data["metric_type"],
                operator=rule_data["operator"],
                threshold_value=rule_data["threshold_value"],
                aggregation_function=rule_data.get("aggregation_function", "AVG"),
                severity=rule_data["severity"],
                is_active=rule_data.get("is_active", True)
            )
            db.add(rule)
            db.commit()
            db.refresh(rule)
            
            logger.info("Alert rule created", rule_id=rule.id, name=rule.name)
            return str(rule.id)
            
        except Exception as e:
            logger.error("Error creating alert rule", error=str(e))
            db.rollback()
            raise
        finally:
            db.close()
    
    async def create_dashboard(self, dashboard_data: Dict[str, Any]) -> str:
        """Create a new dashboard"""
        dashboard_id = f"dashboard_{int(time.time())}"
        
        widgets = [
            DashboardWidget(
                id=w["id"],
                name=w["name"],
                type=w["type"],
                config=w["config"],
                position=w["position"]
            )
            for w in dashboard_data.get("widgets", [])
        ]
        
        dashboard = Dashboard(
            id=dashboard_id,
            name=dashboard_data["name"],
            description=dashboard_data.get("description", ""),
            widgets=widgets,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        self.dashboards[dashboard_id] = dashboard
        
        logger.info("Dashboard created", dashboard_id=dashboard_id, name=dashboard.name)
        return dashboard_id
    
    async def get_dashboard(self, dashboard_id: str) -> Optional[Dashboard]:
        """Get a specific dashboard"""
        return self.dashboards.get(dashboard_id)
    
    async def get_all_dashboards(self) -> List[Dashboard]:
        """Get all dashboards"""
        return list(self.dashboards.values())
    
    async def update_dashboard(self, dashboard_id: str, updates: Dict[str, Any]) -> bool:
        """Update a dashboard"""
        if dashboard_id not in self.dashboards:
            return False
        
        dashboard = self.dashboards[dashboard_id]
        
        if "name" in updates:
            dashboard.name = updates["name"]
        if "description" in updates:
            dashboard.description = updates["description"]
        if "widgets" in updates:
            dashboard.widgets = [
                DashboardWidget(
                    id=w["id"],
                    name=w["name"],
                    type=w["type"],
                    config=w["config"],
                    position=w["position"]
                )
                for w in updates["widgets"]
            ]
        
        dashboard.updated_at = datetime.utcnow()
        
        logger.info("Dashboard updated", dashboard_id=dashboard_id)
        return True
    
    async def delete_dashboard(self, dashboard_id: str) -> bool:
        """Delete a dashboard"""
        if dashboard_id in self.dashboards:
            del self.dashboards[dashboard_id]
            logger.info("Dashboard deleted", dashboard_id=dashboard_id)
            return True
        return False

# Global monitoring service instance
monitoring_service = MonitoringService() 