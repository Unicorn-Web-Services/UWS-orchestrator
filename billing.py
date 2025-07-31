"""
Billing Module for UWS Orchestrator

Handles usage tracking, cost calculation, billing history, and spending limits.
Supports hourly and per-API-call pricing models.
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import structlog
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func
from database import get_db, Node, Container, BillingUsage, BillingInvoice, BillingAlert, ServiceCost

logger = structlog.get_logger()

class BillingPeriod(Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"

class InvoiceStatus(Enum):
    PENDING = "pending"
    PAID = "paid"
    OVERDUE = "overdue"
    CANCELLED = "cancelled"

@dataclass
class UsageData:
    service_id: str
    service_type: str
    usage_amount: float
    unit: str  # hours, requests, GB, etc.
    cost: float
    timestamp: datetime
    extra_data: Dict[str, Any] = None

@dataclass
class CostBreakdown:
    service_type: str
    usage: float
    unit: str
    rate: float
    cost: float
    percentage: float

@dataclass
class BillingSummary:
    period: str
    total_cost: float
    breakdown: List[CostBreakdown]
    usage_summary: Dict[str, float]
    invoice_status: InvoiceStatus
    due_date: datetime

class BillingService:
    def __init__(self):
        self.pricing_rates = {
            "compute": {
                "hourly": 0.10,  # $0.10 per hour
                "unit": "hours"
            },
            "storage": {
                "gb_monthly": 0.05,  # $0.05 per GB per month
                "unit": "GB"
            },
            "database": {
                "hourly": 0.15,  # $0.15 per hour
                "unit": "hours"
            },
            "nosql": {
                "hourly": 0.12,  # $0.12 per hour
                "unit": "hours"
            },
            "queue": {
                "request": 0.0001,  # $0.0001 per request
                "unit": "requests"
            },
            "secrets": {
                "hourly": 0.08,  # $0.08 per hour
                "unit": "hours"
            },
            "lambda": {
                "execution": 0.0002,  # $0.0002 per execution
                "unit": "executions"
            }
        }
        self.spending_limits = {
            "monthly": 100.0,  # $100 monthly limit
            "daily": 5.0,      # $5 daily limit
            "hourly": 0.5      # $0.5 hourly limit
        }
        self.billing_alerts = {}
        
    async def start_billing_service(self):
        """Start the billing service"""
        logger.info("Starting billing service")
        asyncio.create_task(self._usage_tracking_loop())
        asyncio.create_task(self._billing_alert_check_loop())
        asyncio.create_task(self._invoice_generation_loop())
    
    async def _usage_tracking_loop(self):
        """Track usage for all services"""
        while True:
            try:
                await self._track_service_usage()
                await asyncio.sleep(300)  # Track every 5 minutes
            except Exception as e:
                logger.error("Error in usage tracking loop", error=str(e))
                await asyncio.sleep(600)
    
    async def _track_service_usage(self):
        """Track usage for all active services"""
        db = next(get_db())
        try:
            # Track container usage (compute)
            containers = db.query(Container).filter(Container.status == "running").all()
            for container in containers:
                await self._track_container_usage(container)
            
            # Track service-specific usage
            await self._track_bucket_usage(db)
            await self._track_db_usage(db)
            await self._track_nosql_usage(db)
            await self._track_queue_usage(db)
            await self._track_secrets_usage(db)
            
        finally:
            db.close()
    
    async def _track_container_usage(self, container: Container):
        """Track usage for a specific container"""
        try:
            # Calculate hours since container started
            created_at = container.created_at or datetime.utcnow()
            hours_running = (datetime.utcnow() - created_at).total_seconds() / 3600
            
            usage_data = UsageData(
                service_id=container.container_id,
                service_type="compute",
                usage_amount=hours_running,
                unit="hours",
                cost=hours_running * self.pricing_rates["compute"]["hourly"],
                timestamp=datetime.utcnow(),
                extra_data={
                    "container_id": container.container_id,
                    "image": container.image,
                    "node_id": container.node_id
                }
            )
            
            await self._store_usage(usage_data)
            
        except Exception as e:
            logger.error("Error tracking container usage", 
                        container_id=container.container_id, error=str(e))
    
    async def _track_bucket_usage(self, db: Session):
        """Track usage for bucket services"""
        # Implementation for bucket service usage tracking
        pass
    
    async def _track_db_usage(self, db: Session):
        """Track usage for database services"""
        # Implementation for database service usage tracking
        pass
    
    async def _track_nosql_usage(self, db: Session):
        """Track usage for NoSQL services"""
        # Implementation for NoSQL service usage tracking
        pass
    
    async def _track_queue_usage(self, db: Session):
        """Track usage for queue services"""
        # Implementation for queue service usage tracking
        pass
    
    async def _track_secrets_usage(self, db: Session):
        """Track usage for secrets services"""
        # Implementation for secrets service usage tracking
        pass
    
    async def _store_usage(self, usage_data: UsageData):
        """Store usage data in the database"""
        db = next(get_db())
        try:
            db_usage = BillingUsage(
                service_id=usage_data.service_id,
                service_type=usage_data.service_type,
                usage_amount=usage_data.usage_amount,
                unit=usage_data.unit,
                cost=usage_data.cost,
                timestamp=usage_data.timestamp,
                extra_data=json.dumps(usage_data.extra_data or {})
            )
            db.add(db_usage)
            db.commit()
            
            logger.debug("Usage stored", 
                        service_id=usage_data.service_id,
                        service_type=usage_data.service_type,
                        cost=usage_data.cost)
            
        except Exception as e:
            logger.error("Error storing usage", error=str(e))
            db.rollback()
        finally:
            db.close()
    
    async def _billing_alert_check_loop(self):
        """Check for billing alerts"""
        while True:
            try:
                await self._check_billing_alerts()
                await asyncio.sleep(3600)  # Check every hour
            except Exception as e:
                logger.error("Error in billing alert check loop", error=str(e))
                await asyncio.sleep(7200)
    
    async def _check_billing_alerts(self):
        """Check spending limits and trigger alerts"""
        try:
            # Check current period spending
            current_month_cost = await self._get_current_period_cost("monthly")
            current_day_cost = await self._get_current_period_cost("daily")
            current_hour_cost = await self._get_current_period_cost("hourly")
            
            # Check monthly limit
            if current_month_cost > self.spending_limits["monthly"]:
                await self._trigger_billing_alert("monthly", current_month_cost, self.spending_limits["monthly"])
            
            # Check daily limit
            if current_day_cost > self.spending_limits["daily"]:
                await self._trigger_billing_alert("daily", current_day_cost, self.spending_limits["daily"])
            
            # Check hourly limit
            if current_hour_cost > self.spending_limits["hourly"]:
                await self._trigger_billing_alert("hourly", current_hour_cost, self.spending_limits["hourly"])
                
        except Exception as e:
            logger.error("Error checking billing alerts", error=str(e))
    
    async def _get_current_period_cost(self, period: str) -> float:
        """Get current period cost"""
        db = next(get_db())
        try:
            if period == "monthly":
                start_date = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            elif period == "daily":
                start_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            elif period == "hourly":
                start_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            else:
                return 0.0
            
            result = db.query(func.sum(BillingUsage.cost)).filter(
                BillingUsage.timestamp >= start_date
            ).scalar()
            
            return float(result or 0.0)
            
        finally:
            db.close()
    
    async def _trigger_billing_alert(self, period: str, current_cost: float, limit: float):
        """Trigger a billing alert"""
        alert_id = f"billing_alert_{period}_{int(time.time())}"
        
        alert_data = {
            "id": alert_id,
            "type": "spending_limit",
            "period": period,
            "current_cost": current_cost,
            "limit": limit,
            "message": f"Spending limit exceeded for {period} period. Current: ${current_cost:.2f}, Limit: ${limit:.2f}",
            "timestamp": datetime.utcnow()
        }
        
        # Store alert in database
        db = next(get_db())
        try:
            db_alert = BillingAlert(
                alert_id=alert_id,
                alert_type="spending_limit",
                period=period,
                current_cost=current_cost,
                limit_amount=limit,
                message=alert_data["message"],
                timestamp=datetime.utcnow()
            )
            db.add(db_alert)
            db.commit()
            
            # Send notifications
            await self._send_billing_alert_notifications(alert_data)
            
            logger.warning("Billing alert triggered", 
                          alert_id=alert_id, 
                          period=period,
                          current_cost=current_cost,
                          limit=limit)
            
        except Exception as e:
            logger.error("Error triggering billing alert", error=str(e))
            db.rollback()
        finally:
            db.close()
    
    async def _send_billing_alert_notifications(self, alert_data: Dict[str, Any]):
        """Send billing alert notifications"""
        # Email notification
        await self._send_billing_email_alert(alert_data)
        
        # UI notification (WebSocket)
        await self._send_billing_ui_alert(alert_data)
        
        # Log notification
        logger.warning("Billing alert notification sent", 
                      alert_id=alert_data["id"],
                      message=alert_data["message"])
    
    async def _send_billing_email_alert(self, alert_data: Dict[str, Any]):
        """Send billing email alert notification"""
        # TODO: Implement email notification
        logger.info("Billing email alert would be sent", alert_id=alert_data["id"])
    
    async def _send_billing_ui_alert(self, alert_data: Dict[str, Any]):
        """Send billing UI alert notification via WebSocket"""
        # TODO: Implement WebSocket notification
        logger.info("Billing UI alert would be sent", alert_id=alert_data["id"])
    
    async def _invoice_generation_loop(self):
        """Generate invoices periodically"""
        while True:
            try:
                await self._generate_monthly_invoices()
                await asyncio.sleep(86400)  # Check daily
            except Exception as e:
                logger.error("Error in invoice generation loop", error=str(e))
                await asyncio.sleep(172800)
    
    async def _generate_monthly_invoices(self):
        """Generate monthly invoices"""
        # Check if it's the first day of the month
        now = datetime.utcnow()
        if now.day == 1 and now.hour == 0:
            await self._generate_invoice_for_period("monthly", now - timedelta(days=1))
    
    async def _generate_invoice_for_period(self, period: str, end_date: datetime):
        """Generate invoice for a specific period"""
        try:
            # Get usage for the period
            if period == "monthly":
                start_date = end_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            else:
                start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            
            usage_data = await self._get_usage_for_period(start_date, end_date)
            total_cost = sum(usage["cost"] for usage in usage_data)
            
            if total_cost > 0:
                # Generate invoice
                invoice_id = await self._create_invoice(period, start_date, end_date, total_cost, usage_data)
                logger.info("Invoice generated", invoice_id=invoice_id, period=period, total_cost=total_cost)
                
        except Exception as e:
            logger.error("Error generating invoice", period=period, error=str(e))
    
    async def _get_usage_for_period(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get usage data for a specific period"""
        db = next(get_db())
        try:
            usage_records = db.query(BillingUsage).filter(
                and_(
                    BillingUsage.timestamp >= start_date,
                    BillingUsage.timestamp <= end_date
                )
            ).all()
            
            return [
                {
                    "service_id": record.service_id,
                    "service_type": record.service_type,
                    "usage_amount": record.usage_amount,
                    "unit": record.unit,
                    "cost": record.cost,
                    "timestamp": record.timestamp.isoformat()
                }
                for record in usage_records
            ]
            
        finally:
            db.close()
    
    async def _create_invoice(self, period: str, start_date: datetime, end_date: datetime, 
                            total_cost: float, usage_data: List[Dict[str, Any]]) -> str:
        """Create an invoice in the database"""
        db = next(get_db())
        try:
            invoice_id = f"inv_{period}_{start_date.strftime('%Y%m')}_{int(time.time())}"
            
            invoice = BillingInvoice(
                invoice_id=invoice_id,
                period=period,
                start_date=start_date,
                end_date=end_date,
                total_amount=total_cost,
                status=InvoiceStatus.PENDING.value,
                due_date=end_date + timedelta(days=30),
                usage_data=json.dumps(usage_data)
            )
            db.add(invoice)
            db.commit()
            
            return invoice_id
            
        except Exception as e:
            logger.error("Error creating invoice", error=str(e))
            db.rollback()
            raise
        finally:
            db.close()
    
    # Public API methods
    async def get_current_usage(self) -> List[UsageData]:
        """Get current usage for all services"""
        db = next(get_db())
        try:
            # Get usage for the current month
            start_date = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            usage_records = db.query(BillingUsage).filter(
                BillingUsage.timestamp >= start_date
            ).order_by(BillingUsage.timestamp).all()
            
            return [
                UsageData(
                    service_id=record.service_id,
                    service_type=record.service_type,
                    usage_amount=record.usage_amount,
                    unit=record.unit,
                    cost=record.cost,
                    timestamp=record.timestamp,
                    extra_data=json.loads(record.extra_data) if record.extra_data else {}
                )
                for record in usage_records
            ]
            
        finally:
            db.close()
    
    async def get_cost_breakdown(self, period: str = "monthly") -> List[CostBreakdown]:
        """Get cost breakdown by service type"""
        db = next(get_db())
        try:
            if period == "monthly":
                start_date = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            elif period == "daily":
                start_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                start_date = datetime.utcnow() - timedelta(days=30)
            
            # Get aggregated usage by service type
            results = db.query(
                BillingUsage.service_type,
                func.sum(BillingUsage.usage_amount).label('total_usage'),
                func.sum(BillingUsage.cost).label('total_cost'),
                BillingUsage.unit
            ).filter(
                BillingUsage.timestamp >= start_date
            ).group_by(BillingUsage.service_type, BillingUsage.unit).all()
            
            total_cost = sum(result.total_cost for result in results)
            
            breakdown = []
            for result in results:
                percentage = (result.total_cost / total_cost * 100) if total_cost > 0 else 0
                breakdown.append(CostBreakdown(
                    service_type=result.service_type,
                    usage=result.total_usage,
                    unit=result.unit,
                    rate=self.pricing_rates.get(result.service_type, {}).get("hourly", 0),
                    cost=result.total_cost,
                    percentage=percentage
                ))
            
            return breakdown
            
        finally:
            db.close()
    
    async def get_billing_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get billing history (invoices)"""
        db = next(get_db())
        try:
            invoices = db.query(BillingInvoice).order_by(
                desc(BillingInvoice.end_date)
            ).limit(limit).all()
            
            return [
                {
                    "invoice_id": invoice.invoice_id,
                    "period": invoice.period,
                    "start_date": invoice.start_date.isoformat(),
                    "end_date": invoice.end_date.isoformat(),
                    "total_amount": invoice.total_amount,
                    "status": invoice.status,
                    "due_date": invoice.due_date.isoformat(),
                    "created_at": invoice.created_at.isoformat()
                }
                for invoice in invoices
            ]
            
        finally:
            db.close()
    
    async def get_invoice_details(self, invoice_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed invoice information"""
        db = next(get_db())
        try:
            invoice = db.query(BillingInvoice).filter(
                BillingInvoice.invoice_id == invoice_id
            ).first()
            
            if not invoice:
                return None
            
            return {
                "invoice_id": invoice.invoice_id,
                "period": invoice.period,
                "start_date": invoice.start_date.isoformat(),
                "end_date": invoice.end_date.isoformat(),
                "total_amount": invoice.total_amount,
                "status": invoice.status,
                "due_date": invoice.due_date.isoformat(),
                "usage_data": json.loads(invoice.usage_data) if invoice.usage_data else [],
                "created_at": invoice.created_at.isoformat()
            }
            
        finally:
            db.close()
    
    async def set_spending_limit(self, period: str, limit: float) -> bool:
        """Set spending limit for a period"""
        if period in ["monthly", "daily", "hourly"]:
            self.spending_limits[period] = limit
            logger.info("Spending limit updated", period=period, limit=limit)
            return True
        return False
    
    async def get_spending_limits(self) -> Dict[str, float]:
        """Get current spending limits"""
        return self.spending_limits.copy()
    
    async def get_current_period_cost(self, period: str = "monthly") -> float:
        """Get current period cost"""
        return await self._get_current_period_cost(period)
    
    async def get_cost_forecast(self) -> Dict[str, float]:
        """Get cost forecast for current month"""
        try:
            current_cost = await self._get_current_period_cost("monthly")
            days_in_month = 30  # Simplified
            current_day = datetime.utcnow().day
            
            if current_day > 0:
                daily_average = current_cost / current_day
                projected_cost = daily_average * days_in_month
            else:
                projected_cost = current_cost
            
            return {
                "current_cost": current_cost,
                "projected_cost": projected_cost,
                "daily_average": daily_average if current_day > 0 else 0,
                "remaining_days": days_in_month - current_day
            }
            
        except Exception as e:
            logger.error("Error calculating cost forecast", error=str(e))
            return {
                "current_cost": 0.0,
                "projected_cost": 0.0,
                "daily_average": 0.0,
                "remaining_days": 0
            }
    
    async def create_billing_alert(self, alert_data: Dict[str, Any]) -> str:
        """Create a custom billing alert"""
        db = next(get_db())
        try:
            alert_id = f"custom_alert_{int(time.time())}"
            
            db_alert = BillingAlert(
                alert_id=alert_id,
                alert_type=alert_data["type"],
                period=alert_data.get("period", "monthly"),
                current_cost=0.0,
                limit_amount=alert_data["limit_amount"],
                message=alert_data["message"],
                timestamp=datetime.utcnow()
            )
            db.add(db_alert)
            db.commit()
            
            logger.info("Billing alert created", alert_id=alert_id, type=alert_data["type"])
            return alert_id
            
        except Exception as e:
            logger.error("Error creating billing alert", error=str(e))
            db.rollback()
            raise
        finally:
            db.close()

# Global billing service instance
billing_service = BillingService() 