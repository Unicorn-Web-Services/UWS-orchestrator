from sqlalchemy import (
    create_engine,
    Column,
    String,
    DateTime,
    Boolean,
    Float,
    Text,
    ForeignKey,
    Integer,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./orchestrator.db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Node(Base):
    __tablename__ = "nodes"

    node_id = Column(String, primary_key=True, index=True)
    url = Column(String, nullable=False)
    is_healthy = Column(Boolean, default=True)
    last_health_check = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)
    registered_at = Column(DateTime, default=datetime.utcnow)

    # Relationship to containers
    containers = relationship("Container", back_populates="node")


class User(Base):
    __tablename__ = "users"

    user_id = Column(String, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationship to containers
    containers = relationship("Container", back_populates="user")


class Container(Base):
    __tablename__ = "containers"

    container_id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.user_id"), nullable=False)
    node_id = Column(String, ForeignKey("nodes.node_id"), nullable=False)
    image = Column(String, nullable=False)
    name = Column(String)
    env_vars = Column(Text)  # JSON string of environment variables
    cpu = Column(Float)
    memory = Column(String)
    ports = Column(Text)  # JSON string of port mappings
    status = Column(String, default="running")  # running, stopped, failed
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="containers")
    node = relationship("Node", back_populates="containers")


class BucketService(Base):
    __tablename__ = "bucket_services"

    service_id = Column(String, primary_key=True, index=True)
    container_id = Column(String, ForeignKey("containers.container_id"), nullable=False)
    node_id = Column(String, ForeignKey("nodes.node_id"), nullable=False)
    ip_address = Column(String, nullable=False)
    port = Column(Integer, default=8000)
    status = Column(String, default="starting")  # starting, running, stopped, failed
    created_at = Column(DateTime, default=datetime.utcnow)
    last_health_check = Column(DateTime, default=datetime.utcnow)
    is_healthy = Column(Boolean, default=False)

    # Relationships
    container = relationship("Container")


class DBService(Base):
    __tablename__ = "db_services"

    service_id = Column(String, primary_key=True, index=True)
    container_id = Column(String, ForeignKey("containers.container_id"), nullable=False)
    node_id = Column(String, ForeignKey("nodes.node_id"), nullable=False)
    ip_address = Column(String, nullable=False)
    port = Column(Integer, default=8010)
    status = Column(String, default="starting")  # starting, running, stopped, failed
    created_at = Column(DateTime, default=datetime.utcnow)
    last_health_check = Column(DateTime, default=datetime.utcnow)
    is_healthy = Column(Boolean, default=False)
    
    # Resource limits configuration
    max_cpu_percent = Column(Integer, default=90)
    max_ram_mb = Column(Integer, default=2048)
    max_disk_gb = Column(Integer, default=10)
    
    # Database configuration
    database_name = Column(String, default="main")
    instance_name = Column(String, nullable=True)

    # Relationships
    container = relationship("Container")


class NoSQLService(Base):
    __tablename__ = "nosql_services"

    service_id = Column(String, primary_key=True, index=True)
    container_id = Column(String, ForeignKey("containers.container_id"), nullable=False)
    node_id = Column(String, ForeignKey("nodes.node_id"), nullable=False)
    ip_address = Column(String, nullable=False)
    port = Column(Integer, default=8020)
    status = Column(String, default="starting")  # starting, running, stopped, failed
    created_at = Column(DateTime, default=datetime.utcnow)
    last_health_check = Column(DateTime, default=datetime.utcnow)
    is_healthy = Column(Boolean, default=False)

    # Relationships
    container = relationship("Container")


class QueueService(Base):
    __tablename__ = "queue_services"

    service_id = Column(String, primary_key=True, index=True)
    container_id = Column(String, ForeignKey("containers.container_id"), nullable=False)
    node_id = Column(String, ForeignKey("nodes.node_id"), nullable=False)
    ip_address = Column(String, nullable=False)
    port = Column(Integer, default=8030)
    status = Column(String, default="starting")  # starting, running, stopped, failed
    created_at = Column(DateTime, default=datetime.utcnow)
    last_health_check = Column(DateTime, default=datetime.utcnow)
    is_healthy = Column(Boolean, default=False)

    # Relationships
    container = relationship("Container")


class SecretsService(Base):
    __tablename__ = "secrets_services"

    service_id = Column(String, primary_key=True, index=True)
    container_id = Column(String, ForeignKey("containers.container_id"), nullable=False)
    node_id = Column(String, ForeignKey("nodes.node_id"), nullable=False)
    ip_address = Column(String, nullable=False)
    port = Column(Integer, default=8040)
    status = Column(String, default="starting")  # starting, running, stopped, failed
    created_at = Column(DateTime, default=datetime.utcnow)
    last_health_check = Column(DateTime, default=datetime.utcnow)
    is_healthy = Column(Boolean, default=False)

    # Relationships
    container = relationship("Container")


# Monitoring Models
class MonitoringMetric(Base):
    __tablename__ = "monitoring_metrics"

    id = Column(Integer, primary_key=True, index=True)
    service_id = Column(String, nullable=False, index=True)
    metric_type = Column(String, nullable=False)  # cpu_usage, memory_usage, etc.
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    labels = Column(Text)  # JSON string of labels


class AlertRule(Base):
    __tablename__ = "alert_rules"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    service_id = Column(String, nullable=False, index=True)
    metric_type = Column(String, nullable=False)
    operator = Column(String, nullable=False)  # >, <, >=, <=, ==, !=
    threshold_value = Column(Float, nullable=False)
    aggregation_function = Column(String, default="AVG")  # MIN, MAX, SUM, AVG, P95, COUNT
    severity = Column(String, default="warning")  # info, warning, critical
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Alert(Base):
    __tablename__ = "alerts"

    alert_id = Column(String, primary_key=True, index=True)
    rule_id = Column(Integer, ForeignKey("alert_rules.id"), nullable=False)
    service_id = Column(String, nullable=False, index=True)
    severity = Column(String, nullable=False)
    status = Column(String, default="active")  # active, resolved, acknowledged
    message = Column(Text, nullable=False)
    current_value = Column(Float, nullable=False)
    triggered_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)
    acknowledged_at = Column(DateTime, nullable=True)


# Billing Models
class BillingUsage(Base):
    __tablename__ = "billing_usage"

    id = Column(Integer, primary_key=True, index=True)
    service_id = Column(String, nullable=False, index=True)
    service_type = Column(String, nullable=False)  # compute, storage, database, etc.
    usage_amount = Column(Float, nullable=False)
    unit = Column(String, nullable=False)  # hours, requests, GB, etc.
    cost = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    extra_data = Column(Text)  # JSON string of additional data


class BillingInvoice(Base):
    __tablename__ = "billing_invoices"

    invoice_id = Column(String, primary_key=True, index=True)
    period = Column(String, nullable=False)  # monthly, daily, etc.
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    total_amount = Column(Float, nullable=False)
    status = Column(String, default="pending")  # pending, paid, overdue, cancelled
    due_date = Column(DateTime, nullable=False)
    usage_data = Column(Text)  # JSON string of usage breakdown
    created_at = Column(DateTime, default=datetime.utcnow)


class BillingAlert(Base):
    __tablename__ = "billing_alerts"

    alert_id = Column(String, primary_key=True, index=True)
    alert_type = Column(String, nullable=False)  # spending_limit, usage_spike, etc.
    period = Column(String, nullable=False)  # monthly, daily, hourly
    current_cost = Column(Float, nullable=False)
    limit_amount = Column(Float, nullable=False)
    message = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)


class ServiceCost(Base):
    __tablename__ = "service_costs"

    id = Column(Integer, primary_key=True, index=True)
    service_type = Column(String, nullable=False, unique=True)
    hourly_rate = Column(Float, nullable=False)
    request_rate = Column(Float, nullable=True)  # For per-request pricing
    storage_rate = Column(Float, nullable=True)  # For per-GB pricing
    unit = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# Create tables
Base.metadata.create_all(bind=engine)


def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
