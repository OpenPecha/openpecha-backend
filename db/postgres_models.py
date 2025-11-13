from db.postgres import Base
from sqlalchemy import (
    Column,
    String,
    Integer,
    Text,
    JSON,
    ForeignKey,
    CheckConstraint,
    DateTime
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from uuid import uuid4


class RootJob(Base):
    __tablename__ = "root_jobs"

    job_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    manifestation_id = Column(Text, nullable=False)
    total_segments = Column(Integer, nullable=False)
    completed_segments = Column(Integer, nullable=False, default=0)
    status = Column(
        String(20),
        CheckConstraint(
            "status IN ('QUEUED','IN_PROGRESS','COMPLETED','FAILED')",
            name="segment_task_status_check"
        ),
        nullable=False
    )
    merged_result_location = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    segment_tasks = relationship(
        "SegmentTask",
        back_populates="root_job",
        cascade="all, delete-orphan"
    )

class SegmentTask(Base):
    __tablename__ = "segment_tasks"

    task_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("root_jobs.job_id"), nullable=False)
    segment_id = Column(Text, nullable=False)
    status = Column(
        String(20),
        CheckConstraint(
            "status IN ('QUEUED','IN_PROGRESS','COMPLETED','FAILED')",
            name="segment_task_status_check"
        ),
        nullable=False
    )
    result_json = Column(JSONB, nullable=True)
    result_location = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    root_job = relationship("RootJob", back_populates="segment_tasks")