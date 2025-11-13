from celery import Celery
from neo4j_database import Neo4JDatabase

import logging
from db.postgres import SessionLocal
from db.postgres_models import SegmentTask, RootJob
from sqlalchemy import update, select
from datetime import datetime, timezone

app = Celery(
    'tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
)

logger = logging.getLogger(__name__)

@app.task(bind=True, name="process_segment_task")
def process_segment_task(self, job_id, manifestation_id: str, segment_id: str, start: int, end: int):
    logger.info(f"Processing segment {segment_id} for job {job_id}")

    has_completed = _check_if_task_completed(job_id=job_id)
    if has_completed:
        return {
            "job_id": job_id,
            "segment_id": segment_id,
            "status": "COMPLETED"
        }

    _update_root_job_status(job_id=job_id)

    _update_segment_task_record(
        job_id = job_id,
        segment_id = segment_id,
        status = "IN_PROGRESS"
    )

    db = Neo4JDatabase()
    try:
        
        related_segments = db._get_related_segments(
            manifestation_id = manifestation_id,
            start = start,
            end = end,
            transform = True
        )

        _store_related_segments_in_db(
            job_id = job_id,
            segment_id = segment_id,
            result_json = related_segments
        )

        _update_root_job_count(job_id=job_id)

        return {
            "job_id": job_id,
            "segment_id": segment_id,
            "related_segments": related_segments,
            "status": "COMPLETED"
        }
    
    except Exception as exc:
        _update_segment_task_record(
            job_id = job_id,
            segment_id = segment_id,
            status="RETRYING",
            error_message = str(exc)
        )
        raise self.retry(exc=exc, countdown=10, max_retries=3)
    finally:
        db.close()


def _update_segment_task_record(job_id, segment_id, status, error_message=None):
    with SessionLocal() as session:
        segment_task = session.query(SegmentTask).filter(SegmentTask.job_id == job_id, SegmentTask.segment_id == segment_id).update({
            "status": status,
            "error_message": error_message,
            "updated_at": datetime.now(timezone.utc)
        })
        session.commit()

def _store_related_segments_in_db(job_id, segment_id, result_json):
    with SessionLocal() as session:
        segment_task = session.query(SegmentTask).filter(SegmentTask.job_id == job_id, SegmentTask.segment_id == segment_id).first()
        segment_task.result_json = result_json
        session.commit()

def _update_root_job_status(job_id):
    with SessionLocal() as session:
        session.execute(
            update(RootJob)
            .where(RootJob.job_id == job_id)
            .values(status="IN_PROGRESS", updated_at=datetime.now(timezone.utc))
        )
        session.commit()

def _update_root_job_count(job_id):
    with SessionLocal() as session:
        # Increment and fetch in one transaction
        root = session.execute(
            update(RootJob)
            .where(RootJob.job_id == job_id)
            .values(
                completed_segments=RootJob.completed_segments + 1,
                updated_at=datetime.now(timezone.utc)
            )
            .returning(RootJob)
        ).scalar_one()
        
        # Check completion in same transaction
        if root.completed_segments >= root.total_segments:
            root.status = "COMPLETED"
            root.updated_at = datetime.now(timezone.utc)
        
        session.commit()

def _check_if_task_completed(job_id):
    with SessionLocal() as session:
        task = session.query(SegmentTask).filter(SegmentTask.job_id == job_id, SegmentTask.status == "COMPLETED").first()
        return task is not None