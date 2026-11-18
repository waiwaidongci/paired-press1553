from sqlalchemy import Column,Integer,String,JSON,Text,Float,ForeignKey,Index
from src.config.database import Base
class PairedRun(Base):
    __tablename__='runs'
    id=Column(Integer,primary_key=True)
    study_id=Column(Integer,ForeignKey('studies.id'),nullable=False)
    name=Column(String(120),nullable=False)
    status=Column(String(20),default='queued')
    parameters=Column(JSON,nullable=False)
    snapshot=Column(JSON,nullable=False)
    checksum=Column(String(64),nullable=False)
    result=Column(JSON,nullable=True)
    error=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
    # Idempotent acceptance: retries with the same key return the original run.
    request_key=Column(String(64),nullable=True)
    request_hash=Column(String(64),nullable=True)
    # Reliable execution: claim_seq is the fencing token, lease_until the epoch deadline.
    claim_seq=Column(Integer,nullable=False,default=0)
    claimed_by=Column(String(80),nullable=True)
    lease_until=Column(Float,nullable=True)
    finished_at=Column(String(40),nullable=True)
    __table_args__=(Index('runs_request_key_key','request_key',unique=True),)
Run=PairedRun
