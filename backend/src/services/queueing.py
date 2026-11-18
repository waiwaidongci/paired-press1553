# Reliable delivery on top of Redis. The database is the source of truth;
# Redis lists are only a wakeup mechanism, so lost messages are rebuilt from run rows.
import os,time,logging
from redis import Redis
from sqlalchemy import select,and_,or_
from src.config.database import Session
from src.models.run import Run

QUEUE='analysis-queue'            # pending run ids (duplicates tolerated)
PROCESSING='analysis-processing'  # ids popped by a worker, not yet settled
SWEEP_LOCK='analysis-sweep-lock'  # cheap leader election so one worker sweeps at a time

_redis=None
def get_redis():
    global _redis
    if _redis is None: _redis=Redis.from_url(os.environ['REDIS_URL'])
    return _redis

def enqueue(run_id,redis_client=None):
    (redis_client or get_redis()).rpush(QUEUE,run_id)

def sweep(redis_client=None,session_factory=Session):
    # Re-deliver runs whose message never arrived or whose executor went silent,
    # and prune processing entries that are already settled. Safe to run anytime,
    # from any worker: re-enqueue is idempotent because claiming is atomic in the DB.
    r=redis_client or get_redis()
    now=time.time()
    with session_factory() as db:
        stuck=list(db.scalars(select(Run.id).where(or_(Run.status=='queued',and_(Run.status=='running',or_(Run.lease_until.is_(None),Run.lease_until<now))))))
        queued={int(x) for x in r.lrange(QUEUE,0,-1)}
        active=set()
        for raw in r.lrange(PROCESSING,0,-1):
            rid=int(raw);run=db.get(Run,rid)
            if run is None or run.status in ('completed','failed') or rid in stuck: r.lrem(PROCESSING,1,raw)
            else: active.add(rid)
    delivered=0
    for rid in stuck:
        if rid not in queued and rid not in active:
            r.rpush(QUEUE,rid);delivered+=1
    if delivered: logging.info('sweeper redelivered %d run(s)',delivered)
    return delivered
