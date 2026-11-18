# Claim -> compute -> archive -> publish, with leases and fencing so that
# concurrent workers never double-publish and a recovered executor can never
# overwrite a takeover result or its archive.
import os,json,time,logging,threading
from pathlib import Path
from sqlalchemy import update,and_,or_
from src.config.database import Session
from src.models.run import Run
from src.services.algorithm import calculate
from src.services.audits import log as audit
from src.utils.serializers import checksum,now

LEASE_SECONDS=float(os.environ.get('RUN_LEASE_SECONDS','30'))        # claim lifetime without heartbeat
HEARTBEAT_SECONDS=float(os.environ.get('RUN_HEARTBEAT_SECONDS','10'))# renewal cadence while computing

def try_claim(db,run_id,worker_id):
    # Atomic fencing: one UPDATE decides the owner. Claimable when queued, or when
    # a previous executor's lease expired (lost worker). Each claim bumps claim_seq,
    # which invalidates every older executor's writes.
    t=time.time()
    stmt=(update(Run).where(Run.id==run_id)
        .where(or_(Run.status=='queued',and_(Run.status=='running',or_(Run.lease_until.is_(None),Run.lease_until<t))))
        .values(status='running',claim_seq=Run.claim_seq+1,claimed_by=worker_id,lease_until=t+LEASE_SECONDS,error='')
        .returning(Run.claim_seq))
    seq=db.execute(stmt).scalar_one_or_none()
    if seq is None: db.rollback();return None
    audit(db,'worker','run.claim',f'{run_id} #{seq} by {worker_id}')
    db.commit()
    return seq

class Heartbeat:
    # Keeps a live long computation owning its task; stops renewing if the claim was taken over.
    def __init__(self,run_id,claim_seq,session_factory=Session,interval=HEARTBEAT_SECONDS):
        self.run_id,self.claim_seq,self.session_factory,self.interval=run_id,claim_seq,session_factory,interval
        self.lost=threading.Event();self._stop=threading.Event()
    def start(self):
        threading.Thread(target=self._run,daemon=True).start()
    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                with self.session_factory() as db:
                    res=db.execute(update(Run).where(Run.id==self.run_id,Run.claim_seq==self.claim_seq,Run.status=='running').values(lease_until=time.time()+LEASE_SECONDS))
                    db.commit()
                    if res.rowcount!=1: self.lost.set();return
            except Exception:
                logging.exception('heartbeat failed for run %s; will retry',self.run_id)
    def stop(self): self._stop.set()

def archive(artifacts_dir,run_id,claim_seq,payload):
    # Durable archive before publish: write temp, fsync, verify, then hard-link to the
    # final name without ever overwriting. An existing final file must be byte-identical
    # (deterministic recompute after a crash) or the claim refuses to touch it.
    artifacts_dir=Path(artifacts_dir);artifacts_dir.mkdir(parents=True,exist_ok=True)
    tmp=artifacts_dir/f'run-{run_id}.claim-{claim_seq}.tmp'
    final=artifacts_dir/f'run-{run_id}.json'
    try:
        with open(tmp,'wb') as f:
            f.write(payload);f.flush();os.fsync(f.fileno())
        if tmp.read_bytes()!=payload: raise IOError('归档暂存校验失败')
        try:
            os.link(tmp,final)
        except FileExistsError:
            if final.read_bytes()!=payload: raise IOError('归档已存在且内容不一致，拒绝覆盖')
        if final.read_bytes()!=payload: raise IOError('归档校验失败')
    finally:
        tmp.unlink(missing_ok=True)
    fd=os.open(artifacts_dir,os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)

def publish(db,run_id,claim_seq,result):
    # The atomic switch: result, status and audit become visible in one commit, and only
    # if this claim still owns the run. Anything before this commit is invisible to readers.
    res=db.execute(update(Run).where(Run.id==run_id,Run.claim_seq==claim_seq,Run.status=='running')
        .values(result=result,status='completed',error='',finished_at=now(),lease_until=None))
    if res.rowcount!=1: db.rollback();return False
    audit(db,'worker','run.completed',f'{run_id} #{claim_seq}')
    db.commit();return True

def fail(db,run_id,claim_seq,error):
    res=db.execute(update(Run).where(Run.id==run_id,Run.claim_seq==claim_seq,Run.status=='running')
        .values(status='failed',error=error[:500],lease_until=None))
    if res.rowcount!=1: db.rollback();return False
    audit(db,'worker','run.failed',f'{run_id} #{claim_seq}: {error[:200]}')
    db.commit();return True

def execute(run_id,worker_id,artifacts_dir,session_factory=Session):
    with session_factory() as db:
        seq=try_claim(db,run_id,worker_id)
        if seq is None: return 'skipped'
        run=db.get(Run,run_id)
        snapshot,params,expected=run.snapshot,run.parameters,run.checksum
    heartbeat=Heartbeat(run_id,seq,session_factory)
    heartbeat.start()
    try:
        if checksum(snapshot)!=expected: raise ValueError('输入快照校验失败')
        result=calculate(snapshot,params)
        result['input_sha256']=expected
        payload=json.dumps(result,ensure_ascii=False,allow_nan=False).encode('utf-8')
        archive(artifacts_dir,run_id,seq,payload)
        with session_factory() as db:
            if not publish(db,run_id,seq,result):
                logging.warning('run %s claim #%s lost; another executor owns the result',run_id,seq)
                return 'claim-lost'
        return 'completed'
    except Exception as e:
        logging.exception('analysis failed for run %s',run_id)
        with session_factory() as db: fail(db,run_id,seq,str(e))
        return 'failed'
    finally:
        heartbeat.stop()
