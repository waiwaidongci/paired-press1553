# Worker: pops run ids from a reliable Redis queue and executes them with
# DB-fenced claims. Any number of workers may run concurrently; the sweeper
# re-delivers queued or silently-expired runs so nothing is shelved forever.
import os,time,uuid,socket,logging
from pathlib import Path
from src.config.database import migrate
from src.services import execution,queueing

logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
log=logging.getLogger('worker')
WORKER_ID=f'{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}'
ARTIFACTS=Path(os.environ.get('ARTIFACT_DIR','/app/artifacts'))
SWEEP_INTERVAL=float(os.environ.get('SWEEP_INTERVAL_SECONDS','10'))

def main():
    migrate()
    ARTIFACTS.mkdir(parents=True,exist_ok=True)
    redis=queueing.get_redis()
    log.info('worker %s started',WORKER_ID)
    last_sweep=time.monotonic()-SWEEP_INTERVAL  # sweep immediately at startup: adopt pre-existing queued/expired runs
    while True:
        Path('/tmp/worker-heartbeat').touch()
        now=time.monotonic()
        if now-last_sweep>=SWEEP_INTERVAL:
            last_sweep=now
            try:
                if redis.set(queueing.SWEEP_LOCK,WORKER_ID,nx=True,ex=max(5,int(SWEEP_INTERVAL))):
                    queueing.sweep(redis)
            except Exception: log.exception('sweep failed')
        try:
            msg=redis.blmove(queueing.QUEUE,queueing.PROCESSING,timeout=2,src='LEFT',dest='LEFT')
        except Exception:
            log.exception('queue read failed; retrying');time.sleep(2);continue
        if msg is None: continue
        try:
            outcome=execution.execute(int(msg),WORKER_ID,ARTIFACTS)
            log.info('run %s -> %s',msg,outcome)
        except Exception: log.exception('execute crashed for run %s',msg)
        finally:
            try: redis.lrem(queueing.PROCESSING,1,msg)
            except Exception: log.exception('processing cleanup failed for run %s',msg)

if __name__=='__main__': main()
