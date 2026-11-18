# Reliability verification for accepted analyses:
# delivery outage, queue flush, long computation vs concurrent takeover,
# crash after archiving, idempotent acceptance, and legacy row adoption.
import json,time,threading
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import select,func

from src.models.run import Run
from src.services import execution,queueing,runs
from src.services.algorithm import calculate
from src.utils.serializers import checksum,now

USER=SimpleNamespace(email='analyst@test.local',role='analyst')

def payload(study_id,name='本轮核算',params=None):
    return SimpleNamespace(study_id=study_id,name=name,parameters=params if params is not None else {'resamples':200})

def create_run(db,study_id,key=None,name='本轮核算'):
    return runs.create(db,payload(study_id,name),USER,request_key=key)

def get_run(db,run_id):
    return db.get(Run,run_id)

# ---------- acceptance: idempotency and frozen inputs ----------

def test_retry_same_request_returns_same_run(db_session,redis_fake,study_id):
    with db_session() as db:
        r1=create_run(db,study_id,key='req-1')
        r2=create_run(db,study_id,key='req-1')
        assert r2['id']==r1['id']
        assert db.scalar(select(func.count()).select_from(Run))==1

def test_retry_keeps_frozen_snapshot(db_session,redis_fake,study_id):
    from src.models.observation import Observation
    with db_session() as db:
        r1=create_run(db,study_id,key='req-frozen')
        row=db.scalar(select(Observation).where(Observation.study_id==study_id))
        row.values={'before':99.0,'after':99.5};db.commit()  # source data changes afterwards
        r2=create_run(db,study_id,key='req-frozen')
        assert r2['snapshot']==r1['snapshot'] and r2['checksum']==r1['checksum']

def test_same_key_different_inputs_rejected(db_session,redis_fake,study_id):
    with db_session() as db:
        create_run(db,study_id,key='req-2')
        with pytest.raises(HTTPException) as e:
            runs.create(db,payload(study_id,params={'resamples':400}),USER,request_key='req-2')
        assert e.value.status_code==409
        assert db.scalar(select(func.count()).select_from(Run))==1

def test_new_key_creates_new_run(db_session,redis_fake,study_id):
    with db_session() as db:
        r1=create_run(db,study_id,key='req-3a')
        r2=create_run(db,study_id,key='req-3b')
        assert r2['id']!=r1['id']

# ---------- delivery: outage and message loss are healed by the sweeper ----------

def test_delivery_outage_auto_redelivery(db_session,redis_fake,study_id,artifacts):
    redis_fake.down=True  # 断网投递: Redis unreachable at acceptance time
    with db_session() as db: r=create_run(db,study_id,key='req-4')
    assert redis_fake.lists.get(queueing.QUEUE,[])==[]
    redis_fake.down=False
    assert queueing.sweep()==1
    assert str(r['id']) in redis_fake.lrange(queueing.QUEUE,0,-1)
    msg=redis_fake.blmove(queueing.QUEUE,queueing.PROCESSING,timeout=1)
    assert execution.execute(int(msg),'worker-a',artifacts)=='completed'
    redis_fake.lrem(queueing.PROCESSING,1,msg)
    with db_session() as db:
        run=get_run(db,r['id']);assert run.status=='completed' and run.result['input_sha256']==run.checksum

def test_queue_flush_recovery(db_session,redis_fake,study_id,artifacts):
    with db_session() as db: r=create_run(db,study_id,key='req-5')
    redis_fake.flushall()  # 消息清空: every queued message is gone
    assert queueing.sweep()==1
    msg=redis_fake.blmove(queueing.QUEUE,queueing.PROCESSING,timeout=1)
    assert int(msg)==r['id']
    assert execution.execute(int(msg),'worker-a',artifacts)=='completed'

# ---------- execution: lease, heartbeat, takeover, fencing ----------

def test_long_computation_holds_lease_then_expiry_allows_takeover(db_session,redis_fake,study_id,monkeypatch):
    monkeypatch.setattr(execution,'LEASE_SECONDS',0.4)
    with db_session() as db: r=create_run(db,study_id,key='req-6')
    with db_session() as db: seq=execution.try_claim(db,r['id'],'worker-long')
    with db_session() as db: lease1=get_run(db,r['id']).lease_until
    hb=execution.Heartbeat(r['id'],seq,db_session,interval=0.05)  # 长计算持续持有
    hb.start();time.sleep(0.3)
    with db_session() as db:
        lease2=get_run(db,r['id']).lease_until
        assert lease2>lease1
        assert execution.try_claim(db,r['id'],'worker-b') is None  # live lease blocks takeover
    hb.stop();time.sleep(0.5)  # 失联: heartbeat gone, lease expires
    with db_session() as db:
        assert execution.try_claim(db,r['id'],'worker-b')==seq+1

def test_takeover_then_recovered_worker_cannot_overwrite(db_session,redis_fake,study_id,artifacts,monkeypatch):
    monkeypatch.setattr(execution,'LEASE_SECONDS',0.2)
    with db_session() as db: r=create_run(db,study_id,key='req-7')
    with db_session() as db:
        seq_a=execution.try_claim(db,r['id'],'worker-a')
        run=get_run(db,r['id']);snapshot,params,expected=run.snapshot,run.parameters,run.checksum
    time.sleep(0.3)  # worker-a goes silent; lease expires
    with db_session() as db: seq_b=execution.try_claim(db,r['id'],'worker-b')
    assert seq_b==seq_a+1
    result_b=calculate(snapshot,params);result_b['input_sha256']=expected
    payload_b=json.dumps(result_b,ensure_ascii=False,allow_nan=False).encode()
    execution.archive(artifacts,r['id'],seq_b,payload_b)
    with db_session() as db: assert execution.publish(db,r['id'],seq_b,result_b)
    # worker-a recovers late: its fenced publish is rejected, the archive stays intact
    result_a=calculate(snapshot,params);result_a['input_sha256']=expected
    payload_a=json.dumps(result_a,ensure_ascii=False,allow_nan=False).encode()
    execution.archive(artifacts,r['id'],seq_a,payload_a)  # identical recompute: harmless
    with pytest.raises(IOError): execution.archive(artifacts,r['id'],seq_a,b'{"tampered":true}')
    with db_session() as db: assert not execution.publish(db,r['id'],seq_a,result_a)
    assert (artifacts/f'run-{r["id"]}.json').read_bytes()==payload_b
    with db_session() as db:
        run=get_run(db,r['id'])
        assert run.status=='completed' and run.claim_seq==seq_b and run.result==result_b

def test_crash_after_archive_recovers_without_partial_publish(db_session,redis_fake,study_id,artifacts,monkeypatch):
    monkeypatch.setattr(execution,'LEASE_SECONDS',0.2)
    with db_session() as db: r=create_run(db,study_id,key='req-8')
    with db_session() as db:
        seq=execution.try_claim(db,r['id'],'worker-a')
        run=get_run(db,r['id']);snapshot,params,expected=run.snapshot,run.parameters,run.checksum
    result=calculate(snapshot,params);result['input_sha256']=expected
    payload=json.dumps(result,ensure_ascii=False,allow_nan=False).encode()
    execution.archive(artifacts,r['id'],seq,payload)
    # --- 落档后崩溃: archive durable, publish never happened, worker process dies ---
    with db_session() as db:
        run=get_run(db,r['id'])
        assert run.status=='running' and run.result is None  # nothing half-published
    time.sleep(0.3)
    assert execution.execute(r['id'],'worker-b',artifacts)=='completed'
    assert (artifacts/f'run-{r["id"]}.json').read_bytes()==payload  # archive byte-identical
    assert list(artifacts.glob('*.tmp'))==[]
    with db_session() as db:
        run=get_run(db,r['id'])
        assert run.status=='completed' and run.result['metrics']==result['metrics']

def test_concurrent_claims_have_single_winner(db_session,redis_fake,study_id):
    with db_session() as db: r=create_run(db,study_id,key='req-9')
    barrier=threading.Barrier(2);outcome={}
    def claim(worker):
        barrier.wait()
        with db_session() as db: outcome[worker]=execution.try_claim(db,r['id'],worker)
    threads=[threading.Thread(target=claim,args=(f'worker-{i}',)) for i in (1,2)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert sorted(v for v in outcome.values() if v is not None)==[1]

def test_duplicate_message_is_not_recomputed(db_session,redis_fake,study_id,artifacts):
    with db_session() as db: r=create_run(db,study_id,key='req-10')
    assert execution.execute(r['id'],'worker-a',artifacts)=='completed'
    with db_session() as db: result1=get_run(db,r['id']).result
    assert execution.execute(r['id'],'worker-b',artifacts)=='skipped'
    with db_session() as db:
        run=get_run(db,r['id'])
        assert run.result==result1 and run.claim_seq==1

# ---------- adoption: pre-existing queued rows continue, history is immutable ----------

def test_legacy_rows_continue_and_history_untouched(db_session,redis_fake,study_id,artifacts):
    snapshot=[{'source_key':f'L{i}','values':{'before':5.0+i*0.1,'after':5.2+i*0.1}} for i in range(4)]
    history_result={'metrics':{'配对样本量':4},'interpretation':'历史结论'}
    with db_session() as db:
        legacy=Run(study_id=study_id,name='旧排队',parameters={'resamples':200},snapshot=snapshot,
                   checksum=checksum(snapshot),created_at=now(),status='queued')
        done=Run(study_id=study_id,name='历史结论',parameters={'resamples':200},snapshot=snapshot,
                 checksum=checksum(snapshot),created_at=now(),status='completed',result=history_result)
        db.add_all([legacy,done]);db.commit();legacy_id,done_id=legacy.id,done.id
    queueing.sweep()  # 现存排队项接续
    queued=redis_fake.lrange(queueing.QUEUE,0,-1)
    assert str(legacy_id) in queued and str(done_id) not in queued
    assert execution.execute(legacy_id,'worker-a',artifacts)=='completed'
    assert execution.execute(done_id,'worker-a',artifacts)=='skipped'  # 历史结论不改
    with db_session() as db:
        legacy,done=get_run(db,legacy_id),get_run(db,done_id)
        assert legacy.status=='completed' and legacy.result['input_sha256']==legacy.checksum
        assert done.status=='completed' and done.result==history_result and done.claim_seq==0
