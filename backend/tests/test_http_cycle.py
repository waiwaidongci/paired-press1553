# End-to-end smoke test through the HTTP layer: login -> study -> observations ->
# idempotent run acceptance -> worker execution -> result visibility -> export.
import os,sys,time,tempfile
from pathlib import Path

_tmp=tempfile.mkdtemp(prefix='paired-press-e2e-')
os.environ['DATABASE_URL']=f'sqlite:///{_tmp}/e2e.db'
os.environ['REDIS_URL']='redis://127.0.0.1:6399/15'
os.environ.setdefault('JWT_SECRET','test-secret')
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.config.database import Session
from src.middlewares import rate_limit
from src.models.user import User
from src.services import execution,queueing
from src.utils.security import hash_password
from tests.conftest import FakeRedis


@pytest.fixture()
def client(monkeypatch,tmp_path):
    fake=FakeRedis()
    monkeypatch.setattr(queueing,'_redis',fake)
    monkeypatch.setattr(rate_limit,'redis',fake)
    with TestClient(app) as c:
        with Session() as db:
            if not db.get(User,1):
                db.add(User(email='analyst@e2e.local',role='analyst',password_hash=hash_password('Research2026!')))
                db.commit()
        c._artifacts=tmp_path/'artifacts'
        c._redis=fake
        yield c


def login(client):
    r=client.post('/auth/login',json={'email':'analyst@e2e.local','password':'Research2026!'})
    assert r.status_code==200,r.text
    return {'Authorization':'Bearer '+r.json()['token']}


def test_full_cycle_over_http(client):
    auth=login(client)
    r=client.post('/studies',json={'name':'端到端批次','source_filename':'e2e.csv','description':''},headers=auth)
    assert r.status_code==200,r.text
    study_id=r.json()['id']
    for i in range(6):
        r=client.post(f'/studies/{study_id}/observations',json={'source_key':f'E{i+1}','values':{'before':5.0+i*0.1,'after':5.4+i*0.1}},headers=auth)
        assert r.status_code==200,r.text
    body={'study_id':study_id,'name':'端到端核算','parameters':{'resamples':200}}
    key={'Idempotency-Key':'e2e-req-1'}
    r1=client.post('/runs',json=body,headers={**auth,**key});assert r1.status_code==200,r1.text
    r2=client.post('/runs',json=body,headers={**auth,**key});assert r2.status_code==200
    assert r1.json()['id']==r2.json()['id'],'retry must not create a new run'
    conflict=client.post('/runs',json={**body,'parameters':{'resamples':400}},headers={**auth,**key})
    assert conflict.status_code==409,'same key with different inputs must be rejected'
    run_id=r1.json()['id']
    # worker picks it up (queue flushed to simulate message loss first)
    client._redis.flushall()
    assert queueing.sweep()==1
    msg=client._redis.blmove(queueing.QUEUE,queueing.PROCESSING,timeout=1)
    assert execution.execute(int(msg),'worker-e2e',client._artifacts)=='completed'
    client._redis.lrem(queueing.PROCESSING,1,msg)
    r=client.get(f'/runs/{run_id}',headers=auth)
    assert r.json()['status']=='completed' and r.json()['result']['metrics']['配对样本量']==6
    assert (client._artifacts/f'run-{run_id}.json').exists()
    csv=client.get(f'/runs/{run_id}/export',headers=auth)
    assert csv.status_code==200 and '双侧符号翻转p' in csv.text
