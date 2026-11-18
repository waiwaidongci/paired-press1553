import os,sys,time,tempfile
from pathlib import Path
import pytest

_tmp=tempfile.mkdtemp(prefix='paired-press-test-')
os.environ['DATABASE_URL']=f'sqlite:///{_tmp}/test.db'
os.environ['REDIS_URL']='redis://127.0.0.1:6399/15'  # never dialed; FakeRedis is injected
os.environ.setdefault('JWT_SECRET','test-secret')

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))  # backend/ on sys.path

from src.config.database import Base,engine,Session  # noqa: E402
from src.models import user,study,observation,run,review,audit  # noqa: E402,F401
from src.services import queueing  # noqa: E402


class FakeRedis:
    """In-memory stand-in for the Redis commands the queue layer uses."""
    def __init__(self):
        self.lists={};self.kv={};self.down=False
    def _check(self):
        if self.down: raise ConnectionError('redis unreachable')
    def rpush(self,key,*values):
        self._check();lst=self.lists.setdefault(key,[]);lst.extend(str(v) for v in values);return len(lst)
    def lrange(self,key,start,end):
        self._check();items=self.lists.get(key,[]);end=len(items)-1 if end==-1 else end
        return list(items[start:end+1])
    def lrem(self,key,count,value):
        self._check();value=str(value);removed=0;out=[]
        for x in self.lists.get(key,[]):
            if x==value and (count==0 or removed<abs(count)): removed+=1;continue
            out.append(x)
        self.lists[key]=out;return removed
    def blmove(self,source,destination,timeout=0,src='LEFT',dest='LEFT'):
        self._check();deadline=time.monotonic()+(timeout or 0)
        while True:
            items=self.lists.setdefault(source,[])
            if items:
                value=items.pop(0 if src=='LEFT' else -1)
                dst=self.lists.setdefault(destination,[])
                dst.insert(0 if dest=='LEFT' else len(dst),value)
                return value
            if time.monotonic()>=deadline: return None
            time.sleep(0.01)
    def set(self,key,value,nx=False,ex=None):
        self._check()
        if nx and key in self.kv: return None
        self.kv[key]=value;return True
    def incr(self,key):
        self._check();self.kv[key]=int(self.kv.get(key,0))+1;return self.kv[key]
    def expire(self,key,seconds):
        self._check();return True
    def delete(self,*keys):
        self._check()
        for k in keys: self.lists.pop(k,None);self.kv.pop(k,None)
    def flushall(self):
        self.lists.clear();self.kv.clear()
    def ping(self):
        self._check();return True


@pytest.fixture()
def redis_fake(monkeypatch):
    fake=FakeRedis()
    monkeypatch.setattr(queueing,'_redis',fake)
    return fake


@pytest.fixture()
def db_session():
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables): conn.execute(table.delete())
    yield Session
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables): conn.execute(table.delete())


@pytest.fixture()
def study_id(db_session):
    from src.models.study import Study
    from src.models.observation import Observation
    with db_session() as db:
        s=Study(name='测试批次',source_filename='t.csv',description='')
        db.add(s);db.flush()
        for i in range(6):
            db.add(Observation(study_id=s.id,source_key=f'P{i+1}',values={'before':5.0+i*0.1,'after':5.3+i*0.1}))
        db.commit()
        return s.id


@pytest.fixture()
def artifacts(tmp_path):
    return tmp_path/'artifacts'
