import json,csv,io,logging
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from src.models.run import Run
from src.models.review import Review
from src.services import studies,queueing
from src.services.domain_validation import validate_parameters
from src.services.audits import log
from src.utils.serializers import record,now,checksum

def _request_hash(data,params):
    return checksum({'study_id':data.study_id,'name':data.name,'parameters':params})

def create(db,data,user,request_key=None):
    studies.require(db,data.study_id)
    rows=studies.observations(db,data.study_id)
    if not rows: raise HTTPException(422,'先录入至少一条业务观测')
    try: params=validate_parameters(data.parameters)
    except (ValueError,TypeError) as e: raise HTTPException(422,str(e))
    request_hash=_request_hash(data,params) if request_key else None
    if request_key:
        existing=db.scalar(select(Run).where(Run.request_key==request_key))
        if existing:
            if existing.request_hash!=request_hash: raise HTTPException(409,'同一请求已受理且输入已冻结，换输入请修改表单后重新提交')
            return record(existing)
    snapshot=[{'source_key':x['source_key'],'values':x['values']} for x in rows]
    run=Run(study_id=data.study_id,name=data.name,parameters=params,snapshot=snapshot,checksum=checksum(snapshot),created_at=now(),status='queued',request_key=request_key,request_hash=request_hash)
    db.add(run)
    try: db.flush()
    except IntegrityError:
        # Concurrent duplicate submission lost the unique-key race: return the winner.
        db.rollback()
        existing=db.scalar(select(Run).where(Run.request_key==request_key)) if request_key else None
        if existing and existing.request_hash==request_hash: return record(existing)
        raise HTTPException(409,'同一请求已受理且输入已冻结，换输入请修改表单后重新提交')
    log(db,user,'run.queue',str(run.id));db.commit()
    try: queueing.enqueue(run.id)
    except Exception: logging.exception('enqueue failed for run %s; sweeper will redeliver',run.id)
    return record(run)

def get(db,id):
    run=db.get(Run,id)
    if not run: raise HTTPException(404,'运行不存在')
    result=record(run);review=db.scalar(select(Review).where(Review.run_id==id));result['review']=record(review) if review else None
    return result

def list_runs(db):
    return [get(db,x) for x in db.scalars(select(Run.id).order_by(Run.id.desc()))]

def approve(db,id,data,user):
    run=db.get(Run,id)
    if not run: raise HTTPException(404,'运行不存在')
    if run.status!='completed': raise HTTPException(409,'只能复核已完成运行')
    if db.scalar(select(Review).where(Review.run_id==id)): raise HTTPException(409,'已复核，记录不可覆盖')
    obj=Review(run_id=id,note=data.note,author=user.email,created_at=now());db.add(obj);log(db,user,'review.approve',f'{id}: {data.note}');db.commit();return record(obj)

def export(db,id,user):
    r=get(db,id)
    if r['status']!='completed': raise HTTPException(409,'尚无完成结果')
    if user.role=='viewer' and not r['review']: raise HTTPException(403,'观察员只能导出已复核结果')
    stream=io.StringIO();w=csv.writer(stream);w.writerow(['运行编号','输入校验SHA256','指标','值'])
    for k,v in r['result']['metrics'].items(): w.writerow([id,r['checksum'],k,json.dumps(v,ensure_ascii=False) if isinstance(v,(dict,list)) else v])
    w.writerow([id,r['checksum'],'解释',r['result']['interpretation']]);log(db,user,'run.export',id);db.commit()
    return '\ufeff'+stream.getvalue()
