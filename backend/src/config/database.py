import os
from pathlib import Path
from sqlalchemy import create_engine,text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
class Base(DeclarativeBase):
    pass
_url=os.environ['DATABASE_URL']
# check_same_thread/timeout let the worker heartbeat thread share a SQLite test database.
engine=create_engine(_url,pool_pre_ping=True,connect_args={'check_same_thread':False,'timeout':30} if _url.startswith('sqlite') else {})
Session=sessionmaker(bind=engine,expire_on_commit=False)
def get_db():
    with Session() as db:
        yield db
def migrate():
    # Idempotent schema upgrades for the live PostgreSQL database. Tests and fresh
    # installs get the full schema from the models via create_all instead.
    if engine.dialect.name!='postgresql': return
    sql=(Path(__file__).resolve().parents[2]/'migrations'/'002_reliable_execution.sql').read_text()
    with engine.begin() as conn:
        for stmt in sql.split(';'):
            stmt=stmt.strip()
            if stmt: conn.execute(text(stmt))
