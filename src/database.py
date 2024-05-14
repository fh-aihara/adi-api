from sqlmodel import SQLModel, create_engine
from dotenv import load_dotenv
import os

load_dotenv()

url = os.getenv("DB_HOST")
# pool_pre_pingは接続前にプールのコネクションのexpireをチェックするオプション
engine = create_engine(url, pool_pre_ping=True)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine) #DBファイル・テーブル作成
