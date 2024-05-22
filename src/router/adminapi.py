from base64 import decode
from lib2to3.pgen2 import token
from fastapi import APIRouter, Header, Depends, HTTPException, status, Response, UploadFile
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from models import query_histroy
from sqlalchemy import null
from sqlmodel import Field, SQLModel, create_engine, Session, select, literal_column, table, desc
from typing import Union
import datetime
import os
import time
import logging
import random, string
from pathlib import Path
import sqlite3
from sqlite3 import Connection
from typing import List, Dict
from database import create_db_and_tables, engine
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, NotFound
from pydantic import BaseModel

DATABASE = 'bq_query.db'

# セッションを取得するための依存関係
def get_session():
    with Session(engine) as session:
        yield session

# router定義
router = APIRouter()

class SQLQuery(BaseModel):
    sql: str

# @router.post('/gcp/query')
# def post_query(query: SQLQuery, session: Session = Depends(get_session)):
#     try:
#         print(query)
#         query_job = client.query(query.sql)  # クエリの実行
#         results = query_job.result()  # クエリ結果の取得
        
#         # 結果の整形
#         rows = []
#         for row in results:
#             rows.append(dict(row))
#         record_count = len(rows)
#         # post_query({"SQL": str(query.sql), 
#         #             "last_query_records": record_count},)
#         session.add(query_histroy(SQL=str(query.sql), last_query_records=record_count))
#         session.commit()
#         return {"results": rows}
#     except (GoogleAPICallError, NotFound) as e:
#         raise HTTPException(status_code=400, detail=str(e))


# # BigQueryクライアントの初期化
# client = bigquery.Client()

# init 処理
@router.on_event("startup")
def on_startup():
    # データベースとテーブルの作成
    create_db_and_tables()

       
@router.get('/', tags=["utils"])
def health_check():
    """
    サーバーヘルスチェック
    """
    return ('200 OK')


@router.get('/queries')
def get_queris(session: Session = Depends(get_session)):
    query = select(query_histroy)
    results = session.exec(query).all()
    return results


@router.post('/query')
def post_query(query_history: query_histroy, session: Session = Depends(get_session)):
    print(query_history)
    print(session)
    session.add(query_history)
    session.commit()
    session.refresh(query_history)
    return query_history


@router.put("/queries/{query_id}")
def update_queries(query_id: int, query_history: query_histroy, session: Session = Depends(get_session)):
    existing_query = session.get(query_histroy, query_id)
    if not existing_query:
        raise HTTPException(status_code=404, detail="Query not found")
    # existing_query.SQL = query_history.SQL
    existing_query.title = query_history.title
    existing_query.description = query_history.description
    # existing_query.last_query_records = query_history.last_query_records
    session.commit()
    session.refresh(existing_query)
    return existing_query
    

@router.post('/login')
def login(item: dict):
    try:
        print(item)
        username = item["username"]
        password = item["password"]
        # if username == "adi2024" and password == "tc8UYHLT":
        if username == "adi2024" and password == "adi2024":
            return "SUCCESS"
        else:
            return "WRONG"
    except:
        return "ERROR"
    
