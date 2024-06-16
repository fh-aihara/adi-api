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
import random, string
from pathlib import Path
import sqlite3
from sqlite3 import Connection
from typing import List, Dict
from database import create_db_and_tables, engine
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, NotFound
from pydantic import BaseModel
import pandas as pd
import openpyxl

DATABASE = 'bq_query.db'

# セッションを取得するための依存関係
def get_session():
    with Session(engine) as session:
        yield session

# router定義
router = APIRouter()

class SQLQuery(BaseModel):
    sql: str

@router.post('/gcp/query')
def post_query(query: SQLQuery, session: Session = Depends(get_session)):
    try:
        print(query)
        query_job = client.query(query.sql)  # クエリの実行
        results = query_job.result()  # クエリ結果の取得
        
        # 結果の整形
        rows = []
        for row in results:
            rows.append(dict(row))
        record_count = len(rows)
        # post_query({"SQL": str(query.sql), 
        #             "last_query_records": record_count},)
        session.add(query_histroy(SQL=str(query.sql), last_query_records=record_count))
        session.commit()
        return {"results": rows}
    except (GoogleAPICallError, NotFound) as e:
        raise HTTPException(status_code=400, detail=str(e))


# BigQueryクライアントの初期化
client = bigquery.Client()

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
    

def format_to_excel(data, property_customer_managed_id, date):
    # データフレームからExcelに変換
    df = pd.DataFrame(data)
    
    # 既存のテンプレートを読み込む
    template_path = '/mnt/data/レントロール(原本).xlsx'
    wb = openpyxl.load_workbook(template_path)
    ws = wb.active

    # データを所定の場所に書き込む
    # ここでは、適切なセルにデータを書き込む必要があります
    start_row = 2  # 開始行（例として2行目から）
    for index, row in df.iterrows():
        ws.cell(row=start_row + index, column=1, value=row['フロア'])
        ws.cell(row=start_row + index, column=2, value=row['区画'])
        ws.cell(row=start_row + index, column=3, value=row['用途'])
        ws.cell(row=start_row + index, column=4, value=row['契約面積(m2)'])
        ws.cell(row=start_row + index, column=5, value=row['契約面積(坪)'])
        ws.cell(row=start_row + index, column=6, value=row['applicant_name'])
        ws.cell(row=start_row + index, column=7, value=row['start_date'])
        ws.cell(row=start_row + index, column=8, value=row['自'])
        ws.cell(row=start_row + index, column=9, value=row['至'])
        ws.cell(row=start_row + index, column=10, value=row['家賃坪単価'])
        ws.cell(row=start_row + index, column=11, value=row['家賃'])
        ws.cell(row=start_row + index, column=12, value=row['共益費坪単価'])
        ws.cell(row=start_row + index, column=13, value=row['共益費'])
        ws.cell(row=start_row + index, column=14, value=row['リブリクラブ月額会費'])
        ws.cell(row=start_row + index, column=15, value=row['消費税'])
        ws.cell(row=start_row + index, column=16, value=row['その他費用'])
        ws.cell(row=start_row + index, column=17, value=row['その他費用消費税'])
        ws.cell(row=start_row + index, column=18, value=row['security_deposit_incl_tax'])
        ws.cell(row=start_row + index, column=19, value=row['key_money_incl_tax'])
        ws.cell(row=start_row + index, column=20, value=row['guarantee_deposit_incl_tax'])
        ws.cell(row=start_row + index, column=21, value=row['room_cleaning_fee_upon_move_out_excl_tax'])
        ws.cell(row=start_row + index, column=22, value=row['クリーニング消費税'])
        ws.cell(row=start_row + index, column=23, value=row['更新料'])
        ws.cell(row=start_row + index, column=24, value=row['更新事務手数料'])
        ws.cell(row=start_row + index, column=25, value=row['備考'])

    # 出力ファイル名
    output_filename = f"{property_customer_managed_id}_{date}_rentroll.xlsx"
    output_filepath = f"/mnt/data/{output_filename}"
    
    # ファイルに保存
    wb.save(output_filepath)
    
    return output_filepath

@router.post('/gcp/rentroll')
def post_query(item: dict, session: Session = Depends(get_session)):
    property_customer_managed_id = item["property_customer_managed_id"]
    date = item["date"]
    try:
        # ベースとなるSQLクエリ
        base_sql = """
        SELECT shared_ard_rooms.room_floor_entrance_number AS フロア, shared_ard_rooms.room_number AS 区画, shared_ard_rooms.offer_use_type AS 用途, shared_ard_rooms.room_floor_area_area_amount AS [契約面積(m2)], [shared_ard_rooms].[room_floor_area_area_amount]*0.3205 AS [契約面積(坪)], shared_ard_leasings.applicant_name, leasings_origin_leasing.start_date, shared_ard_leasings.contract_start_date AS 自, shared_ard_leasings.contract_end_date AS 至, [shared_ard_adi_view_leasing_invoice_templete].[rent_incl_tax]/([shared_ard_rooms].[room_floor_area_area_amount]*0.3205) AS 家賃坪単価, shared_ard_adi_view_leasing_invoice_templete.rent_incl_tax AS 家賃, [shared_ard_adi_view_leasing_invoice_templete].[maintenance_fee_incl_tax]/([shared_ard_rooms].[room_floor_area_area_amount]*0.3205) AS 共益費坪単価, shared_ard_adi_view_leasing_invoice_templete.maintenance_fee_incl_tax AS 共益費, shared_ard_adi_view_leasing_invoice_templete.libli_club_monthly_fee_incl_tax AS リブリクラブ月額会費, [shared_ard_adi_view_leasing_invoice_templete].[libli_club_monthly_fee_incl_tax]-[shared_ard_adi_view_leasing_invoice_templete].[libli_club_monthly_fee_excl_tax] AS 消費税, 0 AS その他費用, 0 AS その他費用消費税, shared_ard_adi_view_leasing_tenant_invoice.security_deposit_incl_tax, shared_ard_adi_view_leasing_tenant_invoice.key_money_incl_tax, shared_ard_adi_view_leasing_tenant_invoice.guarantee_deposit_incl_tax, shared_ard_adi_view_leasing_tenant_invoice.room_cleaning_fee_upon_move_out_excl_tax, [room_cleaning_fee_upon_move_out_incl_tax]-[room_cleaning_fee_upon_move_out_excl_tax] AS クリーニング消費税, 0 AS 更新料, 0 AS 更新事務手数料, "" AS 備考
        FROM ((((shared_ard_buildings shared_ard_buildings
        LEFT JOIN shared_ard_rooms shared_ard_rooms ON shared_ard_buildings.property_id = shared_ard_rooms.buildling_property_id)
        LEFT JOIN shared_ard_leasings shared_ard_leasings ON shared_ard_rooms.property_id = shared_ard_leasings.property_id)
        LEFT JOIN shared_ard_adi_view_leasing_tenant_invoice shared_ard_adi_view_leasing_tenant_invoice ON shared_ard_leasings.leasing_id = shared_ard_adi_view_leasing_tenant_invoice.leasing_id)
        LEFT JOIN shared_ard_adi_view_leasing_invoice_templete shared_ard_adi_view_leasing_invoice_templete ON shared_ard_leasings.leasing_id = shared_ard_adi_view_leasing_invoice_templete.leasing_id)
        LEFT JOIN leasings_origin_leasing leasings_origin_leasing ON shared_ard_leasings.leasing_id = leasings_origin_leasing.leasing_id
        WHERE ((shared_ard_adi_view_leasing_invoice_templete.rent_incl_tax) Is Not Null)
        """
        
        # フロントから受け取った情報でWHERE句を追加
        where_clause = f"""
        AND shared_ard_buildings.property_customer_managed_code LIKE '%{property_customer_managed_id}%'
        AND '{date}' > shared_ard_leasings.contract_start_date
        AND '{date}' < shared_ard_leasings.contract_end_date
        """
        
        # 完成したSQLクエリ
        final_sql = base_sql + where_clause
        
        query_job = session.query(final_sql)  # クエリの実行
        results = query_job.result()  # クエリ結果の取得
        
        # 結果の整形
        rows = []
        for row in results:
            rows.append(dict(row))
        
        print(rows)

        # 結果をExcelに整形
        output_filepath = format_to_excel(rows, query.property_customer_managed_id, query.date)
        
        # Excelファイルのパスを返す
        return {"file_path": output_filepath}
        
    except (GoogleAPICallError, NotFound) as e:
       raise HTTPException(status_code=400, detail=str(e))