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
    

def format_to_excel(data, property_customer_managed_id, date, building_name):
    # データフレームからExcelに変換
    df = pd.DataFrame(data)
    
    # 既存のテンプレートを読み込む
    template_path = './レントロール(原本).xlsx'
    wb = openpyxl.load_workbook(template_path)
    ws = wb.active

    # データを所定の場所に書き込む
    ws.cell(row=2, column=1, value=building_name)
    
    start_row = 7  # 開始行（例として2行目から）
    for index, row in df.iterrows():
        ws.cell(row=start_row + index, column=1, value=row['floor'])
        ws.cell(row=start_row + index, column=2, value=row['unit'])
        ws.cell(row=start_row + index, column=3, value=row['use_type'])
        ws.cell(row=start_row + index, column=4, value=row['contract_area_m2'])
        ws.cell(row=start_row + index, column=5, value=round(row['contract_area_tsubo'], 2))
        ws.cell(row=start_row + index, column=6, value=row['applicant_name'])
        ws.cell(row=start_row + index, column=7, value=row['contract_type'])
        ws.cell(row=start_row + index, column=8, value=row['start_date'])
        ws.cell(row=start_row + index, column=9, value=row['leasestart_date'])
        ws.cell(row=start_row + index, column=10, value=row['lease_end_date'])
        ws.cell(row=start_row + index, column=11, value=round(row['rent_per_tsubo'], 2))
        ws.cell(row=start_row + index, column=12, value=row['rent'])
        ws.cell(row=start_row + index, column=13, value=round(row['maintenance_fee_per_tsubo'], 2))
        ws.cell(row=start_row + index, column=14, value=row['maintenance_fee'])
        ws.cell(row=start_row + index, column=15, value=row['libli_club_monthly_fee'])
        ws.cell(row=start_row + index, column=16, value=row['tax'])
        ws.cell(row=start_row + index, column=17, value=row['other_cost'])
        ws.cell(row=start_row + index, column=18, value=row['other_cost_tax'])
        ws.cell(row=start_row + index, column=20, value=row['security_deposit_incl_tax'])
        ws.cell(row=start_row + index, column=21, value=row['key_money_incl_tax'])
        ws.cell(row=start_row + index, column=22, value=row['guarantee_deposit_incl_tax'])
        ws.cell(row=start_row + index, column=23, value=row['room_cleaning_fee_upon_move_out_excl_tax'])
        ws.cell(row=start_row + index, column=24, value=row['cleaning_tax'])
        ws.cell(row=start_row + index, column=25, value=row['renewal_fee'])
        ws.cell(row=start_row + index, column=26, value=row['renewal_office_fee'])
        ws.cell(row=start_row + index, column=26, value=row['renewal_office_fee_tax'])
        ws.cell(row=start_row + index, column=27, value=row['note'])

    # 出力ファイル名
    output_filename = f"{property_customer_managed_id}_{date}_rentroll.xlsx"
    output_filepath = f"./rentroll/{output_filename}"
    
    # ファイルに保存
    wb.save(output_filepath)
    
    return output_filepath

@router.post('/gcp/rentroll')
def post_query(item: dict):
    property_customer_managed_id = item["property_customer_managed_id"]
    date = item["date"]
    try:
        # ベースとなるSQLクエリ
        base_sql = """
                SELECT
                shared_ard_buildings.name AS building_name,
                shared_ard_rooms.room_floor_entrance_number AS floor,
                shared_ard_rooms.room_number AS unit,
                shared_ard_rooms.offer_use_type AS use_type,
                shared_ard_rooms.room_floor_area_area_amount AS contract_area_m2,
                shared_ard_rooms.room_floor_area_area_amount * 0.3205 AS contract_area_tsubo,
                shared_ard_leasings.applicant_name,
                shared_ard_leasings.contract_type,
                leasings_origin_leasing.start_date,
                shared_ard_leasings.contract_start_date AS lease_start_date,
                shared_ard_leasings.contract_end_date AS lease_end_date,
                shared_ard_adi_view_leasing_invoice_templete.rent_incl_tax / (shared_ard_rooms.room_floor_area_area_amount * 0.3205) AS rent_per_tsubo,
                shared_ard_adi_view_leasing_invoice_templete.rent_incl_tax AS rent,
                shared_ard_adi_view_leasing_invoice_templete.maintenance_fee_incl_tax / (shared_ard_rooms.room_floor_area_area_amount * 0.3205) AS maintenance_fee_per_tsubo,
                shared_ard_adi_view_leasing_invoice_templete.maintenance_fee_incl_tax AS maintenance_fee,
                shared_ard_adi_view_leasing_invoice_templete.libli_club_monthly_fee_incl_tax AS libli_club_monthly_fee,
                shared_ard_adi_view_leasing_invoice_templete.libli_club_monthly_fee_incl_tax - shared_ard_adi_view_leasing_invoice_templete.libli_club_monthly_fee_excl_tax AS tax,
                0 AS other_cost,
                0 AS other_cost_tax,
                shared_ard_adi_view_leasing_tenant_invoice.security_deposit_incl_tax,
                shared_ard_adi_view_leasing_tenant_invoice.key_money_incl_tax,
                shared_ard_adi_view_leasing_tenant_invoice.guarantee_deposit_incl_tax,
                shared_ard_adi_view_leasing_tenant_invoice.room_cleaning_fee_upon_move_out_excl_tax,
                shared_ard_adi_view_leasing_tenant_invoice.room_cleaning_fee_upon_move_out_incl_tax - shared_ard_adi_view_leasing_tenant_invoice.room_cleaning_fee_upon_move_out_excl_tax AS cleaning_tax,
                0 AS renewal_fee,
                0 AS renewal_office_fee,
                0 AS renewal_office_fee_tax,
                '' AS note
                FROM
                ard-itandi-production.shared_ard.buildings shared_ard_buildings
                LEFT JOIN
                ard-itandi-production.shared_ard.rooms shared_ard_rooms
                ON
                shared_ard_buildings.property_id = shared_ard_rooms.buildling_property_id
                LEFT JOIN
                ard-itandi-production.shared_ard.leasings shared_ard_leasings
                ON
                shared_ard_rooms.property_id = shared_ard_leasings.property_id
                LEFT JOIN
                ard-itandi-production.shared_ard_adi_view.leasing_tenant_invoice shared_ard_adi_view_leasing_tenant_invoice
                ON
                shared_ard_leasings.leasing_id = shared_ard_adi_view_leasing_tenant_invoice.leasing_id
                LEFT JOIN
                ard-itandi-production.shared_ard_adi_view.leasing_invoice_templete shared_ard_adi_view_leasing_invoice_templete
                ON
                shared_ard_leasings.leasing_id = shared_ard_adi_view_leasing_invoice_templete.leasing_id
                LEFT JOIN
                ard-itandi-production.shared_ard.origin_leasings leasings_origin_leasing
                ON
                shared_ard_leasings.origin_leasing_id = leasings_origin_leasing.id 
                """
        
        # フロントから受け取った情報でWHERE句を追加
        # shared_ard_adi_view_leasing_invoice_templete.rent_incl_tax IS NOT NULL
        where_clause = f"""
                        WHERE REGEXP_CONTAINS(shared_ard_buildings.property_customer_managed_code, '..{property_customer_managed_id}(-[0-9]+)?')
                        AND '{date}' > shared_ard_leasings.contract_start_date
                        AND '{date}' < shared_ard_leasings.contract_end_date
                        ORDER BY unit
                        """
        
        # 完成したSQLクエリ
        final_sql = base_sql + where_clause
        query_job = client.query(final_sql)  # クエリの実行
        results = query_job.result()  # クエリ結果の取得
        
        # 結果の整形
        rows = []
        building_name = ""
        for row in results:
            if building_name == "":
                building_name = row['building_name']
            rows.append(dict(row))
        
        print("rows :", rows)

        # 結果をExcelに整形
        output_filepath = format_to_excel(rows, property_customer_managed_id, date, building_name)

        # ファイルを返す
        filename=f"{property_customer_managed_id}_{date}_rentroll.xlsx"
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return FileResponse(output_filepath, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)
        
        
    except (GoogleAPICallError, NotFound) as e:
       print(e)
       raise HTTPException(status_code=400, detail=str(e))