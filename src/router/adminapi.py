from base64 import decode
from lib2to3.pgen2 import token
from fastapi import APIRouter, Header, Depends, HTTPException, status, Response, UploadFile
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from fastapi.middleware.base import BaseHTTPMiddleware
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
from copy import copy
import json
import logging
from logging.handlers import RotatingFileHandler


DATABASE = 'bq_query.db'

# ログ設定
logger = logging.getLogger("user_activity")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    "user_activity.log",
    maxBytes=10485760,  # 10MB
    backupCount=10
)
formatter = logging.Formatter('%(asctime)s,%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# ログミドルウェア
class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # リクエストボディを取得 (非同期)
        body = None
        if request.method in ["POST", "PUT"]:
            body_bytes = await request.body()
            if body_bytes:
                try:
                    body = json.loads(body_bytes.decode())
                except:
                    body = "(non-JSON payload)"
        
        # リクエストパスを取得
        path = request.url.path
        # ユーザーIDを取得（ヘッダーにない場合は "unknown"）
        user_id = request.headers.get("X-User-ID", "unknown")
        
        # クエリパラメータがあれば取得
        query_params = dict(request.query_params)
        
        # パラメータ情報（ボディとクエリの両方）
        params = ""
        if body:
            params = f"{json.dumps(body)}"
        if query_params:
            params += f"{json.dumps(query_params)}"
        
        # ログ出力
        logger.info(f"{user_id},{path},{params}")
        
        # リクエスト処理を続行
        response = await call_next(request)
        return response

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
    existing_query.title = query_history.title
    existing_query.description = query_history.description
    session.commit()
    session.refresh(existing_query)
    return existing_query
    

@router.post('/login')
def login(item: dict):
    try:
        print(item)
        username = item["username"]
        password = item["password"]
        
        # Original account - full access
        if username == "adi2024" and password == "adi2024":
            return {
                "message": "success",
                "auth": {
                    "rentroll": True,
                    "keiri": True
                },
                "user_id" : 1
            }
        # New account - limited access
        elif username == "adirent2025" and password == "adirent2025":
            return {
                "message": "success",
                "auth": {
                    "rentroll": True,
                    "keiri": False
                },
                "user_id" : 2
            }
        else:
            return {
                "message": "wrong",
                "auth": None
            }
    except:
        return {
            "message": "error",
            "auth": None
        }    

@router.post('/gcp/rentroll')
def post_query(item: dict):
    property_customer_managed_id = item["property_customer_managed_id"]
    date = item["date"]
    try:
        # 賃貸借契約のクエリ
        rentroll_sql = f"""
        SELECT
            *
        FROM
            ard-itandi-production.shared_ard_adi_view.rentroll_output_table as rentroll_output_table
        WHERE
            REGEXP_CONTAINS(rentroll_output_table.property_customer_managed_code, '..{property_customer_managed_id}(-[0-9]+)?')
        ORDER BY
            unit
        """
        
        # 駐車場とバイク置き場のクエリ
        parking_sql = f"""
        SELECT
            *
        FROM
            ard-itandi-production.shared_ard_adi_view.rentroll_parking_output_table as rentroll_parking_output_table
        WHERE
            REGEXP_CONTAINS(rentroll_parking_output_table.property_customer_managed_code, '..{property_customer_managed_id}(-[0-9]+)?')
        ORDER BY
            parking_type, parking_space_number
        """

        # クエリの実行
        rentroll_job = client.query(rentroll_sql)
        parking_job = client.query(parking_sql)

        # 結果の取得
        rentroll_results = rentroll_job.result()
        parking_results = parking_job.result()
        
        # 結果の整形
        rentroll_rows = [dict(row) for row in rentroll_results]
        parking_rows = [dict(row) for row in parking_results]
        
        building_name = rentroll_rows[0]['building_name'] if rentroll_rows else ""
        
        print("rentroll_rows :", rentroll_rows)
        print("parking_rows :", parking_rows)

        # 結果をExcelに整形
        output_filepath = format_to_excel(rentroll_rows, parking_rows, property_customer_managed_id, date, building_name)

        # ファイルを返す
        filename = f"{property_customer_managed_id}_{date}_rentroll.xlsx"
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return FileResponse(output_filepath, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)
        
    except (GoogleAPICallError, NotFound) as e:
       print(e)
       raise HTTPException(status_code=400, detail=str(e))


def write_to_merged_cell(ws, row, col, value):
    cell = ws.cell(row=row, column=col)
    if cell.coordinate in ws.merged_cells:
        for merged_range in ws.merged_cells.ranges:
            if cell.coordinate in merged_range:
                top_left = merged_range.min_row, merged_range.min_col
                ws.cell(row=top_left[0], column=top_left[1], value=value)
                return
    ws.cell(row=row, column=col, value=value)


def format_to_excel(rentroll_data, parking_data, property_customer_managed_id, date, building_name):
    # 既存のテンプレートを読み込む
    template_path = './レントロール(原本).xlsx'
    wb = openpyxl.load_workbook(template_path)
    ws = wb.active

    # null値を安全に処理する関数
    def safe_value(value, default=''):
        return value if value is not None else default

    def safe_round(value, decimals=2):
        return round(value, decimals) if value is not None else None

    # データを所定の場所に書き込む
    # title
    write_to_merged_cell(ws, 2, 1, safe_value(building_name))
    write_to_merged_cell(ws, 5, 31, f"{safe_value(date)}時点")

    # 賃貸借契約テーブル 
    start_row = 8
    end_row = 104
    current_row = start_row

    for row in rentroll_data:
        if current_row <= end_row:
            write_to_merged_cell(ws, current_row, 1, safe_value(row.get('floor')))
            write_to_merged_cell(ws, current_row, 2, safe_value(row.get('unit')))
            write_to_merged_cell(ws, current_row, 3, safe_value(row.get('use_type')))
            write_to_merged_cell(ws, current_row, 4, safe_value(row.get('contract_area_m2')))
            write_to_merged_cell(ws, current_row, 5, safe_round(row.get('contract_area_tsubo')))
            write_to_merged_cell(ws, current_row, 6, safe_value(row.get('applicant_name')))
            write_to_merged_cell(ws, current_row, 7, safe_value(row.get('contract_type')))
            write_to_merged_cell(ws, current_row, 8, safe_value(row.get('start_date')))
            write_to_merged_cell(ws, current_row, 9, safe_value(row.get('lease_start_date')))
            write_to_merged_cell(ws, current_row, 10, safe_value(row.get('lease_end_date')))
            write_to_merged_cell(ws, current_row, 11, safe_round(row.get('rent_per_tsubo')))
            write_to_merged_cell(ws, current_row, 12, safe_value(row.get('rent')))
            write_to_merged_cell(ws, current_row, 13, safe_round(row.get('maintenance_fee_per_tsubo')))
            write_to_merged_cell(ws, current_row, 14, safe_value(row.get('maintenance_fee')))
            write_to_merged_cell(ws, current_row, 15, safe_value(row.get('libli_club_monthly_fee')))
            write_to_merged_cell(ws, current_row, 16, safe_value(row.get('tax')))
            write_to_merged_cell(ws, current_row, 17, safe_value(row.get('other_cost')))
            write_to_merged_cell(ws, current_row, 18, safe_value(row.get('other_cost_tax')))
            write_to_merged_cell(ws, current_row, 20, safe_value(row.get('security_deposit_incl_tax')))
            write_to_merged_cell(ws, current_row, 21, safe_value(row.get('key_money_incl_tax')))
            write_to_merged_cell(ws, current_row, 22, safe_value(row.get('guarantee_deposit_incl_tax')))
            write_to_merged_cell(ws, current_row, 23, safe_value(row.get('room_cleaning_fee_upon_move_out_excl_tax')))
            write_to_merged_cell(ws, current_row, 24, safe_value(row.get('cleaning_tax')))
            write_to_merged_cell(ws, current_row, 25, safe_value(row.get('renewal_fee')))
            write_to_merged_cell(ws, current_row, 26, safe_value(row.get('renewal_office_fee')))
            write_to_merged_cell(ws, current_row, 27, safe_value(row.get('renewal_office_fee_tax')))
            write_to_merged_cell(ws, current_row, 28, safe_value(row.get('note')))
            current_row += 1

    # 余った部屋の行を非表示にする
    for row in range(current_row, end_row + 1):
        ws.row_dimensions[row].hidden = True

    # 駐車場の情報を書き込む
    car_parking_start_row = 110
    car_parking_end_row = 151
    car_parking_row = car_parking_start_row

    for row in parking_data:
        if row.get('parking_type') == 'car' and car_parking_row <= car_parking_end_row:
            write_to_merged_cell(ws, car_parking_row, 1, safe_value(row.get('parking_space_number')))
            write_to_merged_cell(ws, car_parking_row, 3, '駐車場')
            write_to_merged_cell(ws, car_parking_row, 6, safe_value(row.get('applicant_name')))
            write_to_merged_cell(ws, car_parking_row, 7, safe_value(row.get('contract_type')))
            write_to_merged_cell(ws, car_parking_row, 8, safe_value(row.get('start_date')))
            write_to_merged_cell(ws, car_parking_row, 9, safe_value(row.get('lease_start_date')))
            write_to_merged_cell(ws, car_parking_row, 10, safe_value(row.get('lease_end_date')))
            parking_fee = safe_value(row.get('parking_fee_excl_tax'), 0)
            write_to_merged_cell(ws, car_parking_row, 11, parking_fee if parking_fee != 0 else None)
            write_to_merged_cell(ws, car_parking_row, 12, safe_value(row.get('parking_fee_tax')))
            write_to_merged_cell(ws, car_parking_row, 14, safe_value(row.get('security_deposit_incl_tax')))
            write_to_merged_cell(ws, car_parking_row, 15, safe_value(row.get('key_money_incl_tax')))
            write_to_merged_cell(ws, car_parking_row, 16, safe_value(row.get('renewal_fee')))
            write_to_merged_cell(ws, car_parking_row, 17, safe_value(row.get('renewal_office_fee')))
            write_to_merged_cell(ws, car_parking_row, 18, safe_value(row.get('renewal_office_fee_tax')))
            car_parking_row += 1

    # 余った駐車場の行を非表示にする
    for row in range(car_parking_row, car_parking_end_row + 1):
        ws.row_dimensions[row].hidden = True

    # バイク置き場の情報を書き込む
    motorbike_parking_start_row = 157
    motorbike_parking_end_row = 196
    motorbike_parking_row = motorbike_parking_start_row

    for row in parking_data:
        if row.get('parking_type') == 'motorbike' and motorbike_parking_row <= motorbike_parking_end_row:
            write_to_merged_cell(ws, motorbike_parking_row, 1, safe_value(row.get('parking_space_number')))
            write_to_merged_cell(ws, motorbike_parking_row, 3, 'バイク置き場')
            write_to_merged_cell(ws, motorbike_parking_row, 6, safe_value(row.get('applicant_name')))
            write_to_merged_cell(ws, motorbike_parking_row, 7, safe_value(row.get('contract_type')))
            write_to_merged_cell(ws, motorbike_parking_row, 8, safe_value(row.get('start_date')))
            write_to_merged_cell(ws, motorbike_parking_row, 9, safe_value(row.get('lease_start_date')))
            write_to_merged_cell(ws, motorbike_parking_row, 10, safe_value(row.get('lease_end_date')))
            motorcycle_fee = safe_value(row.get('motorcycle_parking_fee_excl_tax'), 0)
            write_to_merged_cell(ws, motorbike_parking_row, 11, motorcycle_fee if motorcycle_fee != 0 else None)
            write_to_merged_cell(ws, motorbike_parking_row, 12, safe_value(row.get('motorcycle_parking_fee_tax')))
            write_to_merged_cell(ws, motorbike_parking_row, 14, safe_value(row.get('security_deposit_incl_tax')))
            write_to_merged_cell(ws, motorbike_parking_row, 15, safe_value(row.get('key_money_incl_tax')))
            write_to_merged_cell(ws, motorbike_parking_row, 16, safe_value(row.get('renewal_fee')))
            write_to_merged_cell(ws, motorbike_parking_row, 17, safe_value(row.get('renewal_office_fee')))
            write_to_merged_cell(ws, motorbike_parking_row, 18, safe_value(row.get('renewal_office_fee_tax')))
            motorbike_parking_row += 1

    # 余ったバイク置き場の行を非表示にする
    for row in range(motorbike_parking_row, motorbike_parking_end_row + 1):
        ws.row_dimensions[row].hidden = True

    # 出力ファイル名
    output_filename = f"{property_customer_managed_id}_{date}_rentroll.xlsx"
    output_filepath = f"./rentroll/{output_filename}"
    
    # ファイルに保存
    wb.save(output_filepath)
    
    return output_filepath

# パラメータを受け取るためのモデルを追加
class SuitotyoParams(BaseModel):
    start_date: str
    end_date: str
    account: str

class HosyoKaisyaParams(BaseModel):
   account_year: int 
   account_month: int
   
@router.post('/gcp/suitotyo')
def get_suitotyo(params: SuitotyoParams):
    try:
        # 出納帳のクエリ
        suitotyo_sql = f"""
        SELECT
            *
        FROM
            `ard-itandi-production.shared_ard_adi_view.suitotyo`
        WHERE
            `入金日` between "{params.start_date}" and "{params.end_date}"
            AND `口座` = "{params.account}"
        """

        print(suitotyo_sql)
        
        # クエリの実行
        query_job = client.query(suitotyo_sql)
        results = query_job.result()
        
        # 結果の整形
        rows = [dict(row) for row in results]
        
        return {"results": rows}
        
    except (GoogleAPICallError, NotFound) as e:
        print(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post('/gcp/hosyo-kaisya-unmatch')
def get_hosyo_kaisya_unmatch(params: HosyoKaisyaParams):
   try:
       hosyo_sql = f"""
       SELECT
           *
       FROM
           `ard-itandi-production.shared_ard_adi_view.hosyo_kaisya_matchlist` 
       WHERE
           account_year = {params.account_year}
           AND account_month = {params.account_month}
       """

       print(hosyo_sql)
       
       query_job = client.query(hosyo_sql)
       results = query_job.result()

       rows = [dict(row) for row in results]
       return {"results": rows}
       
   except (GoogleAPICallError, NotFound) as e:
       print(e)
       raise HTTPException(status_code=400, detail=str(e))

