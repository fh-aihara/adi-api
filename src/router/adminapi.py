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
    ws.cell(row=2, column=1, value=safe_value(building_name))
    ws.cell(row=5, column=31, value=f"{safe_value(date)}時点")

    # 賃貸借契約テーブル 
    start_row = 8
    for index, row in enumerate(rentroll_data):
        ws.cell(row=start_row + index, column=1, value=safe_value(row.get('floor')))
        ws.cell(row=start_row + index, column=2, value=safe_value(row.get('unit')))
        ws.cell(row=start_row + index, column=3, value=safe_value(row.get('use_type')))
        ws.cell(row=start_row + index, column=4, value=safe_value(row.get('contract_area_m2')))
        ws.cell(row=start_row + index, column=5, value=safe_round(row.get('contract_area_tsubo')))
        ws.cell(row=start_row + index, column=6, value=safe_value(row.get('applicant_name')))
        ws.cell(row=start_row + index, column=7, value=safe_value(row.get('contract_type')))
        ws.cell(row=start_row + index, column=8, value=safe_value(row.get('start_date')))
        ws.cell(row=start_row + index, column=9, value=safe_value(row.get('lease_start_date')))
        ws.cell(row=start_row + index, column=10, value=safe_value(row.get('lease_end_date')))
        ws.cell(row=start_row + index, column=11, value=safe_round(row.get('rent_per_tsubo')))
        ws.cell(row=start_row + index, column=12, value=safe_value(row.get('rent')))
        ws.cell(row=start_row + index, column=13, value=safe_round(row.get('maintenance_fee_per_tsubo')))
        ws.cell(row=start_row + index, column=14, value=safe_value(row.get('maintenance_fee')))
        ws.cell(row=start_row + index, column=15, value=safe_value(row.get('libli_club_monthly_fee')))
        ws.cell(row=start_row + index, column=16, value=safe_value(row.get('tax')))
        ws.cell(row=start_row + index, column=17, value=safe_value(row.get('other_cost')))
        ws.cell(row=start_row + index, column=18, value=safe_value(row.get('other_cost_tax')))
        ws.cell(row=start_row + index, column=20, value=safe_value(row.get('security_deposit_incl_tax')))
        ws.cell(row=start_row + index, column=21, value=safe_value(row.get('key_money_incl_tax')))
        ws.cell(row=start_row + index, column=22, value=safe_value(row.get('guarantee_deposit_incl_tax')))
        ws.cell(row=start_row + index, column=23, value=safe_value(row.get('room_cleaning_fee_upon_move_out_excl_tax')))
        ws.cell(row=start_row + index, column=24, value=safe_value(row.get('cleaning_tax')))
        ws.cell(row=start_row + index, column=25, value=safe_value(row.get('renewal_fee')))
        ws.cell(row=start_row + index, column=26, value=safe_value(row.get('renewal_office_fee')))
        ws.cell(row=start_row + index, column=27, value=safe_value(row.get('renewal_office_fee_tax')))
        ws.cell(row=start_row + index, column=28, value=safe_value(row.get('note')))

    # 不要な行を非表示にする
    last_row = start_row + len(rentroll_data) - 1
    if last_row < 47:
        for row in range(last_row + 1, 48):
            ws.row_dimensions[row].hidden = True

    # 駐車場の情報を書き込む
    car_parking_start_row = 53
    car_parking_row = car_parking_start_row
    for row in parking_data:
        if row.get('parking_type') == 'car':
            ws.cell(row=car_parking_row, column=1, value=safe_value(row.get('parking_space_number')))
            ws.cell(row=car_parking_row, column=3, value='駐車場')
            ws.cell(row=car_parking_row, column=6, value=safe_value(row.get('applicant_name')))
            ws.cell(row=car_parking_row, column=7, value=safe_value(row.get('contract_type')))
            ws.cell(row=car_parking_row, column=8, value=safe_value(row.get('start_date')))
            ws.cell(row=car_parking_row, column=9, value=safe_value(row.get('lease_start_date')))
            ws.cell(row=car_parking_row, column=10, value=safe_value(row.get('lease_end_date')))
            parking_fee = safe_value(row.get('parking_fee_excl_tax'), 0)
            ws.cell(row=car_parking_row, column=11, value=parking_fee if parking_fee != 0 else None)
            ws.cell(row=car_parking_row, column=12, value=safe_value(row.get('parking_fee_tax')))
            ws.cell(row=car_parking_row, column=14, value=safe_value(row.get('security_deposit_incl_tax')))
            ws.cell(row=car_parking_row, column=15, value=safe_value(row.get('key_money_incl_tax')))
            ws.cell(row=car_parking_row, column=16, value=safe_value(row.get('renewal_fee')))
            ws.cell(row=car_parking_row, column=17, value=safe_value(row.get('renewal_office_fee')))
            ws.cell(row=car_parking_row, column=18, value=safe_value(row.get('renewal_office_fee_tax')))
            car_parking_row += 1
            if car_parking_row > 61:
                ws.insert_rows(car_parking_row)

    # 余った駐車場の行を非表示にする
    for row in range(car_parking_row, 62):
        ws.row_dimensions[row].hidden = True

    # バイク置き場の情報を書き込む
    motorbike_parking_start_row = 67
    motorbike_parking_row = motorbike_parking_start_row
    for row in parking_data:
        if row.get('parking_type') == 'motorbike':
            ws.cell(row=motorbike_parking_row, column=1, value=safe_value(row.get('parking_space_number')))
            ws.cell(row=motorbike_parking_row, column=3, value='バイク置き場')
            ws.cell(row=motorbike_parking_row, column=6, value=safe_value(row.get('applicant_name')))
            ws.cell(row=motorbike_parking_row, column=7, value=safe_value(row.get('contract_type')))
            ws.cell(row=motorbike_parking_row, column=8, value=safe_value(row.get('start_date')))
            ws.cell(row=motorbike_parking_row, column=9, value=safe_value(row.get('lease_start_date')))
            ws.cell(row=motorbike_parking_row, column=10, value=safe_value(row.get('lease_end_date')))
            motorcycle_fee = safe_value(row.get('motorcycle_parking_fee_excl_tax'), 0)
            ws.cell(row=motorbike_parking_row, column=11, value=motorcycle_fee if motorcycle_fee != 0 else None)
            ws.cell(row=motorbike_parking_row, column=12, value=safe_value(row.get('motorcycle_parking_fee_tax')))
            ws.cell(row=motorbike_parking_row, column=14, value=safe_value(row.get('security_deposit_incl_tax')))
            ws.cell(row=motorbike_parking_row, column=15, value=safe_value(row.get('key_money_incl_tax')))
            ws.cell(row=motorbike_parking_row, column=16, value=safe_value(row.get('renewal_fee')))
            ws.cell(row=motorbike_parking_row, column=17, value=safe_value(row.get('renewal_office_fee')))
            ws.cell(row=motorbike_parking_row, column=18, value=safe_value(row.get('renewal_office_fee_tax')))
            motorbike_parking_row += 1
            if motorbike_parking_row > 75:
                ws.insert_rows(motorbike_parking_row)

    # 余ったバイク置き場の行を非表示にする
    for row in range(motorbike_parking_row, 76):
        ws.row_dimensions[row].hidden = True

    # 出力ファイル名
    output_filename = f"{property_customer_managed_id}_{date}_rentroll.xlsx"
    output_filepath = f"./rentroll/{output_filename}"
    
    # ファイルに保存
    wb.save(output_filepath)
    
    return output_filepath