from base64 import decode
from lib2to3.pgen2 import token
from fastapi import APIRouter, Header, Depends, HTTPException, status, Response, UploadFile, Request
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi import FastAPI
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
from copy import copy
import json
import logging
from logging.handlers import RotatingFileHandler
import boto3
import io


DATABASE = 'bq_query.db'

# Logic Apps Workflow URL
LOGIC_APPS_WEBHOOK_URL = "https://prod-02.japaneast.logic.azure.com:443/workflows/38314c48bb6b416a96ced12643cea29c/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=5DJlT6fyzTBMZ1yYi4K75CgmLcDUKrDeFihlifNhUQY"


# ログ設定
logger = logging.getLogger("user_activity")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    "user_activity.log",
    maxBytes=10485760,  # 10MB
    backupCount=10
)
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# ログを記録する依存関係関数
async def log_request(request: Request):
    # 現在時刻を取得
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # ユーザーIDを取得（ヘッダーにない場合は "unknown"）
    user_id = request.headers.get("X-User-ID", "unknown")
    
    # エンドポイントのパス
    path = request.url.path
    
    # リクエストパラメータを取得
    params = ""
    
    # GETパラメータを取得
    query_params = dict(request.query_params)
    if query_params:
        params += json.dumps(query_params)
    
    # POSTボディを取得（可能な場合）
    if request.method in ["POST", "PUT"]:
        try:
            # リクエストのボディを複製して内容を確認する
            body_bytes = await request.body()
            
            # ボディがあれば処理する
            if body_bytes:
                try:
                    body = json.loads(body_bytes.decode())
                    if params:
                        params += " "
                    params += json.dumps(body)
                except:
                    if params:
                        params += " "
                    params += "(non-JSON payload)"
            
            # リクエストボディを再度利用可能にする (FastAPIの内部メカニズム)
            request._body = body_bytes
        except:
            if params:
                params += " "
            params += "(body not available)"
    
    # ログ出力 - カンマ区切りのフォーマット
    log_message = f"{timestamp},{user_id},{path},{params}"
    logger.info(log_message)
    
    # 依存関係関数なので何も返さなくてOK
    return

# セッションを取得するための依存関係
def get_session():
    with Session(engine) as session:
        yield session

# router定義 - すべてのエンドポイントにログ依存関係を適用
router = APIRouter(dependencies=[Depends(log_request)])

class SQLQuery(BaseModel):
    sql: str


def send_webhook_notification(title, status, details, success=True):
    """
    Logic Apps Workflowにwebhook通知を送信
    """
    try:
        payload = {
            "title": title,
            "status": status,
            "success": success,
            "timestamp": datetime.datetime.now().isoformat(),
            "details": details,
            "source": "Pallet Cloud Export API"
        }
        
        response = requests.post(
            LOGIC_APPS_WEBHOOK_URL,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        print(f"Webhook sent successfully. Status: {response.status_code}")
        return True
        
    except Exception as e:
        print(f"Failed to send webhook: {str(e)}")
        return False
    

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

@router.post('/gcp/pallet-cloud')
def export_to_pallet_cloud():
    today = datetime.datetime.now().strftime("%Y%m%d")
    
    try:
        # 出力先のS3バケットとプレフィックス
        bucket_name = "adi-external-integration"
        prefix = "pallet-cloud/prod/"
        
        # S3クライアントの初期化
        s3_client = boto3.client('s3')
        
        # エクスポートするテーブルとファイル名のマッピング
        table_file_mapping = {
            "PC_buildings_output": f"{today}_PC_buildings",
            "PC_rooms_output": f"{today}_PC_rooms",
            "PC_tenants_output": f"{today}_PC_tenants",
            "PC_contract2": f"{today}_PC_contract2",
            "PC_contract_tenant": f"{today}_PC_contract_tenant",
            "PC_contract_resident": f"{today}_PC_contract_resident"
        }
        
        results = {}
        total_rows = 0
        
        # 各テーブルのデータをクエリしてS3にエクスポート
        for table_name, file_name in table_file_mapping.items():
            try:
                print(f"Processing table: {table_name}")
                
                # BigQueryからデータを取得
                query = f"""
                SELECT *
                FROM `ard-itandi-production.shared_ard_adi_view.{table_name}`
                """
                
                query_job = client.query(query)
                df = query_job.to_dataframe()
                
                # DataFrameをCSVに変換 (UTF-8エンコーディングを指定)
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                
                # S3にアップロード (UTF-8エンコーディングを明示的に指定)
                s3_key = f"{prefix}{file_name}.csv"
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=csv_buffer.getvalue().encode('utf-8'),
                    ContentType='text/csv; charset=utf-8'
                )
                
                row_count = len(df)
                total_rows += row_count
                
                results[table_name] = {
                    "status": "success",
                    "rows": row_count,
                    "destination": f"s3://{bucket_name}/{s3_key}"
                }
                
                print(f"Successfully exported {table_name}: {row_count} rows")
                
            except Exception as table_error:
                print(f"Error processing table {table_name}: {str(table_error)}")
                results[table_name] = {
                    "status": "error",
                    "error": str(table_error),
                    "rows": 0
                }
        
        # 成功時のwebhook通知を送信
        success_details = {
            "export_date": today,
            "total_files": len(table_file_mapping),
            "total_rows": total_rows,
            "file_details": [
                {
                    "table": table_name,
                    "filename": f"{table_file_mapping[table_name]}.csv",
                    "rows": result.get("rows", 0),
                    "status": result.get("status", "unknown")
                }
                for table_name, result in results.items()
            ],
            "s3_bucket": bucket_name,
            "s3_prefix": prefix
        }
        
        # エラーがあったかチェック
        failed_tables = [table for table, result in results.items() if result.get("status") == "error"]
        
        if failed_tables:
            # 一部失敗した場合
            webhook_title = "パレットクラウド向けファイル出力 - 一部エラー"
            webhook_status = f"完了（{len(failed_tables)}件のエラーあり）"
            success_details["failed_tables"] = failed_tables
            send_webhook_notification(webhook_title, webhook_status, success_details, success=False)
        else:
            # 全て成功した場合
            webhook_title = "パレットクラウド向けファイル出力 - 完了"
            webhook_status = f"全{len(table_file_mapping)}ファイル正常出力（計{total_rows:,}件）"
            send_webhook_notification(webhook_title, webhook_status, success_details, success=True)
        
        return {
            "message": "データのエクスポートが完了しました",
            "date": today,
            "total_rows": total_rows,
            "results": results
        }
        
    except (GoogleAPICallError, NotFound) as e:
        error_message = f"BigQuery Error: {str(e)}"
        print(error_message)
        
        # BigQueryエラー時のwebhook通知
        error_details = {
            "export_date": today,
            "error_type": "BigQuery Error",
            "error_message": str(e),
            "tables_attempted": list(table_file_mapping.keys())
        }
        
        send_webhook_notification(
            "パレットクラウド向けファイル出力 - BigQueryエラー",
            "BigQueryアクセスに失敗しました",
            error_details,
            success=False
        )
        
        raise HTTPException(status_code=400, detail=str(e))
        
    except Exception as e:
        error_message = f"Unexpected Error: {str(e)}"
        print(error_message)
        
        # 予期しないエラー時のwebhook通知
        error_details = {
            "export_date": today,
            "error_type": "Unexpected Error",
            "error_message": str(e),
            "tables_attempted": list(table_file_mapping.keys())
        }
        
        send_webhook_notification(
            "パレットクラウド向けファイル出力 - システムエラー",
            "予期しないエラーが発生しました",
            error_details,
            success=False
        )
        
        raise HTTPException(status_code=500, detail=str(e))

class DaysAgoParams(BaseModel):
    days_ago: int = 0

@router.post('/gcp/pallet-cloud/rooms-diff')
def rooms_diff(params: DaysAgoParams = None):
    try:
        # パラメータがない場合はデフォルト値を使用
        days_ago = 0 if params is None else params.days_ago
        
        # 現在の日付と指定された日数前の日付をyyyymmdd形式で取得
        today = datetime.datetime.now()
        base_date = today - datetime.timedelta(days=days_ago)
        yesterday = base_date - datetime.timedelta(days=1)
        today_str = base_date.strftime("%Y%m%d")
        yesterday_str = yesterday.strftime("%Y%m%d")
        
        # S3バケットとプレフィックス
        bucket_name = "adi-external-integration"
        prefix = "pallet-cloud/prod/"
        
        # 今日と昨日のファイルパス
        today_file = f"{today_str}_PC_rooms.csv"
        yesterday_file = f"{yesterday_str}_PC_rooms.csv"
        
        today_s3_path = f"{prefix}{today_file}"
        yesterday_s3_path = f"{prefix}{yesterday_file}"
        
        # S3クライアントの初期化
        s3_client = boto3.client('s3')
        
        # 一時ファイルパス
        temp_today_file = f"/tmp/{today_file}"
        temp_yesterday_file = f"/tmp/{yesterday_file}"
        
        # S3からファイルをダウンロード
        try:
            s3_client.download_file(bucket_name, today_s3_path, temp_today_file)
            print(f"Downloaded today's file from s3://{bucket_name}/{today_s3_path}")
        except Exception as e:
            print(f"Error downloading today's file: {e}")
            raise HTTPException(status_code=404, detail=f"Today's file not found: {today_file}")
            
        try:
            s3_client.download_file(bucket_name, yesterday_s3_path, temp_yesterday_file)
            print(f"Downloaded yesterday's file from s3://{bucket_name}/{yesterday_s3_path}")
        except Exception as e:
            print(f"Error downloading yesterday's file: {e}")
            raise HTTPException(status_code=404, detail=f"Yesterday's file not found: {yesterday_file}")
        
        # CSVファイルをDataFrameに読み込む
        today_df = pd.read_csv(temp_today_file, encoding='utf-8')
        yesterday_df = pd.read_csv(temp_yesterday_file, encoding='utf-8')
        
        print(f"Today's file has {len(today_df)} rows")
        print(f"Yesterday's file has {len(yesterday_df)} rows")
        
        # 主キーを特定（32番目のカラムを主キーとする）
        if len(today_df.columns) > 31:  # 0-indexedなので31が32番目
            primary_key = today_df.columns[31]
            
            # 差分を計算
            # 1. 今日のファイルにしかない行を抽出
            only_in_today = today_df[~today_df[primary_key].isin(yesterday_df[primary_key])]
            
            # 2. 両方のファイルに存在する行を抽出
            common_keys = today_df[today_df[primary_key].isin(yesterday_df[primary_key])][primary_key]
            
            # 3. 共通のキーを持つが値が異なる行を抽出
            diff_rows = []
            for key in common_keys:
                today_row = today_df[today_df[primary_key] == key]
                yesterday_row = yesterday_df[yesterday_df[primary_key] == key]
                
                # 行の値を比較（数値項目については1以上の差分がなければ差なしとする）
                is_different = False
                
                # 比較から除外するカラム（インデックス）
                exclude_indices = [19]
                
                # 各カラムを比較
                for i, col in enumerate(today_row.columns):
                    # 除外インデックスはスキップ
                    if i in exclude_indices:
                        continue
                        
                    today_val = today_row[col].iloc[0]
                    yesterday_val = yesterday_row[col].iloc[0]
                    
                    # 数値型の場合は差分が1以上あるかチェック
                    if pd.api.types.is_numeric_dtype(today_row[col]) and pd.api.types.is_numeric_dtype(yesterday_row[col]):
                        if pd.notna(today_val) and pd.notna(yesterday_val):
                            if abs(float(today_val) - float(yesterday_val)) >= 1:
                                is_different = True
                                break
                        elif pd.notna(today_val) != pd.notna(yesterday_val):  # 片方がNaNの場合
                            is_different = True
                            break
                    # 数値型以外は完全一致でチェック
                    elif today_val != yesterday_val:
                        is_different = True
                        break
                
                if is_different:
                    diff_rows.append(today_row)
            
            if diff_rows:
                changed_rows = pd.concat(diff_rows)
            else:
                changed_rows = pd.DataFrame(columns=today_df.columns)
            
            # 4. 差分ファイルを作成（今日にしかない行 + 変更された行）
            diff_df = pd.concat([only_in_today, changed_rows])
            
            # 差分ファイルをCSVに変換
            csv_buffer = io.StringIO()
            diff_df.to_csv(csv_buffer, index=False)
            
            # S3に出力（UTF-8エンコーディングを明示的に指定）
            output_s3_key = f"{prefix}output/room.csv"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=output_s3_key,
                Body=csv_buffer.getvalue().encode('utf-8'),
                ContentType='text/csv; charset=utf-8'
            )
            
            # ローカルにも保存
            local_dir = f"./pallet_cloud/{today_str}"
            os.makedirs(local_dir, exist_ok=True)
            local_file_path = f"{local_dir}/room.csv"
            
            # CSVファイルを保存
            with open(local_file_path, 'w', encoding='utf-8') as f:
                f.write(csv_buffer.getvalue())
            
            print(f"Saved local file to: {local_file_path}")
            
            output_s3_path = f"s3://{bucket_name}/{output_s3_key}"
            
            return {
                "message": "部屋ファイルの差分計算が完了しました",
                "today_date": today_str,
                "yesterday_date": yesterday_str,
                "total_rows_today": len(today_df),
                "total_rows_yesterday": len(yesterday_df),
                "new_rows": len(only_in_today),
                "changed_rows": len(changed_rows),
                "diff_rows": len(diff_df),
                "output_file": output_s3_path
            }
        else:
            raise HTTPException(status_code=400, detail="ファイルに32番目のカラムがありません")
            
    except (GoogleAPICallError, NotFound) as e:
        print(f"BigQuery Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@router.post('/gcp/pallet-cloud/contract2-diff')
def contract2_diff(params: DaysAgoParams = None):
    try:
        # パラメータがない場合はデフォルト値を使用
        days_ago = 0 if params is None else params.days_ago
        
        # 現在の日付と指定された日数前の日付をyyyymmdd形式で取得
        today = datetime.datetime.now()
        base_date = today - datetime.timedelta(days=days_ago)
        yesterday = base_date - datetime.timedelta(days=1)
        today_str = base_date.strftime("%Y%m%d")
        yesterday_str = yesterday.strftime("%Y%m%d")
        
        # S3バケットとプレフィックス
        bucket_name = "adi-external-integration"
        prefix = "pallet-cloud/prod/"
        
        # 今日と昨日のファイルパス
        today_file = f"{today_str}_PC_contract2.csv"
        yesterday_file = f"{yesterday_str}_PC_contract2.csv"
        
        today_s3_path = f"{prefix}{today_file}"
        yesterday_s3_path = f"{prefix}{yesterday_file}"
        
        # S3クライアントの初期化
        s3_client = boto3.client('s3')
        
        # 一時ファイルパス
        temp_today_file = f"/tmp/{today_file}"
        temp_yesterday_file = f"/tmp/{yesterday_file}"
        
        # S3からファイルをダウンロード
        try:
            s3_client.download_file(bucket_name, today_s3_path, temp_today_file)
            print(f"Downloaded today's file from s3://{bucket_name}/{today_s3_path}")
        except Exception as e:
            print(f"Error downloading today's file: {e}")
            raise HTTPException(status_code=404, detail=f"Today's file not found: {today_file}")
            
        try:
            s3_client.download_file(bucket_name, yesterday_s3_path, temp_yesterday_file)
            print(f"Downloaded yesterday's file from s3://{bucket_name}/{yesterday_s3_path}")
        except Exception as e:
            print(f"Error downloading yesterday's file: {e}")
            raise HTTPException(status_code=404, detail=f"Yesterday's file not found: {yesterday_file}")
        
        # CSVファイルをDataFrameに読み込む
        today_df = pd.read_csv(temp_today_file, encoding='utf-8')
        yesterday_df = pd.read_csv(temp_yesterday_file, encoding='utf-8')
        
        print(f"Today's file has {len(today_df)} rows")
        print(f"Yesterday's file has {len(yesterday_df)} rows")
        
        # 主キーを特定（1列目のカラムを主キーとする）
        if len(today_df.columns) > 0:
            primary_key = today_df.columns[0]
            
            # 差分を計算
            # 1. 今日のファイルにしかない行を抽出
            only_in_today = today_df[~today_df[primary_key].isin(yesterday_df[primary_key])]
            
            # 2. 両方のファイルに存在する行を抽出
            common_keys = today_df[today_df[primary_key].isin(yesterday_df[primary_key])][primary_key]
            
            # 3. 共通のキーを持つが値が異なる行を抽出
            diff_rows = []
            for key in common_keys:
                today_row = today_df[today_df[primary_key] == key]
                yesterday_row = yesterday_df[yesterday_df[primary_key] == key]
                
                # 行の値を比較（数値項目については1以上の差分がなければ差なしとする）
                is_different = False
                
                # 各カラムを比較
                for col in today_row.columns:
                    today_val = today_row[col].iloc[0]
                    yesterday_val = yesterday_row[col].iloc[0]
                    
                    # 数値型の場合は差分が1以上あるかチェック
                    if pd.api.types.is_numeric_dtype(today_row[col]) and pd.api.types.is_numeric_dtype(yesterday_row[col]):
                        if pd.notna(today_val) and pd.notna(yesterday_val):
                            if abs(float(today_val) - float(yesterday_val)) >= 1:
                                is_different = True
                                break
                        elif pd.notna(today_val) != pd.notna(yesterday_val):  # 片方がNaNの場合
                            is_different = True
                            break
                    # 数値型以外は完全一致でチェック
                    elif today_val != yesterday_val:
                        is_different = True
                        break
                
                if is_different:
                    diff_rows.append(today_row)
            
            if diff_rows:
                changed_rows = pd.concat(diff_rows)
            else:
                changed_rows = pd.DataFrame(columns=today_df.columns)
            
            # 4. 差分ファイルを作成（今日にしかない行 + 変更された行）
            diff_df = pd.concat([only_in_today, changed_rows])
            
            # 差分ファイルをCSVに変換
            csv_buffer = io.StringIO()
            diff_df.to_csv(csv_buffer, index=False)
            
            # S3に出力（UTF-8エンコーディングを明示的に指定）
            output_s3_key = f"{prefix}output/contract2.csv"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=output_s3_key,
                Body=csv_buffer.getvalue().encode('utf-8'),
                ContentType='text/csv; charset=utf-8'
            )
            
            # ローカルにも保存
            local_dir = f"./pallet_cloud/{today_str}"
            os.makedirs(local_dir, exist_ok=True)
            local_file_path = f"{local_dir}/contract2.csv"
            
            # CSVファイルを保存
            with open(local_file_path, 'w', encoding='utf-8') as f:
                f.write(csv_buffer.getvalue())
            
            print(f"Saved local file to: {local_file_path}")
            
            output_s3_path = f"s3://{bucket_name}/{output_s3_key}"
            
            return {
                "message": "契約ファイルの差分計算が完了しました",
                "today_date": today_str,
                "yesterday_date": yesterday_str,
                "total_rows_today": len(today_df),
                "total_rows_yesterday": len(yesterday_df),
                "new_rows": len(only_in_today),
                "changed_rows": len(changed_rows),
                "diff_rows": len(diff_df),
                "output_file": output_s3_path
            }
        else:
            raise HTTPException(status_code=400, detail="ファイルにカラムがありません")
            
    except (GoogleAPICallError, NotFound) as e:
        print(f"BigQuery Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post('/gcp/pallet-cloud/building-diff')
def building_diff(params: DaysAgoParams = None):
    try:
        # パラメータがない場合はデフォルト値を使用
        days_ago = 0 if params is None else params.days_ago
        
        # 現在の日付と指定された日数前の日付をyyyymmdd形式で取得
        today = datetime.datetime.now()
        base_date = today - datetime.timedelta(days=days_ago)
        yesterday = base_date - datetime.timedelta(days=1)
        today_str = base_date.strftime("%Y%m%d")
        yesterday_str = yesterday.strftime("%Y%m%d")
        
        # S3バケットとプレフィックス
        bucket_name = "adi-external-integration"
        prefix = "pallet-cloud/prod/"
        
        # 今日と昨日のファイルパス
        today_file = f"{today_str}_PC_buildings.csv"
        yesterday_file = f"{yesterday_str}_PC_buildings.csv"
        
        today_s3_path = f"{prefix}{today_file}"
        yesterday_s3_path = f"{prefix}{yesterday_file}"
        
        # S3クライアントの初期化
        s3_client = boto3.client('s3')
        
        # 一時ファイルパス
        temp_today_file = f"/tmp/{today_file}"
        temp_yesterday_file = f"/tmp/{yesterday_file}"
        
        # S3からファイルをダウンロード
        try:
            s3_client.download_file(bucket_name, today_s3_path, temp_today_file)
            print(f"Downloaded today's file from s3://{bucket_name}/{today_s3_path}")
        except Exception as e:
            print(f"Error downloading today's file: {e}")
            raise HTTPException(status_code=404, detail=f"Today's file not found: {today_file}")
            
        try:
            s3_client.download_file(bucket_name, yesterday_s3_path, temp_yesterday_file)
            print(f"Downloaded yesterday's file from s3://{bucket_name}/{yesterday_s3_path}")
        except Exception as e:
            print(f"Error downloading yesterday's file: {e}")
            raise HTTPException(status_code=404, detail=f"Yesterday's file not found: {yesterday_file}")
        
        # CSVファイルをDataFrameに読み込む
        today_df = pd.read_csv(temp_today_file, encoding='utf-8')
        yesterday_df = pd.read_csv(temp_yesterday_file, encoding='utf-8')
        
        print(f"Today's file has {len(today_df)} rows")
        print(f"Yesterday's file has {len(yesterday_df)} rows")
        
        # 主キーを特定（2カラム目を主キーとする）
        if len(today_df.columns) > 1:
            primary_key = today_df.columns[1]
            
            # 差分を計算
            # 1. 今日のファイルにしかない行を抽出
            only_in_today = today_df[~today_df[primary_key].isin(yesterday_df[primary_key])]
            
            # 2. 両方のファイルに存在する行を抽出
            common_keys = today_df[today_df[primary_key].isin(yesterday_df[primary_key])][primary_key]
            
            # 3. 共通のキーを持つが値が異なる行を抽出
            diff_rows = []
            diff_details = []  # 差分の詳細を記録するリスト
            
            for key in common_keys:
                today_row = today_df[today_df[primary_key] == key]
                yesterday_row = yesterday_df[yesterday_df[primary_key] == key]
                
                # 行の値を比較（数値項目については1以上の差分がなければ差なしとする）
                is_different = False
                row_diff_details = []  # この行の差分詳細
                
                # 比較から除外するカラム（インデックス）
                exclude_indices = [60, 61]
                
                # 各カラムを比較
                for i, col in enumerate(today_row.columns):
                    # 除外インデックスはスキップ
                    if i in exclude_indices:
                        continue
                        
                    today_val = today_row[col].iloc[0]
                    yesterday_val = yesterday_row[col].iloc[0]
                    
                    # 数値型の場合は差分が1以上あるかチェック
                    if pd.api.types.is_numeric_dtype(today_row[col]) and pd.api.types.is_numeric_dtype(yesterday_row[col]):
                        if pd.notna(today_val) and pd.notna(yesterday_val):
                            diff_value = abs(float(today_val) - float(yesterday_val))
                            if diff_value >= 1:
                                is_different = True
                                row_diff_details.append({
                                    "column": col,
                                    "today_value": today_val,
                                    "yesterday_value": yesterday_val,
                                    "difference": diff_value
                                })
                        elif pd.notna(today_val) != pd.notna(yesterday_val):  # 片方がNaNの場合
                            is_different = True
                            row_diff_details.append({
                                "column": col,
                                "today_value": "NaN" if pd.isna(today_val) else today_val,
                                "yesterday_value": "NaN" if pd.isna(yesterday_val) else yesterday_val,
                                "difference": "NaN comparison"
                            })
                    # 数値型以外は完全一致でチェック
                    elif today_val != yesterday_val:
                        is_different = True
                        row_diff_details.append({
                            "column": col,
                            "today_value": today_val,
                            "yesterday_value": yesterday_val,
                            "difference": "String difference"
                        })
                
                if is_different:
                    diff_rows.append(today_row)
                    # キーと差分詳細を記録
                    diff_details.append({
                        "key": key,
                        "differences": row_diff_details
                    })
            
            if diff_rows:
                changed_rows = pd.concat(diff_rows)
            else:
                changed_rows = pd.DataFrame(columns=today_df.columns)
            
            # 4. 差分ファイルを作成（今日にしかない行 + 変更された行）
            diff_df = pd.concat([only_in_today, changed_rows])
            
            # 差分ファイルをCSVに変換
            csv_buffer = io.StringIO()
            diff_df.to_csv(csv_buffer, index=False)
            
            # S3に出力（UTF-8エンコーディングを明示的に指定）
            output_s3_key = f"{prefix}output/building.csv"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=output_s3_key,
                Body=csv_buffer.getvalue().encode('utf-8'),
                ContentType='text/csv; charset=utf-8'
            )
            
            # ローカルにも保存
            local_dir = f"./pallet_cloud/{today_str}"
            os.makedirs(local_dir, exist_ok=True)
            local_file_path = f"{local_dir}/building.csv"
            
            # CSVファイルを保存
            with open(local_file_path, 'w', encoding='utf-8') as f:
                f.write(csv_buffer.getvalue())
            
            # 差分詳細をテキストファイルに出力
            diff_result_path = f"{local_dir}/building-diff-result.txt"
            with open(diff_result_path, 'w', encoding='utf-8') as f:
                f.write(f"Building Diff Results - {today_str} vs {yesterday_str}\n")
                f.write(f"Total rows in today's file: {len(today_df)}\n")
                f.write(f"Total rows in yesterday's file: {len(yesterday_df)}\n")
                f.write(f"New rows (only in today's file): {len(only_in_today)}\n")
                f.write(f"Changed rows: {len(changed_rows)}\n\n")
                
                if len(only_in_today) > 0:
                    f.write("=== NEW ROWS ===\n")
                    for _, row in only_in_today.iterrows():
                        f.write(f"New row with key: {row[primary_key]}\n")
                    f.write("\n")
                
                if len(diff_details) > 0:
                    f.write("=== CHANGED ROWS ===\n")
                    for row_diff in diff_details:
                        f.write(f"Row with key '{row_diff['key']}' has the following differences:\n")
                        for diff in row_diff['differences']:
                            f.write(f"  Column '{diff['column']}':\n")
                            f.write(f"    Today's value: {diff['today_value']}\n")
                            f.write(f"    Yesterday's value: {diff['yesterday_value']}\n")
                            f.write(f"    Difference: {diff['difference']}\n")
                        f.write("\n")
                else:
                    f.write("No differences found in existing rows.\n")
            
            print(f"Saved local file to: {local_file_path}")
            print(f"Saved diff details to: {diff_result_path}")
            
            output_s3_path = f"s3://{bucket_name}/{output_s3_key}"
            
            return {
                "message": "建物ファイルの差分計算が完了しました",
                "today_date": today_str,
                "yesterday_date": yesterday_str,
                "total_rows_today": len(today_df),
                "total_rows_yesterday": len(yesterday_df),
                "new_rows": len(only_in_today),
                "changed_rows": len(changed_rows),
                "diff_rows": len(diff_df),
                "output_file": output_s3_path
            }
        else:
            raise HTTPException(status_code=400, detail="ファイルにカラムがありません")
            
    except (GoogleAPICallError, NotFound) as e:
        print(f"BigQuery Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
