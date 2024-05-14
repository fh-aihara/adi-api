from base64 import decode
from lib2to3.pgen2 import token
from pymysql import NULL
from fastapi import APIRouter, Header, Depends, HTTPException, status, Response, UploadFile
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi import FastAPI, Request
from fastapi.routing import APIRoute
from database import create_db_and_tables, engine
from models import users, age_verifications, user_photos, chat_room_user, chat_rooms, friends, messages, states, theme, \
events, participant_group, participants, user_opinions, areas, admin_users, blocks, reviews
from sqlalchemy import null
from sqlmodel import Field, SQLModel, create_engine, Session, select, literal_column, table, desc
from typing import Union
import firebase_admin
from firebase_admin import credentials, auth, messaging
import json
import requests
from cachecontrol import CacheControl 
from cachecontrol.caches import FileCache
import datetime
from jose import jwt
import os
import time
import logging
import random, string
import boto3
import hashlib
import tempfile
from pathlib import Path

# google calendar
from googleapiclient.discovery import build
from google.auth import load_credentials_from_file
from google.oauth2 import service_account



# # log
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s:%(name)s - %(message)s", filename="userapi.log")

# # firebase admin
# JSON_PATH = '../hanabi-ee185-firebase-adminsdk-3ezuk-ad35ec06cd.key'

# aws setting
access_key = os.getenv("ACCESS_KEY")
access_secret = os.getenv("ACCESS_SECRET")
s3 = boto3.client('s3',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=access_secret)
bucket_name_public = 'hanabi-public'
bucket_name_private = 'hanabi-private'

# google calender
gc_SCOPES = ['https://www.googleapis.com/auth/calendar']
gc_credentials = service_account.Credentials.from_service_account_file(
    './hanabi-ee185-02cd044cbabf.json',
    scopes=gc_SCOPES,
    subject='calender@hanabi.tech') # なり代わり
gc_service = build('calendar', 'v3', credentials=gc_credentials)

# サーバー稼働環境
server_environment = os.getenv("ENVIRONMENT")

# http response list
http_response_details = {
    200: "OK",
    400: "Bad Request",
    403: "Forbidden",
    404: "Not Found",
    500: "Internal Server Error"
}

hanabi_unei_user_id = 71 # 運営のユーザーIDは71

# カスタムルーター
class LoggingContextRoute(APIRoute):
    def get_route_handler(self):
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            # before = time.time()
            response: Response = await original_route_handler(request)
            # duration = round(time.time() - before, 4)

            record = {}
            response_message = ""
            response_message += "稼働環境: " + str(server_environment) + "\n"
            
            record["request_uri"] = request.url.path
            record["request_method"] = request.method
            record["status"] = response.status_code
            response_message += str(request.method) + " " + str(request.url.path) + " " + str(response.status_code) + "\n"
            
            if await request.body():
                body = (await request.body()).decode("utf-8")
                record["request_body"] = body
                response_message += "request: " + str(body) + "\n"
            
            record["response_body"] = response.body.decode("utf-8")
            response_message += "response: " + str(response.body.decode("utf-8"))
            
            # if record["request_uri"] in []: # 対象のURLのみnoticeする
            #     send_slack(response_message)
            # else:
            #     pass
            return response
        return custom_route_handler
    
# router = APIRouter(route_class=LoggingContextRoute)
router = APIRouter()


# 内部関数 ****
def send_slack(message):
    web_hook_url = "https://hooks.slack.com/services/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" # monitoring-adminapi
    message = str(message)
    requests.post(web_hook_url, data = json.dumps({'text': message}))


def send_slack_kyc(message):
    web_hook_url = "https://hooks.slack.com/services/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" # kyc
    message = str(message)
    requests.post(web_hook_url, data = json.dumps({'text': message}))


# push通知送信
def send_push(item: dict):
    """
    push対象のuser_id: int
    text: str
    """
    with Session(engine) as session:
        user_id = item["user_id"]
        title = "hanabi"
        text = item["text"]
        target = session.exec(select(users).where(users.user_id == user_id)).one()
        firebase_token = target.firebase_token
        if firebase_token is None: # push通知をOFFにしている人
            return "OK no push"
        else: # push通知OKの人
            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=text,
                ),
                token=firebase_token,
            )
            # メッセージの送信
            try:
                response = messaging.send(message)
                print(response)
            except Exception as e:
                print(e)
            return "OK pushed"


def send_message_from_hanabi(item: dict):
    """
    push対象のroom_id: int
    text: str
    """
    with Session(engine) as session:
        room_id = item["room_id"]
        text = item["text"]
        statement = f"INSERT INTO messages (user_id, room_id, text) VALUES({hanabi_unei_user_id}, {room_id}, '{text}');"
        session.exec(statement)
        session.commit()
        
        statement = f"SELECT * FROM chat_room_user WHERE room_id = {room_id} AND deleted_at is NULL"
        message_to = session.exec(statement).all()
        for user in message_to:
            send_push({"user_id": user["user_id"],
                        "text": f"hanabiからメッセージが届きました"})
        return "OK DMed"


# init 処理
@router.on_event("startup")
def on_startup():
    create_db_and_tables() 
    # firebaseのインスタンスはmain配下のどこかで入っていればOK
    # cred = credentials.Certificate(JSON_PATH)
    # firebase_admin.initialize_app(cred)
       

@router.get('/', tags=["utils"])
def health_check():
    """
    サーバーヘルスチェック
    """
    return ('200 OK')


# idTokenのAuthentication
@router.get('/verify-token' ,tags=["utils"])
def userid_from_token(Authorization: Union[str, None] = Header(default=None)):
    """
    ユーザーのPWとトークンを連結した文字列をhash化したものを受け取って、あっていればユーザーIDを返す
    """
    if Authorization == None:
        raise HTTPException(status_code=400, detail=http_response_details[400])
    else:
        with Session(engine) as session:
            statement = f"SELECT * FROM admin_users WHERE auth_token = '{Authorization}' AND deleted_at is NULL"
            try:
                user = session.exec(statement).one()
                return user.admin_id
            except:
                raise HTTPException(status_code=400, detail=http_response_details[400])


@router.post('/login', tags=["utils"])
def admin_login(item: dict):
    """
    ログインする。認可コードを返却する
    """
    with Session(engine) as session:
        try:
            print("login")
            password_hash = item["password"]
            username = item["name"]
            statement = f"SELECT * FROM admin_users WHERE deleted_at is NULL AND password_hash = '{password_hash}' AND `name` = '{username}'"
            user = session.exec(statement).one() # 必ず一件ヒットする
            return "se5SZHLq3GCBexZb9rZ0r1egf7q2AvRD"
        except:
            raise HTTPException(status_code=400, detail=http_response_details[400])
    

# for batch
# 条件を確認してイベントをキャンセルする処理。不要になったがせっかく書いたので残しておく
# @router.post('/events/update-status-48', tags=["events"])
# def update_status_48(item: dict, admin_user = Depends(userid_from_token)):
#     """
#     body: {}
#     イベントの中で開催時間の48時間前を過ぎたものに対して処理を行う
#     """
#     with Session(engine) as session:
#         now = datetime.datetime.now()
#         now_48 = now - datetime.timedelta(days=2)
#         now = now.strftime('%Y-%m-%d %H:%M:%S')
#         now_48 = now_48.strftime('%Y-%m-%d %H:%M:%S')
#         statement = f"SELECT e.*, up.image_url AS owner_image_url, u.age AS owner_age, u.gender AS owner_gender, u.working_type AS owner_working_type, u.user_id AS owner_user_id, p.user_id AS participants_user_id, p.image_url AS participants_image_url, p.status AS participants_status FROM events AS e \
#                      LEFT JOIN user_photos AS up ON e.owner_user_id = up.user_id AND up.is_main = 1 AND up.deleted_at IS NULL\
#                      LEFT JOIN users AS u ON e.owner_user_id = u.user_id\
#                      LEFT JOIN (SELECT up.image_url AS image_url, p.event_id, p.user_id, p.status FROM participants AS p \
#                                 LEFT JOIN user_photos AS up ON p.user_id = up.user_id AND up.is_main = 1 AND up.deleted_at IS NULL WHERE p.status IN (0,1)) AS p ON p.event_id = e.event_id\
#                      WHERE e.deleted_at is NULL AND e.status = 0 AND e.start_at < '{now_48}' ORDER BY e.start_at ASC , e.event_id ASC"
#         events = session.exec(statement).all()
#         return_list = []
#         before_event_id = 0
#         for event in events:
#             # 新しいイベントのwindowに入ったらまず基本情報を全て入れる
#             if before_event_id != event.event_id:
#                 _ = {}
#                 _participants = []
#                 _participant = {}
#                 _["event_id"] = event.event_id
#                 _["theme"] = event.theme
#                 _["states"] = event.states
#                 _["status"] = event.states
#                 _["area"] = event.area
#                 _["start_at"] = event.start_at
#                 _["name"] = event.name
#                 _["payment_ratio"] = event.payment_ratio
#                 _["male_estimated_payment"] = event.male_estimated_payment
#                 _["female_estimated_payment"] = event.female_estimated_payment
#                 _["message_from_owner"] = event.message_from_owner
#                 _["owner_user_id"] = event.owner_user_id
#                 _["deleted_at"] = event.deleted_at
#                 _["created_at"] = event.created_at
#                 _["updated_at"] = event.updated_at
#                 _["participants_pattern"] = event.participants_pattern
#                 _["restaurant_url"] = event.restaurant_url
#                 _["male_participants_number"] = event.male_participants_number
#                 _["female_participants_number"] = event.female_participants_number
#                 _["owner_image_url"] = event.owner_image_url
#                 _["owner_age"] = event.owner_age
#                 _["owner_gender"] = event.owner_gender
#                 _["owner_working_type"] = event.owner_working_type
#                 if event.participants_user_id is not None:
#                     _participant["user_id"] = event.participants_user_id
#                     _participant["image_url"] = event.participants_image_url
#                     _participant["status"] = event.participants_status
#                     _participants.append(_participant) # dictをlistに入れる
#                 _["participants"] = _participants # listを大元のdictに入れる
#                 return_list.append(_)
#             else: # 同じイベントIDが来る＝複数人のparticipantsがいる
#                 _participant = {}
#                 _participant["user_id"] = event.participants_user_id
#                 _participant["image_url"] = event.participants_image_url
#                 _participant["status"] = event.participants_status
#                 return_list[-1]["participants"].append(_participant) # return_listの最終行を更新する
#             before_event_id = event.event_id
            
#         # return listに必要情報が全て揃っている状態
#         result = []
#         for event in return_list:
#             required_participans_number = 0
#             """
#             1対1 0
#             2対2 1
#             3対3 2
#             4対4 3
#             """
#             if event["participants_pattern"] == 0:
#                 required_participans_number = 2
#             if event["participants_pattern"] == 1:
#                 required_participans_number = 4
#             if event["participants_pattern"] == 2:
#                 required_participans_number = 6
#             if event["participants_pattern"] == 3:
#                 required_participans_number = 8
                
#             print(required_participans_number)
#             confirmed_participants = 0
#             event_id = event["event_id"]
#             for participant in event["participants"]:
#                 if participant["status"] == 1: # approved
#                     confirmed_participants += 1
#             print(event_id, required_participans_number, confirmed_participants)
#             if required_participans_number == confirmed_participants:
#                 # 催行人数が揃っているので開催
#                 statement = f"UPDATE events SET `status` = 1 WHERE event_id = {event_id}"
#                 session.exec(statement)
#                 session.commit()
#                 result.append([event_id, "update_to_Confirmed"])
#             else:
#                 # 人数が揃っていないのでキャンセル
#                 statement = f"UPDATE events SET deleted_at = '{now}' WHERE event_id = {event_id}"
#                 session.exec(statement)
#                 session.commit()
#                 result.append([event_id, "update_to_Canceled"])
#         return result

@router.post('/events/update-status-72', tags=["events"])
def update_status_72(item: dict, admin_user = Depends(userid_from_token)):
    """
    body: {}
    イベントの中で開催時間の72時間前を過ぎたものに対して処理を行う
    """
    with Session(engine) as session:
        now = datetime.datetime.now()
        now_72 = now + datetime.timedelta(days=3)
        now = now.strftime('%Y-%m-%d %H:%M:%S')
        now_72 = now_72.strftime('%Y-%m-%d %H:%M:%S')
        statement = f"SELECT * FROM events WHERE deleted_at is NULL AND start_at < '{now_72}' AND `status` = 0"
        events = session.exec(statement).all()
        result = []
        for event in events:
            event_id = event["event_id"]
            statement = f"UPDATE events SET `status` = 1 WHERE event_id = {event_id}"
            session.exec(statement)
            session.commit()
            result.append([event_id, "update_to_participants_list_fixed"])
            
            # push文面作り
            if event["restaurant_url"] is None:
                send_push({"user_id": event["owner_user_id"],
                           "text": f"{event['name']}のレストラン登録期限が明日までとなります。本日中に、イベント詳細画面から、開催場所の入力をお願いいたします。"})
            
            # push文面作り
            statement = f"SELECT * FROM participants WHERE event_id = {event_id} AND `status` IN (0,1,2)" # 3はcanceled
            participants = session.exec(statement).all()
            for participant in participants:
                send_push({"user_id": participant["user_id"],
                           "text": f"明日が{event['name']}への参加キャンセル期限となります。"})            
        return result

@router.post('/events/update-status-48', tags=["events"])
def update_status_48(item: dict, admin_user = Depends(userid_from_token)):
    """
    body: {}
    イベントの中で開催時間の48時間前を過ぎたものに対して処理を行う
    """
    with Session(engine) as session:
        now = datetime.datetime.now()
        now_48 = now + datetime.timedelta(days=2) # 48時間
        now = now.strftime('%Y-%m-%d %H:%M:%S')
        now_48 = now_48.strftime('%Y-%m-%d %H:%M:%S')
        statement = f"SELECT * FROM events WHERE deleted_at is NULL AND start_at < '{now_48}' AND `status` = 1"
        events = session.exec(statement).all()
        result = []
        for event in events:
            event_id = event["event_id"]
            statement = f"UPDATE events SET `status` = 2 WHERE event_id = {event_id}"
            session.exec(statement)
            session.commit()
            result.append([event_id, "update_to_member_fixed"])
        return result


@router.post('/events/update-status-24', tags=["events"])
def update_status_24(item: dict, admin_user = Depends(userid_from_token)):
    """
    body: {}
    イベントの中で開催時間の24時間前を過ぎたものに対して処理を行う
    """
    with Session(engine) as session:
        now = datetime.datetime.now()
        now_24 = now + datetime.timedelta(days=1)
        now = now.strftime('%Y-%m-%d %H:%M:%S')
        now_24 = now_24.strftime('%Y-%m-%d %H:%M:%S')
        statement = f"SELECT * FROM events WHERE deleted_at is NULL AND start_at < '{now_24}' AND `status` = 2"
        events = session.exec(statement).all()
        result = []
        for event in events:
            event_id = event["event_id"]
            if len(event["restaurant_url"]) != 0:
                statement = f"UPDATE events SET `status` = 3 WHERE event_id = {event_id}"
                session.exec(statement)
                session.commit()
                result.append([event_id, "update_to_finalized"])
            else:
                print("You must add URL")
                # push文面作り
                send_push({"user_id": event["owner_user_id"],
                           "text": f"本日実施予定の{event['name']}の開催場所が未入力です。イベント詳細画面から、開催場所のURL入力をお願いいたします。"})
                result.append([event_id, "aleat"])
        return result


# push送る用
@router.post('/events/push-for-event', tags=["events"])
def push_for_event(item: dict, admin_user = Depends(userid_from_token)):
    """
    body: {}
    イベントまで24時間を切っているイベントに対して、がんばれメッセージを送る
    """
    with Session(engine) as session:
        now = datetime.datetime.now()
        now_24 = now + datetime.timedelta(days=1) # 24時間
        now = now.strftime('%Y-%m-%d %H:%M:%S')
        now_24 = now_24.strftime('%Y-%m-%d %H:%M:%S')
        statement = f"SELECT * FROM events WHERE deleted_at is NULL AND start_at < '{now_24}' AND `status` = 3"
        events = session.exec(statement).all()
        for event in events:
            # push文面作り
            statement = f"SELECT * FROM participants WHERE event_id = {event['event_id']} AND `status` IN (0,1,2)" # 3はcanceled
            participants = session.exec(statement).all()
            statement = f"SELECT * FROM chat_rooms WHERE event_id = {event['event_id']}"
            DM_to = session.exec(statement).one()
            for participant in participants:
                send_push({"user_id": participant["user_id"],
                           "text": f"本日が{event['name']}当日となります。楽しんで来てください！"})
            # DM文面作り
            send_message_from_hanabi({"room_id": DM_to["room_id"],
                                        "text": f"本日が{event['name']}当日となります。場所は {event['restaurant_url']} です。皆さん楽しく飲みましょう！"})
        return "pushed"
        

@router.post('/events/update-status-0', tags=["events"])
def update_status_0(item: dict, admin_user = Depends(userid_from_token)):
    """
    body: {}
    イベントの中で開催時間を過ぎたものに対して処理を行う
    """
    with Session(engine) as session:
        now = datetime.datetime.now()
        now = now.strftime('%Y-%m-%d %H:%M:%S')
        statement = f"SELECT * FROM events WHERE deleted_at is NULL AND start_at < '{now}' AND `status` IN (2,3)" # 2はお店のURLが入っていない状態で開始時刻を迎えたもの。このパターンもクローズするために4に変更する
        events = session.exec(statement).all()
        result = []
        for event in events:
            event_id = event["event_id"]
            statement = f"UPDATE events SET `status` = 4 WHERE event_id = {event_id}"
            session.exec(statement)
            session.commit()
            result.append([event_id, "update_to_finished"])
        return result


# DMを送るためだけの関数
@router.post('/events/archive_massage', tags=["events"])
def archive_massage(item: dict, admin_user = Depends(userid_from_token)):
    """
    body: {}
    イベントの中で開催時間を過ぎたものに対してメッセージを送る
    """
    with Session(engine) as session:
        # 終了=4 を 終了翌日以降 = 5に変更 かつ チャットルームが生きているものが対象
        # 処理は朝9時に稼働すると言うことを前提に処理ができている
        statement = f"UPDATE events SET `status` = 5 WHERE deleted_at is NULL AND `status` = 4"
        session.exec(statement)
        session.commit()
        
        statement = f"SELECT e.*, cr.room_id FROM events AS e\
                      JOIN chat_rooms AS cr ON e.event_id = cr.event_id AND cr.deleted_at is NULL \
                      WHERE e.deleted_at is NULL AND `status` = 5"
        events = session.exec(statement).all()
        for event in events:
            send_message_from_hanabi({"room_id": event["room_id"],
                                      "text": f"昨日はお疲れ様でした！このグループチャットは本日24時にアーカイブされます。引き続きhanabiをお楽しみください！"})
        return "OK"


@router.post('/events/close_finished_event_rooms', tags=["events"])
def close_finished_event_rooms(item: dict, admin_user = Depends(userid_from_token)):
    """
    body: {}
    開催後24時間を経過したルームを閉じる
    """
    with Session(engine) as session:
        now = datetime.datetime.now()
        now_24 = now - datetime.timedelta(days=1)
        now = now.strftime('%Y-%m-%d %H:%M:%S')
        now_24 = now_24.strftime('%Y-%m-%d %H:%M:%S')
        # チャットルームが消えてなくて、stats=4で24時間以上経過しているイベント
        statement = f"SELECT e.*, cr.room_id FROM events AS e\
                      JOIN chat_rooms AS cr ON e.event_id = cr.event_id AND cr.deleted_at is NULL\
                      WHERE e.deleted_at is NULL AND e.start_at < '{now_24}' AND e.status = 5"
        events = session.exec(statement).all()
        result = []
        for event in events:
            room_id = event["room_id"]
            statement = f"UPDATE chat_rooms SET deleted_at = '{now}' WHERE room_id = {room_id}"
            session.exec(statement)
            session.commit()
            result.append([room_id, "updated_room_closed"])
        return result


# for Admin Front

@router.get('/admin-user', tags=["admin"])
def get_admin_users(user_id = Depends(userid_from_token)):
    """
    JWTの電話番号と一致するユーザー情報を全て取得する
    """
    with Session(engine) as session:
        try:
            print(user_id)
            user = session.exec(select(admin_users).where(admin_users.admin_id == user_id)).one()
            return user
        except:
            raise HTTPException(status_code=403, detail=http_response_details[403])


@router.get('/events', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(events)).all()
            return res
        except:
            return 403


@router.post('/events', tags=["admin"])
def post_(item: events, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


@router.patch('/events/{id}', tags=["admin"])
def patch_(id, item: dict):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        target = session.exec(select(events).where(events.event_id == id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


@router.get('/users', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(users)).all()
            return res
        except:
            return 403


# @router.post('/users', tags=["admin"])
# def post_(item: users, user_id = Depends(userid_from_token)):
#     with Session(engine) as session:
#         try:
#             session.add(item)
#             session.commit()
#             session.refresh(item)
#             return item
#         except:
#             return 403


# 年齢を算出して返すAPI
def calculate_age(birthdate):
    # birthdate = datetime.datetime.strptime(birthdate, "%Y-%m-%d %H:%M:%S")
    current_date = datetime.datetime.now()  # 現在の日付を取得

    # 年齢を計算
    age = current_date.year - birthdate.year

    # 誕生日がまだ来ていない場合は、年齢から1を引く
    if (current_date.month, current_date.day) < (birthdate.month, birthdate.day):
        age -= 1

    return age


@router.post('/users/update-age', tags=["users"])
def update_age(item: dict, admin_user = Depends(userid_from_token)):
    """
    body: {}
    今日が誕生日の人の年齢を更新する（毎朝UTC 00:00に稼働前提）
    """
    with Session(engine) as session:
        print("age update")
        now = datetime.datetime.now()

        statement = f"SELECT * FROM users WHERE MONTH(birthday) = :month AND DAY(birthday) = :day AND deleted_at is NULL;"
        target_users = session.execute(statement, [{"month": now.month, "day": now.day}]).all()
        print(target_users)
        
        for target_user in target_users:
            new_age = calculate_age(target_user["birthday"])
            if new_age != target_user["age"]:
                print(new_age, target_user["age"])
                statement = "UPDATE users SET age = :age WHERE user_id = :user_id"
                session.execute(statement, [{"age": new_age, "user_id": target_user["user_id"]}])
                session.commit()
        return "OK"


@router.post('/users/update-age-all', tags=["users"])
def update_age_all(item: dict, admin_user = Depends(userid_from_token)):
    """
    body: {}
    今日が誕生日の人の年齢を更新する（毎朝UTC 00:00に稼働前提）
    """
    with Session(engine) as session:
        statement = "SELECT * FROM users WHERE deleted_at is NULL AND birthday is NOT NULL"
        target_users = session.execute(statement).all()
        
        for target_user in target_users:
            new_age = calculate_age(target_user["birthday"])
            if new_age != target_user["age"]:
                print(target_user["user_id"], new_age, target_user["age"])
                statement = "UPDATE users SET age = :age WHERE user_id = :user_id"
                session.execute(statement, [{"age": new_age, "user_id": target_user["user_id"]}])
                session.commit()
        return "OK"


@router.patch('/users/{id}', tags=["admin"])
def patch_(id, item: dict, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        target = session.exec(select(users).where(users.user_id == id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


@router.get('/age_verifications/{id}', tags=["admin"])
def get_(id: int, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            statement = f"SELECT a.*, u.birthday AS birthday, u.user_id, u.name, u.gender FROM age_verifications AS a\
                          JOIN users AS u ON a.user_id = u.user_id\
                          WHERE a.verification_id = {id};"
            res = session.exec(statement).one()
            print(res)
            return res
        except:
            return 403


@router.get('/age_verifications', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(age_verifications)).all()
            return res
        except:
            return 403

import base64


@router.get('/age_verifications/image/{id}', tags=["admin"])
def get_(id: int, user_id = Depends(userid_from_token)):
# def get_(id: int):
    with Session(engine) as session:
        try:
            statement = f"SELECT a.*, u.birthday AS birthday FROM age_verifications AS a\
                          JOIN users AS u ON a.user_id = u.user_id\
                          WHERE a.verification_id = {id};"
            res = session.exec(statement).one()
            print(res) # https://s3-ap-northeast-1.amazonaws.com/hanabi-private/personal-information/30_PI_20230904143445.png
            # key = "personal-information/menkyo.png"
            url_list = res["image_url"].split("/")
            file_type = res["image_url"].split(".")[-1]
            key = url_list[-2] + "/" + url_list[-1] # personal-information/30_PI_20230904143445.png
            filename = "tmp." + file_type
            s3.download_file(bucket_name_private, key, filename)
            with open(filename, "rb") as image_file:
                return_data = base64.b64encode(image_file.read())
            os.remove(filename)
            return return_data.decode('utf-8')
        except Exception as e:
            print(e)
            return 403


# @router.post('/age_verifications', tags=["admin"])
# def post_(item: age_verifications, user_id = Depends(userid_from_token)):
#     with Session(engine) as session:
#         try:
#             session.add(item)
#             session.commit()
#             session.refresh(item)
#             return item
#         except:
#             return 403


@router.patch('/age_verifications/{id}', tags=["admin"])
def patch_(id, item: dict, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        item["update_by"] = user_id
        target = session.exec(select(age_verifications).where(age_verifications.verification_id == id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        
        print(target)
        print(item)
        # push文面つくり
        # status: 送った 0, 承認 1, 否認 ２  pending 3
        if item["status"] == "1": # 承認された場合
            send_push({"user_id": target.user_id,
                       "text": "お客さまの本人確認の審査が完了しました"})
        elif item["status"] == "2": # 非承認された場合
            send_push({"user_id": target.user_id,
                       "text": "提出頂いた本人確認書類に不備がございました。お手数ですが、再度本人確認手続きをお願いいたします。"})
        
        # 保留になった場合はslackに通知
        if item["status"] == "3": # 保留
            message = f"<@U05AWMTL9TN> <@U0568PS6R40> <@U055K3AQEM8>\nkycが保留に更新されました。\n対象ユーザーID: {id}\n備考: {item['memo']}"
            send_slack_kyc(message)
        return target


@router.get('/user_photos', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(user_photos)).all()
            return res
        except:
            return 403


# @router.post('/user_photos', tags=["admin"])
# def post_(item: user_photos, user_id = Depends(userid_from_token)):
#     with Session(engine) as session:
#         try:
#             session.add(item)
#             session.commit()
#             session.refresh(item)
#             return item
#         except:
#             return 403


@router.patch('/user_photos/{id}', tags=["admin"])
def patch_(id, item: dict):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        target = session.exec(select(user_photos).where(user_photos.photo_id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


@router.get('/chat_rooms', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(chat_rooms)).all()
            return res
        except:
            return 403


@router.post('/chat_rooms', tags=["admin"])
def post_(item: chat_rooms, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


@router.patch('/chat_rooms/{id}', tags=["admin"])
def patch_(id, item: dict):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        target = session.exec(select(chat_rooms).where(chat_rooms.room_id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


@router.get('/chat_room_user', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(chat_room_user)).all()
            return res
        except:
            return 403


@router.post('/chat_room_user', tags=["admin"])
def post_(item: chat_room_user, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


# @router.patch('/chat_room_user/{id}', tags=["admin"])
# def patch_(id, item: dict):
#     with Session(engine) as session:
#         target = session.exec(select(chat_room_user).where()).one()
#         for key, value in item.items():
#             setattr(target, key, value)
#         session.add(target)
#         session.commit()
#         session.refresh(target)
#         return target


@router.get('/friends', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(friends)).all()
            return res
        except:
            return 403


@router.post('/friends', tags=["admin"])
def post_(item: friends, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


# @router.patch('/friends/{id}', tags=["admin"])
# def patch_(id, item: dict):
#     with Session(engine) as session:
#         target = session.exec(select().where()).one()
#         for key, value in item.items():
#             setattr(target, key, value)
#         session.add(target)
#         session.commit()
#         session.refresh(target)
#         return target


@router.get('/messages', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(messages)).all()
            return res
        except:
            return 403


@router.post('/messages', tags=["admin"])
def post_(item: messages, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


@router.patch('/messages/{id}', tags=["admin"])
def patch_(id, item: dict):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        target = session.exec(select(messages).where(messages.message_id == id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


@router.get('/areas', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(areas)).all()
            return res
        except:
            return 403


@router.post('/areas', tags=["admin"])
def post_(item: areas, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


@router.patch('/areas/{id}', tags=["admin"])
def patch_(id, item: dict):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        target = session.exec(select(areas).where(areas.area_id == id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


@router.get('/theme', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(theme)).all()
            return res
        except:
            return 403


@router.post('/theme', tags=["admin"])
def post_(item: theme, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


@router.patch('/theme/{id}', tags=["admin"])
def patch_(id, item: dict):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        target = session.exec(select(theme).where(theme.id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


@router.get('/participants', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(participants)).all()
            return res
        except:
            return 403


@router.post('/participants', tags=["admin"])
def post_(item: participants, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


@router.patch('/participants/{id}', tags=["admin"])
def patch_(id, item: dict):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        target = session.exec(select(participants).where(participants.participants_id == id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


@router.get('/user_opinions', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(user_opinions)).all()
            return res
        except:
            return 403


@router.post('/user_opinions', tags=["admin"])
def post_(item: user_opinions, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


@router.patch('/user_opinions/{id}', tags=["admin"])
def patch_(id, item: dict):
    with Session(engine) as session:
        del item["id"] # パスパラメータがクエリに入っているので消す
        target = session.exec(select(user_opinions).where(user_opinions.opinion_id == id)).one()
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


@router.get('/blocks', tags=["admin"])
def get_(user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            res = session.exec(select(blocks)).all()
            return res
        except:
            return 403


@router.post('/blocks', tags=["admin"])
def post_(item: blocks, user_id = Depends(userid_from_token)):
    with Session(engine) as session:
        try:
            session.add(item)
            session.commit()
            session.refresh(item)
            return item
        except:
            return 403


# @router.patch('/blocks/{id}', tags=["admin"])
# def patch_(id, item: dict):
#     with Session(engine) as session:
#         target = session.exec(select(blocks).where(blocks)).one()
#         for key, value in item.items():
#             setattr(target, key, value)
#         session.add(target)
#         session.commit()
#         session.refresh(target)
#         return target

@router.post('/push-test', tags=["utils"])
def push_test(item: dict):
    """
    push対象のuser_id: int
    text: str    
    """
    with Session(engine) as session:
        user_id = item["user_id"]
        title = "hanabi"
        text = item["text"]
        print(user_id)
        target = session.exec(select(users).where(users.user_id == user_id)).one()
        firebase_token = target.firebase_token
                
        text = text
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=text,
            ),
            token=firebase_token,
        )

        # メッセージの送信
        response = messaging.send(message)
        print("response:", response)
        
        return response


# full
@router.post('/push-test/content-available', tags=["utils"])
def push_test(item: dict):
    """
    ペイロードの全てをbodyで渡すAPI
    """
    with Session(engine) as session:
        user_id = item["user_id"]
        title = "hanabi"
        text = item["text"]
        content_available = item["content_available"] # bool
        print(user_id)
        target = session.exec(select(users).where(users.user_id == user_id)).one()
        firebase_token = target.firebase_token
        apns = messaging.APNSConfig(
            payload = messaging.APNSPayload(
                aps = messaging.Aps( content_available = content_available ) #　ここがバックグランド通知に必要な部分
            )
        )    
        
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=text,
            ),
            apns=apns,
            token=firebase_token,
        )

        # メッセージの送信
        response = messaging.send(message)
        print("response:", response)
        
        return response


# 内部関数
def delete_rooms_inner(event_id: int):
    """
    自分が所属するroomの情報は削除できる
    """
    with Session(engine) as session:
        target = session.exec(select(chat_rooms).where(chat_rooms.event_id == event_id)).one()
        now = datetime.datetime.now()
        item = {}
        item["deleted_at"] = now
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        return target


# SI済
@router.delete('/events/{event_id}/cancel', tags=["events"])
def delete_events(event_id: int, admin_id = Depends(userid_from_token)):
    """
    自分が主催者のイベントのみ削除可能
    """
    with Session(engine) as session:
        target = session.exec(select(events).where(events.event_id == event_id)).one()
        now = datetime.datetime.now()
        item = {}
        item["deleted_at"] = now
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        
        # グルチャのクローズ
        delete_rooms_inner(event_id)
        
        # カレンダーの削除
        if target.calender_id is not None:
            delete_calender_events(target.calender_id)
        
        # push文面作り
        statement = f"SELECT * FROM participants WHERE event_id = :event_id AND `status` IN (0,1,2)" # 3はcanceled
        participants = session.execute(statement, [{"event_id": event_id}]).all()
        start_date = target.start_at.strftime('%m月%d日')
        for participant in participants:
            send_push({"user_id": participant["user_id"],
                       "text": f"{start_date}開催予定の{target.name}は利用規約に違反していたため、運営によりキャンセルされました。"})
        
        send_slack(f"admin id: {admin_id}\naction: delete event. id: {event_id}")
        return target


def delete_calender_events(event_id: str):
    resopnse = gc_service.events().delete(calendarId='calender@hanabi.tech', eventId=event_id, sendUpdates="all").execute()
    return resopnse


# SI済
@router.delete('/users/{user_id}', tags=["users"])
def delete_users(user_id: int, admin_id = Depends(userid_from_token)):
    """
    DB上は論理削除。Firebase上はデータ物理削除。
    """
    with Session(engine) as session:
        target = session.exec(select(users).where(users.user_id == user_id).where(users.deleted_at == None)).one()
        now = datetime.datetime.now()
        item = {}
        item["deleted_at"] = now
        for key, value in item.items():
            setattr(target, key, value)
        session.add(target)
        session.commit()
        session.refresh(target)
        
        # firebase上の削除
        user = auth.get_user_by_phone_number(target.phone_number) # uid取得
        target_uid = user.uid
        auth.delete_user(target_uid) # 削除。戻りなし。削除失敗時にエラー吐く
        
        send_slack(f"admin id: {admin_id}\naction: delete user. id: {user_id}")
        return target


# idTokenのAuthentication
@router.get('/kpi/account-creation' ,tags=["kpi"])
def get_account_creation_data(admin_id = Depends(userid_from_token)):
    """
    アカウント登録者数の推移を取得する
    """
    with Session(engine) as session:
        statement = """SELECT 
                        DATE_FORMAT(created_at, '%m/%d') AS formatted_date,
                        COUNT(*) AS daily_count,
                        SUM(COUNT(*)) OVER (ORDER BY DATE_FORMAT(created_at, '%Y-%m-%d')) AS cumulative_count
                       FROM users
                       GROUP BY DATE_FORMAT(created_at, '%Y-%m-%d')
                       ORDER BY DATE_FORMAT(created_at, '%Y-%m-%d');
                    """
        try:
            data = session.exec(statement).all()
            return data
        except:
            raise HTTPException(status_code=400, detail=http_response_details[400])


@router.get('/kpi/account-gender' ,tags=["kpi"])
def get_account_gender_data(admin_id = Depends(userid_from_token)):
    """
    アカウントの性別の比率を取得する
    """
    with Session(engine) as session:
        statement = """SELECT count(*) AS count, CASE gender
        WHEN 0 THEN '男'
        WHEN 1 THEN '女'
        ELSE '不明'
    END as gender_text FROM users GROUP BY gender;
                    """
        try:
            data = session.exec(statement).all()
            return data
        except:
            raise HTTPException(status_code=400, detail=http_response_details[400])


@router.get('/kpi/kyc-gender/{gender}' ,tags=["kpi"])
def get_kyc_gender_data(gender: int, admin_id = Depends(userid_from_token)):
    """
    KYC済アカウントの性別の比率を取得する
    """
    with Session(engine) as session:
        statement = """SELECT 
                        DATE_FORMAT(u.created_at, '%m/%d') AS formatted_date,
                        COUNT(*) AS daily_count,
                        SUM(COUNT(*)) OVER (ORDER BY DATE_FORMAT(u.created_at, '%Y-%m-%d')) AS cumulative_count
                       FROM users AS u
                       JOIN age_verifications AS av ON u.user_id = av.user_id AND av.status = 1
                       WHERE u.gender = :gender
                       GROUP BY DATE_FORMAT(u.created_at, '%Y-%m-%d')
                       ORDER BY DATE_FORMAT(u.created_at, '%Y-%m-%d');
                    """
        try:
            data = session.execute(statement, [{"gender": gender}]).all()
            return data
        except Exception as e:
            print(e)
            raise HTTPException(status_code=400, detail=http_response_details[400])

