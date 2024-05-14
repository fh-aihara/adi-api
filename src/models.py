from datetime import datetime
from typing import Optional
from unicodedata import decimal
from sqlmodel import Field, SQLModel
from decimal import Decimal

class users(SQLModel, table=True):
    user_id: Optional[int] = Field(primary_key=True)
    name: Optional[str]
    age: Optional[int]
    gender: Optional[int]
    state: Optional[int]
    phone_number: str
    bio: Optional[str] # 自己紹介文
    authentication_token: Optional[str]
    firebase_token: Optional[str]
    device_uuid: Optional[str]
    close_reason: Optional[str]
    invite_code: Optional[str]
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]
    deleted_at: Optional[datetime]
    birthday: Optional[datetime] # 誕生日　画面非表示　KYC情報
    blood_type: Optional[int] # 血液型
    height: Optional[int] # 身長
    educational_background: Optional[int] # 学歴
    working_type: Optional[int] # 職業
    holiday: Optional[int] # 休日
    drink_type: Optional[int] # お酒
    smoke_type: Optional[int] # タバコ
    top_message: Optional[str] # ひとこと入力
    mail: Optional[str] # メールアドレス　画面非表示
    workplace: Optional[str] # 勤務先
    graduated_from: Optional[str] # 大学名
    annual_income: Optional[int] # 年収
    mail_unsubscribed: Optional[int] # メール配信停止 NULL or 1
    referral_code:  Optional[str] # 入力された招待コード
    vip_display: Optional[int] # VIP表示のON/OFF デフォルトON
    participated_rate: Optional[float] # 参加率
    not_problematic_person_rate: Optional[float] # 問題がなかった率


class age_verifications(SQLModel, table=True):
    verification_id: Optional[int] = Field(primary_key=True)
    user_id: int = Field(foreign_key="users.user_id") 
    image_url: str
    status: int # status: 送った 0, 承認 1, 否認 ２  pending 3
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]
    memo: Optional[str]
    update_by: Optional[int]
    

class user_photos(SQLModel, table=True):
    photo_id: Optional[int] = Field(primary_key=True)
    user_id: int = Field(foreign_key="users.user_id") 
    image_url: str
    is_main: int
    deleted_at: Optional[datetime]
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]
    priority: Optional[int]


class chat_rooms(SQLModel, table=True):
    room_id: Optional[int] = Field(primary_key=True)
    name: str = Field(foreign_key="users.user_id")
    deleted_at: Optional[datetime]
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]
    event_id: Optional[int] = Field(foreign_key="events.event_id") 


class chat_room_user(SQLModel, table=True):
    room_id: int = Field(foreign_key="chat_rooms.room_id", primary_key=True) 
    user_id: int = Field(foreign_key="users.user_id", primary_key=True) 
    # created_at: Optional[datetime]
    deleted_at: Optional[datetime]


class friends(SQLModel, table=True):
    from_user_id: int = Field(foreign_key="users.user_id", primary_key=True) 
    to_user_id: int = Field(foreign_key="users.user_id", primary_key=True) 
    # created_at: Optional[datetime]
    deleted_at: Optional[datetime]


class messages(SQLModel, table=True):
    message_id: Optional[int] = Field(primary_key=True)
    text: str
    user_id: int = Field(foreign_key="users.user_id") 
    room_id: int = Field(foreign_key="chat_rooms.room_id") 
    deleted_at: Optional[datetime]
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]


class states(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    name: str
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]


class events(SQLModel, table=True):
    event_id: Optional[int] = Field(primary_key=True)
    theme: int
    states: int = Field(foreign_key="states.id") 
    area: int = Field(foreign_key="areas.area_id") 
    start_at: datetime
    name: str
    payment_ratio: int
    male_estimated_payment: int
    female_estimated_payment: int
    message_from_owner: Optional[str]
    owner_user_id: Optional[int] = Field(foreign_key="users.user_id") 
    deleted_at: Optional[datetime]
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]
    participants_pattern: int
    restaurant_url: Optional[str]
    male_participants_number: int
    female_participants_number: int
    status: Optional[int]
    calender_id: Optional[str]
    

class theme(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    name: str
    image_url: str
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]
    deleted_at: Optional[datetime]
    priority: int


class participant_group(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    event_id: int = Field(foreign_key="events.event_id") 
    user_id: int = Field(foreign_key="users.user_id") 
    group_id: int
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]


class participants(SQLModel, table=True):
    participants_id: Optional[int] = Field(primary_key=True)
    event_id: Optional[int] = Field(foreign_key="events.event_id") 
    user_id: Optional[int] = Field(foreign_key="users.user_id") 
    status: Optional[int]
    group_id: Optional[int] = Field(foreign_key="participant_group.group_id") 
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]


class user_opinions(SQLModel, table=True):
    opinion_id: Optional[int] = Field(primary_key=True)
    opinion_type: Optional[int]  # お問合せ、意見（要望、不満、不安）、苦情・通報 リスト
    return_address: Optional[str] # 返信用email
    function_name: Optional[int] # 機能名　リスト
    user_id: Optional[int] = Field(foreign_key="users.user_id")  # キーから取得して格納する
    content: Optional[str] # 内容
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]


class blocks(SQLModel, table=True):
    from_user_id: int = Field(foreign_key="users.user_id", primary_key=True) 
    to_user_id: int = Field(foreign_key="users.user_id", primary_key=True) 
    # created_at: Optional[datetime]


class areas(SQLModel, table=True):
    area_id: Optional[int] = Field(primary_key=True)
    states: int = Field(foreign_key="states.id") 
    name: str
    priority: int
    deleted_at: Optional[datetime]
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]


class admin_users(SQLModel, table=True):
    admin_id: Optional[int] = Field(primary_key=True)
    name: str
    password_hash: str
    authority: int
    deleted_at: Optional[datetime]
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]
    auth_token: str


class apple_receipts(SQLModel, table=True):
    receipt_id: Optional[int] = Field(primary_key=True)
    user_id: Optional[int] = Field(foreign_key="users.user_id") 
    transaction_id: Optional[str]
    product_id: Optional[str]
    original_transaction_id: Optional[str] # 元データのトランザクションID
    purchase_date: Optional[datetime] # 購入日
    original_purchase_date: Optional[datetime] # 元データの購入日
    expires_date: Optional[datetime] # 期限
    web_order_line_item_id: Optional[str]
    environment: Optional[str] # prod or sandbox
    receipt_data: str # rawデータ
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]


class reviews(SQLModel, table=True):
    from_user_id: Optional[int] = Field(foreign_key="users.user_id", primary_key=True) 
    to_user_id: int = Field(foreign_key="users.user_id", primary_key=True) 
    event_id: int = Field(foreign_key="events.event_id") 
    participated: int
    problematic_person: int
    # created_at: Optional[datetime]


class viewed_messages(SQLModel, table=True):
    user_id: Optional[int] = Field(foreign_key="users.user_id", primary_key=True) # primary_keyを定義しないとこけるのでつけている
    message_id: int = Field(foreign_key="messages.message_id", primary_key=True) # primary_keyを定義しないとこけるのでつけている
    # created_at: Optional[datetime]


class tags(SQLModel, table=True):
    tag_id: Optional[int] = Field(primary_key=True)
    content: str
    priority: int
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]
    deleted_at: Optional[datetime]


class event_tag(SQLModel, table=True):
    event_id: Optional[int] = Field(foreign_key="events.event_id", primary_key=True) # primary_keyを定義しないとこけるのでつけている
    tag_id: int = Field(foreign_key="tags.tag_id", primary_key=True) # primary_keyを定義しないとこけるのでつけている
    # created_at: Optional[datetime]


class sugu_nomi(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    user_id: int = Field(foreign_key="users.user_id") 
    latitude: float # 緯度
    longitude: float # 経度
    expire_at: datetime
    deleted_at: Optional[datetime]
    estimated_payment: Optional[int] # 予算
    restaurant_genre: Optional[int] # レストランのジャンル
    waiting_with: Optional[str] # 一緒に待機しているユーザーのID "11,22,33"
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]


class sugu_nomi_apply(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    user_id: int = Field(foreign_key="users.user_id") 
    sugu_nomi_id: int = Field(foreign_key="sugu_nomi.id") 
    status: Optional[int] # nullが何もしてない 0が非承認 1が承認
    apply_with: Optional[str] # 一緒に参加するユーザーのID "11,22,33"
    deleted_at: Optional[datetime]
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]


class sugu_nomi_available_area(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    latitude: float # 緯度
    longitude: float # 経度
    name: str # 所在名称
    # created_at: Optional[datetime]
    # updated_at: Optional[datetime]

