from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel
from decimal import Decimal

class query_histroy(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    SQL: Optional[str] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    title: Optional[str] = None
    description: Optional[str] = None
    last_query_records: Optional[int] = None

