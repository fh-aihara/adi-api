from fastapi import FastAPI, Header, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from router import adminapi
import router
from starlette.responses import Response
import uvicorn
import requests
import json
    
app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
]

# CORS対策
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # anyで許可しているが公開時は絞る
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(adminapi.router, prefix="/api")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info" ,reload=True)



