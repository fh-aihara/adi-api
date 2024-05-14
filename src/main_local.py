from fastapi import FastAPI, Header, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from router import userapi, adminapi
import router
from starlette.responses import Response
import uvicorn
import requests
import json


def send_slack_error(message):
    web_hook_url = "https://hooks.slack.com/services/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" # error
    message = str(message)
    requests.post(web_hook_url, data = json.dumps({'text': message}))
    

# カスタムミドルウェアを定義
class DetectNon200ResponseMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, request: Request, call_next):
        response = await call_next(request)
        if response.status_code != 200 and response.status_code != 404:
            # ここにステータスコードが200以外の場合の処理を書く
            response_message = ""
            response_message += f"Detected non-200 response: {response.status_code}\n"
            response_message += request.method + ": " + request.url.path + "\n"
            response_message += str(request.headers) + "\n"
            send_slack_error(response_message)
        return response

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "https://sys.hanabi.tech"
]

# CORS対策
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # anyで許可しているが公開時は絞る
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.middleware('http')(DetectNon200ResponseMiddleware(userapi.router.__call__))
app.include_router(userapi.router, prefix="/api/v1")
app.include_router(adminapi.router, prefix="/api/v1/23FSnEBzTm6M")

if __name__ == "__main__":
    uvicorn.run("main_local:app", host="0.0.0.0", port=8000, log_level="info" ,reload=True)



