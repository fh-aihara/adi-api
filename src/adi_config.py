# 実行するPythonがあるパス
pythonpath = './'

# ワーカー数
workers = 8

# ワーカーのクラス、*2 にあるようにUvicornWorkerを指定 (Uvicornがインストールされている必要がある)
worker_class = 'uvicorn.workers.UvicornWorker'

# IPアドレスとポート
bind = '0.0.0.0:8000'

# プロセスIDを保存するファイル名
pidfile = 'prod.pid'

# Pythonアプリに渡す環境変数
raw_env = []

# デーモン化する場合はTrue
daemon = True

# エラーログ
errorlog = './error_log.txt'

# プロセスの名前
proc_name = 'hanabi_apigw'

# アクセスログ
accesslog = './access_log.txt'
