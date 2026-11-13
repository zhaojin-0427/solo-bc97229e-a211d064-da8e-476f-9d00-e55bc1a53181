FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data

WORKDIR /srv

# 先装依赖，利用镜像层缓存
COPY requirements.txt /srv/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝应用代码与演示素材
COPY app /srv/app
COPY demo /srv/demo

# 持久化目录：上传副本 + 控制库（报告）
RUN mkdir -p /data/uploads
VOLUME ["/data"]

EXPOSE 8000

# 单容器单 worker：批次取消注册表为进程内状态，多 worker 会使取消信号失效；
# 批次间并行由内部线程池提供；每条规则在独立子进程中执行，
# 超时/取消由父进程 SIGKILL 子进程真正停止计算（长文本 GLOB 等 SQLite
# 内置函数执行期间 interrupt 无效，进程级终止是唯一可靠手段）。
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=5 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4).status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
