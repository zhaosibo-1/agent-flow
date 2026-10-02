FROM python:3.12-slim

WORKDIR /srv

# 先拷依赖清单再装依赖：这一层只要 requirements 不变就是缓存的，
# 改代码重建镜像时不用重新拉包。
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY web ./web

# 非root用户跑，容器被攻破也不至于直接拿到 root
RUN useradd --create-home runner && chown -R runner /srv
USER runner

EXPOSE 8132
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8132/api/health',timeout=2)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8132"]
