NYC Taxi 大数据大作业 / Stage 5：Streamlit 可交互 Web 系统

文件放置（将 ZIP 直接解压到 ~/nyc-taxi-bigdata 根目录，而不是 apps 目录）：
  docker-compose.dashboard.yml
  stage5_app/
    app.py
    requirements.txt
    Dockerfile.dashboard
    prepare_geo.py
    validate.py

数据来源：阶段三 Spark SQL CSV 汇总表、阶段四 Spark MLlib 六月测试集 Parquet 预测结果、NYC Open Data 出租车区域 GeoJSON。

Ubuntu 中执行：
  cd ~/nyc-taxi-bigdata
  curl -fL --retry 3 -o data/raw/taxi_zones.geojson 'https://data.cityofnewyork.us/api/v3/views/8meu-9t5y/query.geojson?accessType=DOWNLOAD'
  python3 stage5_app/prepare_geo.py
  sudo docker compose stop
  sudo docker compose -p taxi-dashboard -f docker-compose.dashboard.yml up -d --build
  sudo docker compose -p taxi-dashboard -f docker-compose.dashboard.yml exec -T dashboard python /app/validate.py
  sudo docker compose -p taxi-dashboard -f docker-compose.dashboard.yml logs --tail 30

Windows 中打开： http://192.168.92.134:8501 （IP 可能变；Ubuntu hostname -I 查询）

注：首次构建需从 Docker Hub 获取 Python 镜像并通过 pip 下载依赖，必要时使用 Clash 代理：
  sudo docker compose -p taxi-dashboard -f docker-compose.dashboard.yml build \
    --build-arg HTTP_PROXY=http://192.168.92.1:7897 \
    --build-arg HTTPS_PROXY=http://192.168.92.1:7897
  sudo docker compose -p taxi-dashboard -f docker-compose.dashboard.yml up -d
端口 7897 仅示例，需改为你的 Clash 混合端口。

关闭：
  sudo docker compose -p taxi-dashboard -f docker-compose.dashboard.yml stop
重启：
  sudo docker compose -p taxi-dashboard -f docker-compose.dashboard.yml up -d

本系统不做实时推理，预测页是 2024 年 6 月历史数据的单步滚动预测回测。分析统计为 2024 年 1 月。
请勿把虚拟机 Web 端口暴露到公网。
