#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/nyc-taxi-bigdata"
mkdir -p data/raw
for month in 02 03 04 05 06; do
  filename="yellow_tripdata_2024-${month}.parquet"
  destination="data/raw/$filename"
  url="https://d37ci6vzurychx.cloudfront.net/trip-data/$filename"
  echo "========= $filename ========="
  if [[ -f "$destination" ]] && python3 - "$destination" <<'PY'
import os,sys
p=sys.argv[1]
with open(p,'rb') as f:
    head=f.read(4)
    f.seek(-4,os.SEEK_END)
    tail=f.read(4)
sys.exit(0 if (head==b'PAR1' and tail==b'PAR1') else 1)
PY
  then
    echo 'Already downloaded: Parquet header/footer detected'
  else
    proxy_args=()
    if [[ -n "${PROXY_URL:-}" ]]; then
      proxy_args=(--proxy "$PROXY_URL")
    fi
    curl -fL --retry 3 --connect-timeout 20 -C - "${proxy_args[@]}" -o "$destination" "$url"
    python3 - "$destination" <<'PY'
import os,sys
p=sys.argv[1]
with open(p,'rb') as f:
    head=f.read(4)
    f.seek(-4,os.SEEK_END)
    tail=f.read(4)
assert head == b'PAR1' and tail == b'PAR1', 'Downloaded file is not a complete Parquet file'
print('OK: ',os.path.basename(p),os.path.getsize(p),'bytes')
PY
  fi
done
echo STAGE4_DOWNLOAD_SUCCESS
