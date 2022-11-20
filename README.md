Job Dispatcher Core
===================
여러가지 언어로 job dispatcher core 부분 구현하고 성능 비교하기

local dynamodb
--------------
로컬에서 ddb container 실행
```
docker run -p 8000:8000 amazon/dynamodb-local \
  -jar DynamoDBLocal.jar -sharedDb
```

table 세팅 스크립트 실행
```
node node/ddb_settings --table-name={tableName} --item-count={itemCount}
```
launch program
---------------
node
```
npm install

node index.js
```

python
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python3 index.py
```

golang
```
go mod tidy

go run main.go
```

metric 측정
----------