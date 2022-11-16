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

table 생성
```
aws dynamodb create-table \
    --table-name deali_schedules \
    --attribute-definitions AttributeName=shard_id,AttributeType=S AttributeName=date_token,AttributeType=S \
    --key-schema AttributeName=shard_id,KeyType=HASH AttributeName=date_token,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --profile dynamodb-local \
    --endpoint-url http://localhost:8000
```

table 정보
```
aws dynamodb describe-table --table-name deali_schedules --profile dynamodb-local --endpoint-url http://localhost:8000
```

table 삭제
```
aws dynamodb delete-table --table-name deali_schedules --profile dynamodb-local --endpoint-url http://localhost:8000
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

metric 측정
----------