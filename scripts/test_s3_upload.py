"""Teste rápido de upload para S3 — valida credenciais e acesso ao bucket."""

import os
from pathlib import Path

import boto3

BUCKET = os.getenv("DEVRADAR_S3_BUCKET", "devradar-raw")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=REGION,
)

print(f"Bucket : {BUCKET}")
print(f"Region : {REGION}")
print(f"Key ID : {os.getenv('AWS_ACCESS_KEY_ID', '???')[:8]}...")

# 1) Testar listagem do bucket
print("\n[1] Listando objetos no bucket...")
try:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="reddit/", MaxKeys=5)
    contents = resp.get("Contents", [])
    if contents:
        for obj in contents:
            print(f"  {obj['Key']}  ({obj['Size'] / 1024:.1f} KB)")
    else:
        print("  (vazio)")
    print("  -> Listagem OK")
except Exception as e:
    print(f"  -> ERRO na listagem: {e}")

# 2) Testar upload de um arquivo pequeno
print("\n[2] Testando upload de arquivo de teste...")
test_key = "reddit/_test/ping.json"
test_body = b'{"test": true, "msg": "DevRadar S3 test"}'
try:
    s3.put_object(Bucket=BUCKET, Key=test_key, Body=test_body, ContentType="application/json")
    print(f"  -> Upload OK: s3://{BUCKET}/{test_key}")
except Exception as e:
    print(f"  -> ERRO no upload: {e}")
    raise

# 3) Testar upload de um arquivo real
print("\n[3] Testando upload de um arquivo real...")
data_dir = Path(__file__).parent.parent / "airflow" / "data" / "reddit"
real_files = sorted(data_dir.glob("*/date=*/raw_*.json"))

if real_files:
    local_path = real_files[0]
    relative = local_path.relative_to(data_dir)
    s3_key = f"reddit/{relative.as_posix()}"

    body = local_path.read_bytes()
    print(f"  Arquivo: {local_path.name} ({len(body) / 1024:.1f} KB)")
    print(f"  S3 Key : {s3_key}")

    try:
        s3.put_object(Bucket=BUCKET, Key=s3_key, Body=body, ContentType="application/json")
        print(f"  -> Upload OK: s3://{BUCKET}/{s3_key}")
    except Exception as e:
        print(f"  -> ERRO no upload: {e}")
else:
    print("  Nenhum arquivo raw_*.json encontrado em airflow/data/reddit/")

# 4) Limpeza do arquivo de teste
print("\n[4] Limpando arquivo de teste...")
try:
    s3.delete_object(Bucket=BUCKET, Key=test_key)
    print(f"  -> Removido: {test_key}")
except Exception as e:
    print(f"  -> Erro ao limpar (não crítico): {e}")

print("\nTeste concluído!")
