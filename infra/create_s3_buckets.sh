#!/usr/bin/env bash
# Cria os buckets S3 do data lake (camadas raw e trusted) e o database do
# Glue Data Catalog. Requer AWS CLI configurado (aws configure).
set -euo pipefail

REGION="us-east-1"
PROJETO="oet-datalake"

echo "Criando bucket raw..."
aws s3api create-bucket \
  --bucket "${PROJETO}-raw" \
  --region "$REGION"

echo "Criando bucket trusted..."
aws s3api create-bucket \
  --bucket "${PROJETO}-trusted" \
  --region "$REGION"

echo "Bloqueando acesso público nos dois buckets..."
for BUCKET in "${PROJETO}-raw" "${PROJETO}-trusted"; do
  aws s3api put-public-access-block \
    --bucket "$BUCKET" \
    --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
done

echo "Criando database no Glue Data Catalog..."
aws glue create-database --database-input '{"Name": "oet_datalake"}' --region "$REGION"

echo "Concluído. Buckets: ${PROJETO}-raw, ${PROJETO}-trusted"
