#!/bin/bash
set -e

# ── Configuration ─────────────────────────────────────────────────────────────
FUNCTION_NAME="lambda-mcp-server"
API_NAME="lambda-mcp-api"
REGION="${AWS_REGION:-us-east-1}"
RUNTIME="python3.12"
TIMEOUT=30
MEMORY=256

# Required: export LAMBDA_ROLE_ARN=arn:aws:iam::<account-id>:role/<role-name>
if [ -z "$LAMBDA_ROLE_ARN" ]; then
  echo "ERROR: LAMBDA_ROLE_ARN environment variable is not set."
  echo "  export LAMBDA_ROLE_ARN=arn:aws:iam::<account-id>:role/<your-lambda-role>"
  exit 1
fi

# ── Build package ──────────────────────────────────────────────────────────────
echo "→ Installing dependencies (Linux x86_64 wheels for Lambda)..."
rm -rf .target
mkdir -p .target/package
pip3 install \
  --platform manylinux2014_x86_64 \
  --target .target/package/ \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  --quiet \
  -r requirements.txt

echo "→ Copying source..."
cp lambda_function.py .target/package/

echo "→ Stripping unnecessary files..."
find .target/package -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find .target/package -type f -name "*.pyc" -delete
find .target/package -type f -name "*.pyo" -delete

echo "→ Zipping..."
cd .target/package && zip -r ../deployment.zip . -q && cd ../..
echo "   Package size: $(du -sh .target/deployment.zip | cut -f1)"

# ── Deploy Lambda ──────────────────────────────────────────────────────────────
if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" > /dev/null 2>&1; then
  echo "→ Updating existing Lambda function..."
  aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file fileb://.target/deployment.zip \
    --region "$REGION" > /dev/null

  aws lambda wait function-updated \
    --function-name "$FUNCTION_NAME" \
    --region "$REGION"
else
  echo "→ Creating new Lambda function..."
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --runtime "$RUNTIME" \
    --role "$LAMBDA_ROLE_ARN" \
    --handler "lambda_function.handler" \
    --zip-file fileb://.target/deployment.zip \
    --timeout "$TIMEOUT" \
    --memory-size "$MEMORY" \
    --region "$REGION" > /dev/null

  aws lambda wait function-active \
    --function-name "$FUNCTION_NAME" \
    --region "$REGION"
fi

# ── Set environment variables ──────────────────────────────────────────────────
echo "→ Setting environment variables..."
if [ -z "$GMAIL_USER" ] || [ -z "$GMAIL_APP_PASSWORD" ] || [ -z "$MCP_API_KEY" ]; then
  echo "WARNING: One or more required env vars not set (GMAIL_USER, GMAIL_APP_PASSWORD, MCP_API_KEY)."
  echo "  Set them manually in the Lambda console, or re-run after exporting them."
else
  aws lambda update-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --environment "Variables={GMAIL_USER=$GMAIL_USER,GMAIL_APP_PASSWORD=$GMAIL_APP_PASSWORD,MCP_API_KEY=$MCP_API_KEY}" \
    --region "$REGION" > /dev/null

  aws lambda wait function-updated \
    --function-name "$FUNCTION_NAME" \
    --region "$REGION"
fi

# ── API Gateway HTTP API ───────────────────────────────────────────────────────
LAMBDA_ARN=$(aws lambda get-function \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" \
  --query 'Configuration.FunctionArn' \
  --output text)

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

EXISTING_API_ID=$(aws apigatewayv2 get-apis \
  --region "$REGION" \
  --query "Items[?Name=='$API_NAME'].ApiId" \
  --output text)

if [ -n "$EXISTING_API_ID" ]; then
  echo "→ Updating existing API Gateway ($EXISTING_API_ID)..."
  API_ID="$EXISTING_API_ID"

  # Update the integration to point at the (possibly redeployed) Lambda
  INTEGRATION_ID=$(aws apigatewayv2 get-integrations \
    --api-id "$API_ID" \
    --region "$REGION" \
    --query 'Items[0].IntegrationId' \
    --output text)

  aws apigatewayv2 update-integration \
    --api-id "$API_ID" \
    --integration-id "$INTEGRATION_ID" \
    --integration-uri "arn:aws:apigateway:${REGION}:lambda:path/2015-03-31/functions/${LAMBDA_ARN}/invocations" \
    --region "$REGION" > /dev/null
else
  echo "→ Creating API Gateway HTTP API..."
  API_ID=$(aws apigatewayv2 create-api \
    --name "$API_NAME" \
    --protocol-type HTTP \
    --region "$REGION" \
    --query 'ApiId' \
    --output text)

  INTEGRATION_ID=$(aws apigatewayv2 create-integration \
    --api-id "$API_ID" \
    --integration-type AWS_PROXY \
    --integration-uri "arn:aws:apigateway:${REGION}:lambda:path/2015-03-31/functions/${LAMBDA_ARN}/invocations" \
    --payload-format-version "2.0" \
    --region "$REGION" \
    --query 'IntegrationId' \
    --output text)

  aws apigatewayv2 create-route \
    --api-id "$API_ID" \
    --route-key '$default' \
    --target "integrations/$INTEGRATION_ID" \
    --region "$REGION" > /dev/null

  aws apigatewayv2 create-stage \
    --api-id "$API_ID" \
    --stage-name '$default' \
    --auto-deploy \
    --region "$REGION" > /dev/null
fi

# Allow API Gateway to invoke the Lambda function
aws lambda add-permission \
  --function-name "$FUNCTION_NAME" \
  --statement-id "AllowAPIGatewayInvoke" \
  --action "lambda:InvokeFunction" \
  --principal "apigateway.amazonaws.com" \
  --source-arn "arn:aws:execute-api:${REGION}:${ACCOUNT_ID}:${API_ID}/*/*" \
  --region "$REGION" > /dev/null 2>&1 || true  # already exists is fine

API_URL=$(aws apigatewayv2 get-api \
  --api-id "$API_ID" \
  --region "$REGION" \
  --query 'ApiEndpoint' \
  --output text)

# ── Done ───────────────────────────────────────────────────────────────────────
echo ""
echo "✓ Deployment complete!"
echo ""
echo "  API Gateway URL : $API_URL"
echo "  MCP endpoint    : ${API_URL}/mcp"
echo ""
echo "Add this to your MCP client config (see mcp_client_config_example.json):"
echo ""
echo '  "mcpServers": {'
echo '    "email-sender": {'
echo "      \"url\": \"${API_URL}/mcp\","
echo '      "headers": {'
echo "        \"x-api-key\": \"$MCP_API_KEY\""
echo '      }'
echo '    }'
echo '  }'
