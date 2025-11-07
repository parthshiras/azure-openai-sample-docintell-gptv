#!/bin/bash

# Validation script for deployed Azure resources
# Usage: ./validate.sh [environment]

set -e

ENVIRONMENT=${1:-dev}
ENV_FILE="../.env.${ENVIRONMENT}"

echo "🔍 Validating Azure resources for environment: $ENVIRONMENT"

# Check if environment file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Environment file not found: $ENV_FILE"
    echo "   Run deployment first: ./deploy.sh $ENVIRONMENT"
    exit 1
fi

# Source environment variables
source "$ENV_FILE"

echo "📋 Environment Configuration:"
echo "  OpenAI Endpoint: $AZURE_OPENAI_ENDPOINT"
echo "  OpenAI Deployment: $AZURE_OPENAI_API_DEPLOYMENT"
echo "  Document Intelligence Endpoint: $DOC_INTEL_ENDPOINT"

# Test Azure OpenAI connection
echo ""
echo "🧠 Testing Azure OpenAI connection..."
OPENAI_TEST=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "api-key: $AZURE_OPENAI_API_KEY" \
    "$AZURE_OPENAI_ENDPOINT/openai/deployments?api-version=2023-05-15")

if [ "$OPENAI_TEST" = "200" ]; then
    echo "✅ Azure OpenAI connection successful"
else
    echo "❌ Azure OpenAI connection failed (HTTP: $OPENAI_TEST)"
fi

# Test Document Intelligence connection
echo ""
echo "📄 Testing Document Intelligence connection..."
DOC_INTEL_TEST=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Ocp-Apim-Subscription-Key: $DOC_INTEL_KEY" \
    "$DOC_INTEL_ENDPOINT/formrecognizer/documentModels?api-version=2023-07-31")

if [ "$DOC_INTEL_TEST" = "200" ]; then
    echo "✅ Document Intelligence connection successful"
else
    echo "❌ Document Intelligence connection failed (HTTP: $DOC_INTEL_TEST)"
fi

# Check GPT-4 deployment specifically
echo ""
echo "🎯 Testing GPT-4 deployment..."
GPT4_TEST=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -H "api-key: $AZURE_OPENAI_API_KEY" \
    "$AZURE_OPENAI_ENDPOINT/openai/deployments/$AZURE_OPENAI_API_DEPLOYMENT?api-version=2023-05-15")

if [ "$GPT4_TEST" = "200" ]; then
    echo "✅ GPT-4 deployment accessible"
else
    echo "❌ GPT-4 deployment not accessible (HTTP: $GPT4_TEST)"
    echo "   Note: It may take a few minutes for the deployment to be ready"
fi

echo ""
echo "🎉 Validation complete!"
echo ""
echo "💡 Next steps:"
echo "   1. Copy environment file: cp .env.$ENVIRONMENT ../.env"
echo "   2. Install Python dependencies: pip install -r ../requirements.txt"
echo "   3. Test the application: python ../test.py"