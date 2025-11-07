#!/bin/bash

# Azure Infrastructure Deployment Script
# Usage: ./deploy.sh [environment] [subscription-id]
# Example: ./deploy.sh dev your-subscription-id

set -e

# Default values
ENVIRONMENT=${1:-dev}
SUBSCRIPTION_ID=${2}
RESOURCE_GROUP="rg-docintell-gptv-${ENVIRONMENT}"
LOCATION="eastus"
DEPLOYMENT_NAME="docintell-gptv-${ENVIRONMENT}-$(date +%Y%m%d-%H%M%S)"

echo "🚀 Starting Azure infrastructure deployment..."
echo "Environment: $ENVIRONMENT"
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo "Deployment Name: $DEPLOYMENT_NAME"

# Set subscription if provided
if [ ! -z "$SUBSCRIPTION_ID" ]; then
    echo "Setting subscription to $SUBSCRIPTION_ID"
    az account set --subscription "$SUBSCRIPTION_ID"
fi

# Get current subscription info
CURRENT_SUBSCRIPTION=$(az account show --query name -o tsv)
echo "Current subscription: $CURRENT_SUBSCRIPTION"

# Create resource group if it doesn't exist
echo "📦 Creating resource group '$RESOURCE_GROUP'..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --output table

# Deploy the Bicep template
echo "🔧 Deploying Bicep template..."
DEPLOYMENT_OUTPUT=$(az deployment group create \
    --resource-group "$RESOURCE_GROUP" \
    --template-file "main.bicep" \
    --parameters "@parameters.${ENVIRONMENT}.json" \
    --name "$DEPLOYMENT_NAME" \
    --output json)

# Extract outputs
echo "📤 Extracting deployment outputs..."
AZURE_OPENAI_ENDPOINT=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.azureOpenAIEndpoint.value')
AZURE_OPENAI_KEY=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.azureOpenAIApiKey.value')
AZURE_OPENAI_DEPLOYMENT=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.azureOpenAIDeployment.value')
DOC_INTEL_ENDPOINT=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.documentIntelligenceEndpoint.value')
DOC_INTEL_KEY=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.documentIntelligenceKey.value')

# Create .env file
echo "📝 Creating .env file..."
cat > "../.env.${ENVIRONMENT}" << EOF
# Azure OpenAI Configuration
AZURE_OPENAI_API_DEPLOYMENT=${AZURE_OPENAI_DEPLOYMENT}
AZURE_OPENAI_API_KEY=${AZURE_OPENAI_KEY}
AZURE_OPENAI_ENDPOINT=${AZURE_OPENAI_ENDPOINT}
OPENAI_API_VERSION=2024-03-01-preview

# Azure Document Intelligence Configuration
DOC_INTEL_ENDPOINT=${DOC_INTEL_ENDPOINT}
DOC_INTEL_KEY=${DOC_INTEL_KEY}
EOF

echo "✅ Deployment completed successfully!"
echo ""
echo "📋 Resource Information:"
echo "  OpenAI Endpoint: $AZURE_OPENAI_ENDPOINT"
echo "  OpenAI Deployment: $AZURE_OPENAI_DEPLOYMENT"
echo "  Document Intelligence Endpoint: $DOC_INTEL_ENDPOINT"
echo ""
echo "🔐 Environment file created: .env.${ENVIRONMENT}"
echo "   Copy this to .env to use with your application"
echo ""
echo "🎯 To copy the environment file:"
echo "   cp .env.${ENVIRONMENT} .env"