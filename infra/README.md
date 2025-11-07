# Infrastructure as Code for Document Intelligence GPT Vision Application

This folder contains Bicep templates to deploy the required Azure resources for the Document Intelligence GPT Vision application.

## Required Azure Resources

The application requires the following Azure services:
- **Azure OpenAI Service** - for GPT-4 Vision capabilities
- **Azure Document Intelligence** - for OCR and barcode detection

## Files Structure

```
infra/
├── main.bicep              # Main Bicep template
├── parameters.dev.json     # Development environment parameters
├── parameters.prod.json    # Production environment parameters
├── deploy.sh              # Deployment script
└── README.md              # This file
```

## Prerequisites

1. **Azure CLI** installed and configured
2. **Bicep CLI** installed (`az bicep install`)
3. **jq** installed for JSON parsing
4. Appropriate Azure subscription permissions to create:
   - Resource Groups
   - Cognitive Services accounts
   - OpenAI deployments

## Deployment

### Option 1: Using the deployment script (Recommended)

**Note**: The `.env.dev` and `.env.prod` files are created automatically during deployment.

```bash
cd infra

# Deploy to development environment
./deploy.sh dev

# Deploy to production environment  
./deploy.sh prod

# Deploy with specific subscription
./deploy.sh dev your-subscription-id
```

The script will:
1. Create a resource group
2. Deploy the Bicep template
3. Extract the deployment outputs
4. Create an `.env.{environment}` file in the parent directory with the connection details

### Option 2: Manual setup (for existing resources)

If you already have Azure resources or want to configure manually:

```bash
cd infra

# Create environment template
./setup-manual.sh dev

# Edit the generated file with your actual values
nano ../.env.dev
```

### Option 3: Manual deployment

```bash
cd infra

# Create resource group
az group create --name rg-docintell-gptv-dev --location eastus

# Deploy the template
az deployment group create \
  --resource-group rg-docintell-gptv-dev \
  --template-file main.bicep \
  --parameters @parameters.dev.json
```

## Configuration Parameters

You can customize the deployment by modifying the parameters files:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `baseName` | Base name for all resources | `docintell-gptv` |
| `environmentSuffix` | Environment suffix (dev/prod) | `dev` |
| `location` | Azure region | `eastus` |
| `openAISku` | Azure OpenAI SKU | `S0` |
| `documentIntelligenceSku` | Document Intelligence SKU | `S0` |

## Outputs

After deployment, the template provides:
- Azure OpenAI endpoint and API key
- Azure OpenAI deployment name
- Document Intelligence endpoint and API key

These values are automatically written to a `.env.{environment}` file when using the deployment script.

## Post-Deployment Steps

1. Copy the generated environment file:
   ```bash
   cp .env.dev .env
   ```

2. Verify the GPT-4 Vision deployment is available:
   ```bash
   az cognitiveservices account deployment list \
     --name your-openai-resource-name \
     --resource-group rg-docintell-gptv-dev
   ```

## Resource Naming Convention

Resources are named using the pattern:
- OpenAI: `{baseName}-openai-{environment}-{uniqueId}`
- Document Intelligence: `{baseName}-docintel-{environment}-{uniqueId}`

## Security Considerations

- Resources are deployed with public network access enabled
- API keys are stored in the deployment outputs
- Consider using Azure Key Vault for production deployments
- Review network access policies based on your security requirements

## Cost Optimization

- **Development**: Use `F0` (free) tier where available
- **Production**: Use `S0` tier for better performance and SLA
- Monitor usage through Azure Cost Management

## Troubleshooting

### Common Issues

1. **OpenAI not available in region**: Try different regions like `eastus`, `westus2`, or `westeurope`
2. **Quota exceeded**: Request quota increase through Azure portal
3. **Insufficient permissions**: Ensure you have Contributor role on the subscription

### Checking deployment status

```bash
az deployment group list --resource-group rg-docintell-gptv-dev --output table
```

### Viewing deployment logs

```bash
az deployment group show \
  --resource-group rg-docintell-gptv-dev \
  --name your-deployment-name \
  --query properties.error
```

## Clean Up

To delete all resources:

```bash
az group delete --name rg-docintell-gptv-dev --yes --no-wait
```