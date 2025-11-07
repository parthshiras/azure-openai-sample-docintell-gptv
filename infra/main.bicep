@description('Location for all resources')
param location string = resourceGroup().location

@description('Base name for all resources')
param baseName string = 'docintell-gptv'

@description('Environment suffix (dev, test, prod)')
param environmentSuffix string = 'dev'

@description('SKU for Azure OpenAI service')
param openAISku string = 'S0'

@description('SKU for Document Intelligence service')
param documentIntelligenceSku string = 'S0'

var uniqueSuffix = substring(uniqueString(resourceGroup().id), 0, 5)
var openAIName = '${baseName}-openai-${environmentSuffix}-${uniqueSuffix}'
var docIntelName = '${baseName}-docintel-${environmentSuffix}-${uniqueSuffix}'

// Azure OpenAI Service
resource openAIAccount 'Microsoft.CognitiveServices/accounts@2023-05-01' = {
  name: openAIName
  location: location
  kind: 'OpenAI'
  sku: {
    name: openAISku
  }
  properties: {
    customSubDomainName: openAIName
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      defaultAction: 'Allow'
    }
  }
}

// GPT-4 Vision deployment
resource gpt4Deployment 'Microsoft.CognitiveServices/accounts/deployments@2023-05-01' = {
  parent: openAIAccount
  name: 'gpt-4o'
  sku: {
    name: 'Standard'
    capacity: 10
  }
  properties: {
    model: {
      format: 'OpenAI'
      name: 'gpt-4o'
      version: '2024-08-06'
    }
    versionUpgradeOption: 'OnceNewDefaultVersionAvailable'
    raiPolicyName: 'Microsoft.Default'
  }
}

// Azure Document Intelligence Service
resource documentIntelligence 'Microsoft.CognitiveServices/accounts@2023-05-01' = {
  name: docIntelName
  location: location
  kind: 'FormRecognizer'
  sku: {
    name: documentIntelligenceSku
  }
  properties: {
    customSubDomainName: docIntelName
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      defaultAction: 'Allow'
    }
  }
}

// Outputs for environment variables
output azureOpenAIEndpoint string = openAIAccount.properties.endpoint
output azureOpenAIApiKey string = openAIAccount.listKeys().key1
output azureOpenAIDeployment string = gpt4Deployment.name
output documentIntelligenceEndpoint string = documentIntelligence.properties.endpoint
output documentIntelligenceKey string = documentIntelligence.listKeys().key1

// Additional outputs for reference
output openAIResourceName string = openAIAccount.name
output documentIntelligenceResourceName string = documentIntelligence.name
output resourceGroupName string = resourceGroup().name
