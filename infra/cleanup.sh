#!/bin/bash

# Azure Resource Cleanup Script
# Usage: ./cleanup.sh [environment]
# Example: ./cleanup.sh dev

set -e

ENVIRONMENT=${1:-dev}
RESOURCE_GROUP="rg-docintell-gptv-${ENVIRONMENT}"

echo "🗑️  Azure Resource Cleanup"
echo "=========================="
echo "Environment: $ENVIRONMENT"
echo "Resource Group: $RESOURCE_GROUP"
echo ""

# Get current subscription info
CURRENT_SUBSCRIPTION=$(az account show --query name -o tsv 2>/dev/null || echo "Not logged in")
echo "Current subscription: $CURRENT_SUBSCRIPTION"

if [ "$CURRENT_SUBSCRIPTION" = "Not logged in" ]; then
    echo "❌ Please login to Azure first: az login"
    exit 1
fi

# Check if resource group exists
if az group exists --name "$RESOURCE_GROUP" --output tsv | grep -q "true"; then
    echo "📋 Found resource group: $RESOURCE_GROUP"
    
    # List resources in the group
    echo ""
    echo "🔍 Resources to be deleted:"
    az resource list --resource-group "$RESOURCE_GROUP" --output table --query "[].{Name:name,Type:type,Location:location}"
    
    echo ""
    read -p "❓ Do you want to delete all resources in '$RESOURCE_GROUP'? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "🔥 Deleting resource group: $RESOURCE_GROUP"
        echo "⏳ This may take a few minutes..."
        
        # Delete the resource group (this deletes all resources inside it)
        az group delete --name "$RESOURCE_GROUP" --yes --no-wait
        
        echo "✅ Deletion started successfully!"
        echo ""
        echo "📝 Notes:"
        echo "  - Deletion is running in the background (--no-wait)"
        echo "  - All resources in the group will be deleted"
        echo "  - This will stop all Azure costs for these resources"
        echo ""
        echo "🔍 To check deletion status:"
        echo "  az group show --name '$RESOURCE_GROUP' --query 'properties.provisioningState'"
        echo ""
        echo "💡 To recreate resources later:"
        echo "  cd infra && ./deploy.sh $ENVIRONMENT"
        
    else
        echo "❌ Cleanup cancelled"
        exit 0
    fi
    
else
    echo "ℹ️  Resource group '$RESOURCE_GROUP' does not exist or was already deleted"
fi

echo ""
echo "🎉 Cleanup script completed!"