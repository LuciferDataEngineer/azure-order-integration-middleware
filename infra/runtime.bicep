targetScope = 'resourceGroup'
param location string
param suffix string
param dataAccountName string
param namespaceName string
param workflowPrincipals array
param applicationClientId string
param insightsName string

var tags = { project: 'order-integration-middleware', environment: 'dev' }
var hostOwner = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b')
var reader = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
var sender = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '69a216fc-b8fb-44d8-bc22-1f3c2cd27a39')
var tableWriter = subscriptionResourceId('Microsoft.Authorization/roleDefinitions','0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
resource dataAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = { name: dataAccountName }
resource orders 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' existing = { name: '${dataAccount.name}/default/orders' }
resource effects 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' existing = { name: '${dataAccount.name}/default/OrderEffects' }
resource ns 'Microsoft.ServiceBus/namespaces@2024-01-01' existing = { name: namespaceName }
resource topic 'Microsoft.ServiceBus/namespaces/topics@2024-01-01' existing = { parent: ns, name: 'order-events' }
resource insights 'Microsoft.Insights/components@2020-02-02' existing = { name: insightsName }

resource hostStore 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: 'ordhost${suffix}'
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
  }
}
resource hostBlobs 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: hostStore
  name: 'default'
}
resource deploymentStore 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: hostBlobs
  name: 'deployment'
  properties: { publicAccess: 'None' }
}
resource plan 'Microsoft.Web/serverfarms@2024-04-01' = {
  name: 'orders-flex-${suffix}'
  location: location
  tags: tags
  sku: { name: 'FC1', tier: 'FlexConsumption' }
  kind: 'functionapp'
  properties: { reserved: true }
}
resource api 'Microsoft.Web/sites@2024-04-01' = {
  name: 'orders-api-${suffix}'
  location: location
  kind: 'functionapp,linux'
  tags: tags
  identity: { type: 'SystemAssigned' }
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    publicNetworkAccess: 'Enabled'
    functionAppConfig: {
      deployment: { storage: { type: 'blobContainer', value: '${hostStore.properties.primaryEndpoints.blob}${deploymentStore.name}', authentication: { type: 'SystemAssignedIdentity' } } }
      runtime: { name: 'python', version: '3.12' }
      scaleAndConcurrency: { maximumInstanceCount: 40, instanceMemoryMB: 2048 }
    }
    siteConfig: {
      minTlsVersion: '1.2'
      ftpsState: 'Disabled'
      appSettings: [
        { name: 'AzureWebJobsStorage__accountName', value: hostStore.name }
        { name: 'AzureWebJobsStorage__credential', value: 'managedidentity' }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: insights.properties.ConnectionString }
        { name: 'ORDERS_STORAGE_ACCOUNT', value: dataAccount.name }
        { name: 'ORDERS_CONTAINER', value: 'orders' }
        { name: 'ORDERS_PREFIX', value: 'events/' }
        { name: 'ORDERS_SERVICEBUS_NAMESPACE', value: '${ns.name}.servicebus.windows.net' }
        { name: 'ORDERS_TOPIC', value: topic.name }
        { name: 'ORDERS_EFFECTS_TABLE', value: 'OrderEffects' }
        { name: 'ORDERS_FULFILLMENT_PRINCIPAL_ID', value: workflowPrincipals[0] }
        { name: 'ORDERS_AUDIT_PRINCIPAL_ID', value: workflowPrincipals[1] }
      ]
    }
  }
}
resource auth 'Microsoft.Web/sites/config@2024-04-01' = {
  parent: api
  name: 'authsettingsV2'
  properties: {
    platform: { enabled: true }
    globalValidation: {
      requireAuthentication: true
      unauthenticatedClientAction: 'Return401'
      excludedPaths: ['/runtime/webhooks/EventGrid']
    }
    identityProviders: {
      azureActiveDirectory: {
        enabled: true
        registration: { clientId: applicationClientId, openIdIssuer: '${environment().authentication.loginEndpoint}${tenant().tenantId}/v2.0' }
        validation: {
          allowedAudiences: ['api://${applicationClientId}', applicationClientId]
          defaultAuthorizationPolicy: { allowedPrincipals: { identities: workflowPrincipals } }
        }
      }
    }
    httpSettings: { requireHttps: true }
  }
}
resource hostRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(hostStore.id, api.id, hostOwner)
  scope: hostStore
  properties: { principalId: api.identity.principalId, principalType: 'ServicePrincipal', roleDefinitionId: hostOwner }
}
resource blobRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(orders.id, api.id, reader)
  scope: orders
  properties: { principalId: api.identity.principalId, principalType: 'ServicePrincipal', roleDefinitionId: reader }
}
resource sendRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(topic.id, api.id, sender)
  scope: topic
  properties: { principalId: api.identity.principalId, principalType: 'ServicePrincipal', roleDefinitionId: sender }
}
resource effectsRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(effects.id, api.id, tableWriter)
  scope: effects
  properties: { principalId: api.identity.principalId, principalType: 'ServicePrincipal', roleDefinitionId: tableWriter }
}

output appName string = api.name
