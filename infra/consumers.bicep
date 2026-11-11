targetScope = 'resourceGroup'
param location string
param suffix string
param dataAccountName string
param namespaceName string
param functionAppName string
param applicationClientId string
param workflowIdentityIds array
param logsName string

var subs = ['fulfillment','audit']
resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' existing = { name: logsName }
resource account 'Microsoft.Storage/storageAccounts@2023-05-01' existing = { name: dataAccountName }
resource app 'Microsoft.Web/sites@2024-04-01' existing = { name: functionAppName }
resource publishFunction 'Microsoft.Web/sites/functions@2024-04-01' existing = { parent: app, name: 'publish_order' }
resource onCreated 'Microsoft.EventGrid/eventSubscriptions@2022-06-15' = {
  name: 'order-blob-created'
  scope: account
  properties: {
    eventDeliverySchema: 'EventGridSchema'
    filter: {
      includedEventTypes: ['Microsoft.Storage.BlobCreated']
      subjectBeginsWith: '/blobServices/default/containers/orders/blobs/events/'
      subjectEndsWith: '.json'
      isSubjectCaseSensitive: true
    }
    destination: {
      endpointType: 'AzureFunction'
      properties: { resourceId: publishFunction.id, maxEventsPerBatch: 1, preferredBatchSizeInKilobytes: 64 }
    }
    retryPolicy: { maxDeliveryAttempts: 10, eventTimeToLiveInMinutes: 1440 }
  }
}
resource consumers 'Microsoft.Logic/workflows@2019-05-01' = [for (sub,i) in subs: {
  name: 'orders-${sub}-${suffix}'
  location: location
  tags: { project: 'order-integration-middleware', environment: 'dev' }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${workflowIdentityIds[i]}': {} }
  }
  properties: {
    state: 'Disabled'
    definition: loadJsonContent('consumer.workflow.json')
    parameters: {
      brokerBase: { value: 'https://${namespaceName}.servicebus.windows.net/order-events/subscriptions/${sub}/messages/' }
      applyUri: { value: 'https://${app.properties.defaultHostName}/api/apply/${sub}' }
      brokerAuth: { value: { type: 'ManagedServiceIdentity', identity: workflowIdentityIds[i], audience: 'https://servicebus.azure.net' } }
      functionAuth: { value: { type: 'ManagedServiceIdentity', identity: workflowIdentityIds[i], audience: 'api://${applicationClientId}' } }
    }
  }
}]
resource consumerLogs 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = [for (sub,i) in subs: {
  scope: consumers[i]
  name: 'order-consumer-runs'
  properties: {
    workspaceId: logs.id
    logs: [{ category: 'WorkflowRuntime', enabled: true }]
    metrics: [{ category: 'AllMetrics', enabled: true }]
  }
}]
