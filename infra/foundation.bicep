targetScope = 'resourceGroup'
param location string
param environment string
param suffix string
param sqlAdminName string
param sqlAdminObjectId string
param exporterImage string
param actionGroupId string

var tags = { project: 'order-integration-middleware', environment: environment }
var subs = ['fulfillment', 'audit']
var blobWriter = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
var messageReader = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4f6d3b9b-027b-4f4c-9142-0e5a2a2247e0')
var withExporter = !empty(exporterImage)

resource lake 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: 'orddata${suffix}'
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
resource blobs 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: lake
  name: 'default'
  properties: {
    deleteRetentionPolicy: { enabled: true, days: 7 }
    containerDeleteRetentionPolicy: { enabled: true, days: 7 }
  }
}
resource orders 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobs
  name: 'orders'
  properties: { publicAccess: 'None' }
}
resource tables 'Microsoft.Storage/storageAccounts/tableServices@2023-05-01' = {
  parent: lake
  name: 'default'
}
resource effects 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tables
  name: 'OrderEffects'
}

resource server 'Microsoft.Sql/servers@2023-08-01-preview' = {
  name: 'orders-sql-${suffix}'
  location: location
  tags: tags
  properties: {
    version: '12.0'
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
    administrators: {
      administratorType: 'ActiveDirectory'
      principalType: 'Group'
      login: sqlAdminName
      sid: sqlAdminObjectId
      tenantId: tenant().tenantId
      azureADOnlyAuthentication: true
    }
  }
}
resource db 'Microsoft.Sql/servers/databases@2023-08-01-preview' = {
  parent: server
  name: 'NorthwindLab'
  location: location
  sku: { name: 'Basic', tier: 'Basic', capacity: 5 }
  properties: { maxSizeBytes: 2147483648 }
}

resource bus 'Microsoft.ServiceBus/namespaces@2024-01-01' = {
  name: 'orders-sb-${suffix}'
  location: location
  tags: tags
  sku: { name: 'Standard', tier: 'Standard' }
  properties: { disableLocalAuth: true, minimumTlsVersion: '1.2', publicNetworkAccess: 'Enabled' }
}
resource topic 'Microsoft.ServiceBus/namespaces/topics@2024-01-01' = {
  parent: bus
  name: 'order-events'
  properties: {
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    defaultMessageTimeToLive: 'P2D'
    enableBatchedOperations: true
    enablePartitioning: false
    maxSizeInMegabytes: 1024
  }
}
resource subscriptions 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2024-01-01' = [for sub in subs: {
  parent: topic
  name: sub
  properties: {
    deadLetteringOnMessageExpiration: true
    defaultMessageTimeToLive: 'P2D'
    maxDeliveryCount: 5
    lockDuration: 'PT2M'
    enableBatchedOperations: true
    requiresSession: false
  }
}]

resource workers 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = [for sub in subs: {
  name: 'orders-${sub}-${suffix}'
  location: location
  tags: tags
}]
resource subReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for (sub, i) in subs: {
  name: guid(subscriptions[i].id, workers[i].id, messageReader)
  scope: subscriptions[i]
  properties: { principalId: workers[i].properties.principalId, principalType: 'ServicePrincipal', roleDefinitionId: messageReader }
}]
resource exporter 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: 'orders-export-${suffix}'
  location: location
  tags: tags
}
resource exportBlobRights 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(orders.id, exporter.id, blobWriter)
  scope: orders
  properties: { principalId: exporter.properties.principalId, principalType: 'ServicePrincipal', roleDefinitionId: blobWriter }
}

resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: 'orders-logs-${suffix}'
  location: location
  tags: tags
  properties: { sku: { name: 'PerGB2018' }, retentionInDays: 30, workspaceCapping: { dailyQuotaGb: 1 } }
}
resource insights 'Microsoft.Insights/components@2020-02-02' = {
  name: 'orders-appi-${suffix}'
  location: location
  kind: 'web'
  tags: tags
  properties: { Application_Type: 'web', WorkspaceResourceId: logs.id }
}
resource busDiagnostic 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  scope: bus
  name: 'orders-broker-diagnostics'
  properties: { workspaceId: logs.id, logs: [{ category: 'OperationalLogs', enabled: true }], metrics: [{ category: 'AllMetrics', enabled: true }] }
}
resource dlqAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'orders-deadletters-${suffix}'
  location: 'global'
  properties: {
    description: 'One or more subscription messages reached a dead-letter queue.'
    severity: 2
    enabled: true
    scopes: [bus.id]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [{ name: 'DLQBacklog', metricName: 'DeadletteredMessages', metricNamespace: 'Microsoft.ServiceBus/namespaces', timeAggregation: 'Maximum', operator: 'GreaterThan', threshold: 0, criterionType: 'StaticThresholdCriterion' }]
    }
    actions: empty(actionGroupId) ? [] : [{ actionGroupId: actionGroupId }]
  }
}

resource appEnv 'Microsoft.App/managedEnvironments@2024-03-01' = if (withExporter) {
  name: 'orders-ca-${suffix}'
  location: location
  tags: tags
  properties: { appLogsConfiguration: { destination: 'azure-monitor' } }
}
resource exportJob 'Microsoft.App/jobs@2024-03-01' = if (withExporter) {
  name: 'orders-export-${suffix}'
  location: location
  tags: tags
  identity: { type: 'UserAssigned', userAssignedIdentities: { '${exporter.id}': {} } }
  properties: {
    environmentId: appEnv!.id
    configuration: { triggerType: 'Manual', replicaTimeout: 1800, replicaRetryLimit: 0, manualTriggerConfig: { parallelism: 1, replicaCompletionCount: 1 } }
    template: {
      containers: [{
        name: 'export'
        image: exporterImage
        resources: { cpu: json('0.5'), memory: '1Gi' }
        env: [
          { name: 'AZURE_CLIENT_ID', value: exporter.properties.clientId }
          { name: 'ORDERS_SQL_SERVER', value: server.properties.fullyQualifiedDomainName }
          { name: 'ORDERS_SQL_DATABASE', value: db.name }
          { name: 'ORDERS_STORAGE_ACCOUNT', value: lake.name }
          { name: 'ORDERS_CONTAINER', value: orders.name }
          { name: 'ORDERS_PREFIX', value: 'events/' }
          { name: 'ORDERS_EXPORT_ENABLED', value: 'false' }
        ]
      }]
    }
  }
}
resource exportDiagnostic 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (withExporter) {
  scope: appEnv!
  name: 'exporter-logs'
  properties: { workspaceId: logs.id, logs: [{ category: 'ContainerAppConsoleLogs', enabled: true }, { category: 'ContainerAppSystemLogs', enabled: true }] }
}

output dataAccount string = lake.name
output broker string = bus.name
output insights string = insights.name
output logs string = logs.name
output logicPrincipalIds array = [for (sub,i) in subs: workers[i].properties.principalId]
output logicResourceIds array = [for (sub,i) in subs: workers[i].id]
output names object = { sourceServer: server.properties.fullyQualifiedDomainName, sourceDatabase: db.name, data: lake.name, broker: bus.name, exporterIdentity: exporter.name, logs: logs.name }
