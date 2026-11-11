targetScope = 'resourceGroup'

@allowed(['off', 'foundation', 'runtime', 'wired'])
param phase string = 'off'
@allowed(['dev'])
param environment string = 'dev'
param location string = 'australiaeast'
param sqlAdminName string = ''
param sqlAdminObjectId string = ''
param applicationClientId string = ''
param exporterImage string = ''
param actionGroupId string = ''

var suffix = uniqueString(resourceGroup().id, 'northwind-middleware-v1')
module base './foundation.bicep' = if (phase != 'off') {
  name: 'orders-foundation'
  params: {
    location: location
    environment: environment
    suffix: suffix
    sqlAdminName: sqlAdminName
    sqlAdminObjectId: sqlAdminObjectId
    exporterImage: exporterImage
    actionGroupId: actionGroupId
  }
}
module runtime './runtime.bicep' = if (phase == 'runtime' || phase == 'wired') {
  name: 'orders-functions'
  params: {
    location: location
    suffix: suffix
    dataAccountName: base!.outputs.dataAccount
    namespaceName: base!.outputs.broker
    workflowPrincipals: base!.outputs.logicPrincipalIds
    applicationClientId: applicationClientId
    insightsName: base!.outputs.insights
  }
}
module connections './consumers.bicep' = if (phase == 'wired') {
  name: 'orders-wiring'
  params: {
    location: location
    suffix: suffix
    dataAccountName: base!.outputs.dataAccount
    namespaceName: base!.outputs.broker
    functionAppName: runtime!.outputs.appName
    applicationClientId: applicationClientId
    workflowIdentityIds: base!.outputs.logicResourceIds
    logsName: base!.outputs.logs
  }
}

output deploymentPhase string = phase
output labResources object = phase == 'off' ? {} : base!.outputs.names
