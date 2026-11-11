"""Compile Bicep locally and verify selected cross-service contracts."""
import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent


def check(condition, detail):
    if not condition:
        raise ValueError(detail)


def resource(template, kind):
    return next(r for r in template['resources'] if r['type'] == kind)


def verify(arm, parameters):
    modules = arm['resources']
    check(len(modules)==3 and all(m['type']=='Microsoft.Resources/deployments' and 'condition' in m for m in modules),
          'Every stage must be conditional')
    check(arm['parameters']['phase']['defaultValue']=='off' and parameters['parameters']['phase']['value']=='off',
          'Lab must be off by default')
    templates = [m['properties']['template'] for m in modules]
    base = next(t for t in templates if any(r['type']=='Microsoft.Sql/servers' for r in t['resources']))
    server = resource(base, 'Microsoft.Sql/servers')
    check(server['properties']['administrators']['azureADOnlyAuthentication'], 'Unexpected SQL auth mode')
    check(resource(base,'Microsoft.ServiceBus/namespaces')['sku']['name']=='Standard','Requires topics')
    topic = resource(base,'Microsoft.ServiceBus/namespaces/topics')
    subscriptions = resource(base,'Microsoft.ServiceBus/namespaces/topics/subscriptions')
    check(base['variables']['subs']==['fulfillment','audit'] and subscriptions['properties']['maxDeliveryCount']==5,
          'Expected isolated subscriptions with bounded retry')
    check("'order-events'" in topic['name'] and topic['properties']['requiresDuplicateDetection'],
          'Expected deduplicating order-events topic')
    job = resource(base,'Microsoft.App/jobs')
    env = {x['name']:x['value'] for x in job['properties']['template']['containers'][0]['env']}
    check('condition' in job and job['properties']['configuration']['triggerType']=='Manual'
          and env['ORDERS_EXPORT_ENABLED']=='false', 'Export must be gated')

    runtime = next(t for t in templates if any(r['type']=='Microsoft.Web/sites' for r in t['resources']))
    site = resource(runtime,'Microsoft.Web/sites')
    appenv = {x['name']:x['value'] for x in site['properties']['siteConfig']['appSettings']}
    check(appenv['ORDERS_EFFECTS_TABLE']=='OrderEffects' and appenv['ORDERS_TOPIC']=='order-events'
          and appenv['ORDERS_CONTAINER']=='orders' and appenv['ORDERS_PREFIX']=='events/', 'Invalid binding')
    check(site['properties']['functionAppConfig']['runtime']=={'name':'python','version':'3.12'},'Runtime differs')
    auth = resource(runtime,'Microsoft.Web/sites/config')['properties']
    check(auth['globalValidation']=={'requireAuthentication':True,'unauthenticatedClientAction':'Return401',
                                    'excludedPaths':['/runtime/webhooks/EventGrid']}, 'Review HTTP auth boundary')
    principals=auth['identityProviders']['azureActiveDirectory']['validation']['defaultAuthorizationPolicy']['allowedPrincipals']
    check('workflowPrincipals' in principals['identities'], 'Require allowlisted workflow identities')

    wired = next(t for t in templates if any(r['type']=='Microsoft.Logic/workflows' for r in t['resources']))
    eventsub=resource(wired,'Microsoft.EventGrid/eventSubscriptions')['properties']
    check(eventsub['filter']['includedEventTypes']==['Microsoft.Storage.BlobCreated'] and
          eventsub['filter']['subjectBeginsWith'].endswith('/orders/blobs/events/') and
          eventsub['destination']['endpointType']=='AzureFunction', 'Unexpected Event Grid destination or filter')
    logic = resource(wired,'Microsoft.Logic/workflows')
    check('copy' in logic and wired['variables']['subs']==['fulfillment','audit'] and
          logic['properties']['state']=='Disabled', 'Require disabled per-subscription flows')
    auth_params=logic['properties']['parameters']
    check(all(auth_params[key]['value']['type']=='ManagedServiceIdentity' for key in ('brokerAuth','functionAuth'))
          and auth_params['brokerAuth']['value']['audience']=='https://servicebus.azure.net',
          'Workflow authentication requires service-specific managed identity audiences')
    definition=json.loads((ROOT/'consumer.workflow.json').read_text())
    emitted=logic['properties']['definition']
    if isinstance(emitted,str):
        var=re.fullmatch(r"\[variables\('([^']+)'\)\]",emitted)
        check(var is not None, 'Unknown embedded workflow expression')
        emitted=wired['variables'][var.group(1)]
    check(emitted==definition, 'Compiled workflow must contain the reviewed definition')
    check(definition['actions']['Peek']['inputs']['method']=='POST'
          and 'head?timeout=5' in definition['actions']['Peek']['inputs']['uri'], 'Expected peek-lock')
    msg=definition['actions']['Handle_received_message']
    check("201)" in msg['expression'] and msg['runAfter']=={'Peek':['Succeeded']}, 'Require a message')
    guard=msg['actions']['Validate_lock_location']
    check('startsWith' in guard['expression'] and 'brokerBase' in guard['expression'], 'Check settlement origin')
    settle=guard['actions']['Complete_after_commit']
    check(settle['runAfter']=={'Apply':['Succeeded']} and "200)" in settle['expression']
          and settle['actions']['Settle']['inputs']['method']=='DELETE', 'Complete only after durable apply')
    return {'conditional_modules':len(modules),'subscription_names':base['variables']['subs'],
            'topic_duplicate_window':topic['properties']['duplicateDetectionHistoryTimeWindow'],
            'max_delivery_count':subscriptions['properties']['maxDeliveryCount'],'initial_workflow_state':'Disabled',
            'phase_default':'off','function_runtime':'Python 3.12','workflow_settlement':'apply succeeds with HTTP 200 then DELETE locked URI'}


if __name__=='__main__':
    args=argparse.ArgumentParser(description=__doc__)
    args.add_argument('--bicep',default=shutil.which('bicep') or str(Path.home()/'.azure/bin/bicep'))
    exe=args.parse_args().bicep
    with tempfile.TemporaryDirectory() as temp:
        a=Path(temp)/'main.json'
        d=Path(temp)/'dev.json'
        subprocess.run([exe,'build',str(ROOT/'main.bicep'),'--outfile',str(a)],check=True)
        subprocess.run([exe,'build-params',str(ROOT/'dev.bicepparam'),'--outfile',str(d)],check=True)
        report=verify(json.loads(a.read_text()), json.loads(d.read_text()))
        blocked=subprocess.run([exe,'build-params',str(ROOT/'prod.bicepparam'),'--outfile',str(Path(temp)/'prod.json')],
                               capture_output=True,text=True)
        check(blocked.returncode and 'BCP033' in blocked.stderr and 'prod' in blocked.stderr,
              'Production parameter file must fail its dev-only guard')
        report.update(dev_parameters_compiled=True, production_parameters_rejected=True,
                      bicep_version=subprocess.check_output([exe,'--version'],text=True).strip())
        print(json.dumps(report,indent=2))
