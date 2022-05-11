import datetime
import os
import asyncio
import boto3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--profile") # typical of the onelogin SSO CLI tool https://developers.onelogin.com/api-docs/1/samples/aws-cli
parser.add_argument("--region")
parser.add_argument("--service", choices=['ses', 'SES'])
parser.add_argument("--action", choices=['get', 'delete', 'get_templates', 'delete_templates'])
parser.add_argument("--older-than-days", default=1)
parser.add_argument("--starting-token", default=None)
parser.add_argument("--debug", action='store_true')
args = parser.parse_args()

if args.profile is not None:
    LOCAL=True
DEBUG = args.debug
if DEBUG:
    print(f'{{args: {args.__dict__}}}')


if args.service.lower() in ['ses']:
    service = 'ses'
    paginator_action = 'list_templates'
if args.action.lower() in ['get', 'get_templates', 'get_template']: action = 'get_template'
elif args.action.lower() in ['delete', 'delete_templates', 'delete_template']: action = 'delete_template'
retention_cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=int(args.older_than_days))

if LOCAL:
    session = boto3.Session(profile_name=args.profile)
    client = session.client(service_name=service, region_name=args.region)
else:
    client = boto3.client(service_name=service, region_name=args.region)


# setup a 'connection' to the service to iterate over batches using the NextToken
paginator = client.get_paginator(paginator_action)


# async functions: batch_operation() iterates over the object names and unpacks each as a separate function call
# asyncio.gather runs these functions concurrently and returns a list of their results when the last one returns
#
async def single_operation(object_name, operation='get_template'):
    if operation == 'delete_template':
        result = client.delete_template(TemplateName=object_name)
    else:
        result = client.get_template(TemplateName=object_name)
        if DEBUG:
            print(f"would have deleted: {result.get('Template').get('TemplateName')}")
    
    return result

async def batch_operation(object_name_list, operation='get_template'):
    # note to myself on how to visualize this splat comprehension -- for i, make i one parameter in one call of single_function()
    #     await (
    #         single_operation('obj1', 'get_template'),
    #         single_operation('obj2', 'get_template'),
    #         ...and so on
    #     )
    #
    awaitable = await asyncio.gather(*[single_operation(object_name, operation) for object_name in object_name_list])
    return awaitable

# quickly get a big list of all the objects and apply client-side filtering
# for future: can accept a StartingToken if the page is already known
def iterate(paginator=paginator, starting_token=args.starting_token):
    filtered_object_list = []

    page_iterator = paginator.paginate(PaginationConfig={'StartingToken': starting_token})
    for page in [pages.get('TemplatesMetadata') for pages in page_iterator]:
        for object in page:
            if object.get('CreatedTimestamp') < retention_cutoff:
                if DEBUG:
                    print(f"included: {object.get('Name')} having: {object.get('CreatedTimestamp')}")
                filtered_object_list.append(object.get('Name'))

    return filtered_object_list

if __name__ == '__main__':
    asyncio.run(batch_operation(object_name_list=iterate(), operation=action))