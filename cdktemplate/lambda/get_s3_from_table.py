# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import cfnresponse
import boto3
from botocore.exceptions import ClientError

glue = boto3.client("glue")

def handler(event, context):

	print("Received event: %s" % event)
	request_type = event["RequestType"]
	if request_type == "Create": return create_resource(event, context)
	elif request_type == "Update": return update_resource(event, context)
	elif request_type == "Delete": return delete_resource(event, context)
	else :
		# Unknown RequestType
		print("Invalid request type: %s." % request_type)
		cfnresponse.send(event, context, cfnresponse.FAILED, {})

def create_resource(event, context):

	glue_table = event["ResourceProperties"]["GlueTable"]
	glue_database = event["ResourceProperties"]["GlueDatabase"]

	try:
		response = glue.get_table(
			DatabaseName=glue_database,
			Name=glue_table
		)

		location = response["Table"]["StorageDescriptor"]["Location"]
		print(location)

		table_bucket = location.split("/")[2]
		table_prefix = "/".join(location.split("/")[3:])

		response = {
			"TableBucket" : table_bucket,
			"TablePrefix" : table_prefix
		}

		cfnresponse.send(event, context, cfnresponse.SUCCESS, response)

	except ClientError as e:
		print("Unexpected error: %s" % e)
		cfnresponse.send(event, context, cfnresponse.FAILED, {})

def update_resource(event, context):

	glue_table = event["ResourceProperties"]["GlueTable"]
	glue_database = event["ResourceProperties"]["GlueDatabase"]
	
	try:
		response = glue.get_table(
			DatabaseName=glue_database,
			Name=glue_table
		)

		location = response["Table"]["StorageDescriptor"]["Location"]
		print(location)

		table_bucket = location.split("/")[2]
		table_prefix = "/".join(location.split("/")[3:])

		response = {
			"TableBucket" : table_bucket,
			"TablePrefix" : table_prefix
		}

		cfnresponse.send(event, context, cfnresponse.SUCCESS, response)
		
	except ClientError as e:
		print("Unexpected error: %s." % e)
		cfnresponse.send(event, context, cfnresponse.FAILED, {})

def delete_resource(event, context):

	try:
		cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
		
	except ClientError as e:
		print("Unexpected error: %s." % e)
		cfnresponse.send(event, context, cfnresponse.FAILED, {})