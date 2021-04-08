# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import cfnresponse
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

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

	public_dataset_bucket = event["ResourceProperties"]["PublicDatasetBucket"]
	local_dataset_bucket = event["ResourceProperties"]["LocalDatasetBucket"]
	public_dataset_object = event["ResourceProperties"]["PublicDatasetObject"]
	local_dataset_prefix = event["ResourceProperties"]["LocalDatasetPrefix"]

	local_dataset_object = local_dataset_prefix + "/" + public_dataset_object.split("/")[1]

	try:
		s3.copy(
			{'Bucket': public_dataset_bucket, 'Key': public_dataset_object}, 
			local_dataset_bucket, 
			local_dataset_object
		)

		cfnresponse.send(event, context, cfnresponse.SUCCESS, {})

	except ClientError as e:
		print("Unexpected error: %s" % e)
		cfnresponse.send(event, context, cfnresponse.FAILED, {})

def update_resource(event, context):

	try:
		cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
		
	except ClientError as e:
		print("Unexpected error: %s." % e)
		cfnresponse.send(event, context, cfnresponse.FAILED, {})

def delete_resource(event, context):
	
	local_dataset_bucket = event["ResourceProperties"]["LocalDatasetBucket"]
	public_dataset_object = event["ResourceProperties"]["PublicDatasetObject"]
	local_dataset_prefix = event["ResourceProperties"]["LocalDatasetPrefix"]

	local_dataset_object = local_dataset_prefix + "/" + public_dataset_object.split("/")[1]

	try:
		# Verify if object exists. If not, exit with SUCCESS too.

		s3.head_object(
			Bucket=local_dataset_bucket,
			Key=local_dataset_object
		)

		s3.delete_object(
		    Bucket=local_dataset_bucket,
		    Key=local_dataset_object
		)

		cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
		
	except ClientError as e:

		if e.response["Error"]["Code"] == "NoSuchKey":
			# Object not found, nothing to do
			cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
		else:
			print("Unexpected error: %s." % e)
			cfnresponse.send(event, context, cfnresponse.FAILED, {})