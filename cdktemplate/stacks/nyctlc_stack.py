# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import ( 
	aws_lakeformation as lf,
	aws_glue as glue,
	aws_s3 as s3,
	aws_iam as iam,
	aws_lambda as _lambda,
	core
)
import os

# https://aws.amazon.com/blogs/big-data/build-and-automate-a-serverless-data-lake-using-an-aws-glue-trigger-for-the-data-catalog-and-etl-jobs/

BUCKET_ARN = os.environ["NYC_TLC_BUCKET_ARN"]
OBJECT = os.environ["NYC_TLC_OBJECT"]

class NycTlcDatasetStack(core.Stack):

	def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
		super().__init__(scope, id, **kwargs)

	# CloudFormation Parameters

		glue_db_name = core.CfnParameter(self, "GlueDatabaseNameNycTlc", 
				type="String",
				description="Name of Glue Database to be created for NYC TLC.",
				allowed_pattern="[\w-]+",
				default = "nyc_tlc_db"
			)

		glue_table_name = core.CfnParameter(self, "GlueTableNameNycTlc", 
				type="String",
				description="Name of Glue Table to be created for NYC TLC.",
				allowed_pattern="[\w-]+",
				default = "nyc_tlc_table"
			)

		self.template_options.description = "\
This template deploys the dataset containing New York City Taxi and Limousine Commission (TLC) Trip Record Data.\n \
Sample data is copied from the public dataset into a local S3 bucket, a database and table are created in AWS Glue, \
and the S3 location is registered with AWS Lake Formation."

		self.template_options.metadata = {
			"AWS::CloudFormation::Interface": {
				"License": "MIT-0"
			}
		}
	# Create S3 bucket for storing a copy of the Dataset locally in the AWS Account

		local_dataset_bucket = s3.Bucket(self, "LocalNycTlcBucket",
			block_public_access = s3.BlockPublicAccess(
				block_public_acls=True, 
				block_public_policy=True, 
				ignore_public_acls=True, 
				restrict_public_buckets=True),
			removal_policy = core.RemovalPolicy.DESTROY)

		public_dataset_bucket = s3.Bucket.from_bucket_arn(self, "PublicDatasetBucket", BUCKET_ARN)

		with open("lambda/s3_copy.py", encoding="utf8") as fp:
			s3_copy_code = fp.read()

		s3_copy_execution_role = iam.Role(self, "S3CopyHandlerServiceRole",
			assumed_by = iam.ServicePrincipal('lambda.amazonaws.com'),
			managed_policies = [
				iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
			],
			inline_policies = { "S3CopyHandlerRoleInlinePolicy" : iam.PolicyDocument( 
				statements = [
					iam.PolicyStatement(
						effect=iam.Effect.ALLOW,
						actions=[
							"s3:Get*"
						],
						resources=[
							public_dataset_bucket.bucket_arn,
							public_dataset_bucket.arn_for_objects("*")
						]),
					iam.PolicyStatement(
						effect=iam.Effect.ALLOW,
						actions=[
							"s3:PutObject",
							"s3:GetObject",
							"s3:DeleteObject"
						],
						resources=[local_dataset_bucket.arn_for_objects("*")]
						)
					]
				) }
			)

		s3_copy_fn = _lambda.Function(self, "S3CopyHandler", 
			runtime = _lambda.Runtime.PYTHON_3_7,
			code = _lambda.InlineCode.from_inline(s3_copy_code),
			handler = "index.handler",
			role =  s3_copy_execution_role,
			timeout = core.Duration.seconds(600)
		)

		s3_copy = core.CustomResource(self, "S3Copy", 
			service_token = s3_copy_fn.function_arn,
			resource_type = "Custom::S3Copy",
			properties = {
				"PublicDatasetBucket": public_dataset_bucket.bucket_name,
				"LocalDatasetBucket" : local_dataset_bucket.bucket_name,
				"PublicDatasetObject": OBJECT,
				"LocalDatasetPrefix": glue_table_name.value_as_string
			} 
		)	

	# Create Database, Table and Partitions for Amazon Reviews

		lakeformation_resource = lf.CfnResource(self, "LakeFormationResource", 
			resource_arn = local_dataset_bucket.bucket_arn, 
			use_service_linked_role = True)

		lakeformation_resource.node.add_dependency(s3_copy)

		cfn_glue_db = glue.CfnDatabase(self, "GlueDatabase", 
			catalog_id = core.Aws.ACCOUNT_ID,
			database_input = glue.CfnDatabase.DatabaseInputProperty(
				name = glue_db_name.value_as_string, 
				location_uri=local_dataset_bucket.s3_url_for_object(),
			)
		)

		nyc_tlc_table = glue.CfnTable(self, "GlueTableNycTlc", 
			catalog_id = cfn_glue_db.catalog_id,
			database_name = glue_db_name.value_as_string,
			table_input = glue.CfnTable.TableInputProperty(
				description = "New York City Taxi and Limousine Commission (TLC) Trip Record Data",
				name = glue_table_name.value_as_string,
				parameters = {
					"skip.header.line.count": "1",
					"compressionType": "none",
					"classification": "csv",
					"delimiter": ",",
					"typeOfData": "file"
				},
				storage_descriptor = glue.CfnTable.StorageDescriptorProperty(
					columns = [
						{"name":"vendorid","type":"bigint"},
						{"name":"lpep_pickup_datetime","type":"string"},
						{"name":"lpep_dropoff_datetime","type":"string"},
						{"name":"store_and_fwd_flag","type":"string"},
						{"name":"ratecodeid","type":"bigint"},
						{"name":"pulocationid","type":"bigint"},
						{"name":"dolocationid","type":"bigint"},
						{"name":"passenger_count","type":"bigint"},
						{"name":"trip_distance","type":"double"},
						{"name":"fare_amount","type":"double"},
						{"name":"extra","type":"double"},
						{"name":"mta_tax","type":"double"},
						{"name":"tip_amount","type":"double"},
						{"name":"tolls_amount","type":"double"},
						{"name":"ehail_fee","type":"string"},
						{"name":"improvement_surcharge","type":"double"},
						{"name":"total_amount","type":"double"},
						{"name":"payment_type","type":"bigint"},
						{"name":"trip_type","type":"bigint"},
						{"name":"congestion_surcharge","type":"double"}],
					location = local_dataset_bucket.s3_url_for_object() + "/" + glue_table_name.value_as_string + "/",
					input_format = "org.apache.hadoop.mapred.TextInputFormat",
					output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
					compressed = False,
					serde_info = glue.CfnTable.SerdeInfoProperty( 
						serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
						parameters = {
							"field.delim": ","
						}
					)
				),
				table_type = "EXTERNAL_TABLE"
			)
		)

		nyc_tlc_table.node.add_dependency(cfn_glue_db)

		core.CfnOutput(self, "LocalNycTlcBucketOutput", 
			value=local_dataset_bucket.bucket_name, 
			description="S3 Bucket created to store the dataset")

		core.CfnOutput(self, "GlueDatabaseOutput", 
			value=cfn_glue_db.ref, 
			description="Glue DB created to host the dataset table")

		core.CfnOutput(self, "GlueTableNycTlcOutput", 
			value=nyc_tlc_table.ref, 
			description="Glue Table created to host the dataset")
