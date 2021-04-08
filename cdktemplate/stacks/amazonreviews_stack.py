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

BUCKET_ARN = os.environ["AMAZON_REVIEWS_BUCKET_ARN"]
OBJECT = os.environ["AMAZON_REVIEWS_OBJECT"]

class AmazonReviewsDatasetStack(core.Stack):

	def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
		super().__init__(scope, id, **kwargs)

	# CloudFormation Parameters

		glue_db_name = core.CfnParameter(self, "GlueDatabaseNameAmazonReviews", 
				type="String",
				description="Name of Glue Database to be created for Amazon Reviews.",
				allowed_pattern="[\w-]+",
				default = "amazon_reviews_db"
			)

		glue_table_name = core.CfnParameter(self, "GlueTableNameAmazonReviews", 
				type="String",
				description="Name of Glue Table to be created for Amazon Reviews.",
				allowed_pattern="[\w-]+",
				default = "amazon_reviews_table"
			)

		self.template_options.description = "\
This template deploys the dataset containing Amazon Customer Reviews (a.k.a. Product Reviews).\n \
Sample data is copied from the public dataset into a local S3 bucket, a database and table are created in AWS Glue, \
and the S3 location is registered with AWS Lake Formation."

		self.template_options.metadata = {
			"AWS::CloudFormation::Interface": {
				"License": "MIT-0"
			}
		}
	# Create S3 bucket for storing a copy of the Dataset locally in the AWS Account

		local_dataset_bucket = s3.Bucket(self, "LocalAmazonReviewsBucket",
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

		amazon_reviews_table = glue.CfnTable(self, "GlueTableAmazonReviews", 
			catalog_id = cfn_glue_db.catalog_id,
			database_name = glue_db_name.value_as_string,
			table_input = glue.CfnTable.TableInputProperty(
				description = "Amazon Customer Reviews (a.k.a. Product Reviews)",
				name = glue_table_name.value_as_string,
				parameters = {
					"skip.header.line.count": "1",
					"compressionType": "gzip",
					"classification": "csv",
					"delimiter": "\t",
					"typeOfData": "file"
				},
				storage_descriptor = glue.CfnTable.StorageDescriptorProperty(
					columns = [
						{"name": "marketplace", "type": "string"},
						{"name": "customer_id", "type": "string"},
						{"name": "review_id","type": "string"},
						{"name": "product_id","type": "string"},
						{"name": "product_parent","type": "string"},
						{"name": "product_title","type": "string"},
						{"name": "product_category","type": "string"},
						{"name": "star_rating","type": "int"},
						{"name": "helpful_votes","type": "int"},
						{"name": "total_votes","type": "int"},
						{"name": "vine","type": "string"},
						{"name": "verified_purchase","type": "string"},
						{"name": "review_headline","type": "string"},
						{"name": "review_body","type": "string"},
						{"name": "review_date","type": "string"}],
					location = local_dataset_bucket.s3_url_for_object() + "/" + glue_table_name.value_as_string + "/",
					input_format = "org.apache.hadoop.mapred.TextInputFormat",
					output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
					compressed = True,
					serde_info = glue.CfnTable.SerdeInfoProperty( 
						serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
						parameters = {
							"field.delim": "\t"
						}
					)
				),
				table_type = "EXTERNAL_TABLE"
			)
		)

		amazon_reviews_table.node.add_dependency(cfn_glue_db)

		core.CfnOutput(self, "LocalAmazonReviewsBucketOutput", 
			value=local_dataset_bucket.bucket_name, 
			description="S3 Bucket created to store the dataset")

		core.CfnOutput(self, "GlueDatabaseAmazonReviewsOutput", 
			value=cfn_glue_db.ref, 
			description="Glue DB created to host the dataset table")

		core.CfnOutput(self, "GlueTableAmazonReviewsOutput", 
			value=amazon_reviews_table.ref, 
			description="Glue Table created to host the dataset")
