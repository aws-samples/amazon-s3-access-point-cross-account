# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import ( 
	aws_lakeformation as lf,
	aws_glue as glue,
	aws_s3 as s3,
	aws_iam as iam,
	aws_lambda as _lambda,
	aws_cloudformation as cfn,
	core
)
import os

class S3AccessPointFromTable(core.Stack):

	def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
		super().__init__(scope, id, **kwargs)

	# CloudFormation Parameters

		glue_db_name = core.CfnParameter(self, "GlueDatabaseName", 
				type="String",
				description="Glue Database where the Table belongs.",
				allowed_pattern="[\w-]+",
			)

		glue_table_name = core.CfnParameter(self, "GlueTableName", 
				type="String",
				description="Glue Table where access will be granted.",
				allowed_pattern="[\w-]+",
			)

		grantee_role_arn = core.CfnParameter(self, "GranteeIAMRoleARN", 
				type="String",
				description="IAM Role's ARN.",
				allowed_pattern="arn:(aws[a-zA-Z-]*)?:iam::\d{12}:role\/?[a-zA-Z0-9_+=,.@\-]+"
			)
		
		grantee_vpc = core.CfnParameter(self, "GranteeVPC", 
				type="String",
				description="VPC ID from where the S3 access point will be accessed.",
				allowed_pattern="vpc-[a-zA-Z0-9]+"
			)

		is_lakeformation = core.CfnParameter(self, "LakeFormationParam", 
				type="String",
				description="If Lake Formation is used, the stack must be deployed using an IAM role with Lake Formation Admin permissions.",
				allowed_values=[
					"Yes",
					"No"
				]
			)

	# CloudFormation Parameter Groups

		self.template_options.description = "\
This template deploys an S3 Access Point which provides a given IAM Role \
access to the underlying data location for a given Glue Table.\n\
Main use case for this template is to grant an ETL process in another AWS Account, \
access to the S3 objects (e.g., Parquet files) associated to a Glue Table."

		self.template_options.metadata = {
		
		"AWS::CloudFormation::Interface": {
			"License": "MIT-0",
			"ParameterGroups": [
				{
					"Label": { "default": "Lake Formation (Producer Account)" },
					"Parameters": [ is_lakeformation.logical_id ]
				},
				{
					"Label": { "default": "Source Data Catalog Resource (Producer Account)" },
					"Parameters": [ glue_db_name.logical_id, glue_table_name.logical_id ]
				},
				{
					"Label": { "default": "Grantee IAM Role (Consumer Account)" },
					"Parameters": [ grantee_role_arn.logical_id, grantee_vpc.logical_id ]
				}
			],
			"ParameterLabels": {
				is_lakeformation.logical_id: {
					"default": "Are data permissions managed by Lake Formation?"
				},
				glue_db_name.logical_id: {
					"default": "What is the Glue DB Name for the Table?"
				},
				glue_table_name.logical_id: {
					"default": "What is the Glue Table Name?"
				},
				grantee_role_arn.logical_id: {
					"default": "What is the ARN of the IAM Role?"
				},
				grantee_vpc.logical_id: {
					"default": "What VPC will be used to access the S3 Access Point?"
				}
			}
		} }

		is_lakeformation_condition = core.CfnCondition(self, "IsLakeFormation", 
			expression = core.Fn.condition_equals("Yes", is_lakeformation)
		)

	# Create S3 Access Point to share dataset objects

		grantee_role = iam.Role.from_role_arn(self, "GranteeIAMRole", grantee_role_arn.value_as_string)
		
		glue_table_arn = f"arn:aws:glue:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:table/{glue_db_name.value_as_string}/{glue_table_name.value_as_string}"

		glue_table = glue.Table.from_table_arn(self, "GlueTable", table_arn = glue_table_arn)

	# Invoke Lambda to obtain S3 bucket and S3 prefix from Glue Table

		get_s3_from_table_execution_role = iam.Role(self, "GetS3FromTableServiceRole",
			assumed_by = iam.ServicePrincipal('lambda.amazonaws.com'),
			managed_policies = [
				iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
				iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
			] )

		lf_permission = lf.CfnPermissions(self, "LFPermissionForLambda", 
			data_lake_principal = lf.CfnPermissions.DataLakePrincipalProperty(data_lake_principal_identifier = get_s3_from_table_execution_role.role_arn),
			resource = lf.CfnPermissions.ResourceProperty(
				table_resource = lf.CfnPermissions.TableResourceProperty(
					name = glue_table_name.value_as_string,
					database_name = glue_db_name.value_as_string
				)
			), 
			permissions = ["DESCRIBE"])

		lf_permission.apply_removal_policy(core.RemovalPolicy.DESTROY, apply_to_update_replace_policy=True)
		lf_permission.node.add_dependency(get_s3_from_table_execution_role)
		lf_permission.cfn_options.condition = is_lakeformation_condition

		lf_wait_condition_handle = cfn.CfnWaitConditionHandle(self, "LFWaitConditionHandle")
		lf_wait_condition_handle.add_metadata(
			"WaitForLFPermissionIfExists",
			core.Fn.condition_if(is_lakeformation_condition.logical_id, lf_permission.logical_id, "")
		)

		with open("lambda/get_s3_from_table.py", encoding="utf8") as fp:
			get_s3_from_table_code = fp.read()

		get_s3_from_table_fn = _lambda.Function(self, "GetS3FromTableHandler", 
			runtime = _lambda.Runtime.PYTHON_3_7,
			code = _lambda.InlineCode.from_inline(get_s3_from_table_code),
			handler = "index.handler",
			role =  get_s3_from_table_execution_role,
			timeout = core.Duration.seconds(600)
		)

		get_s3_from_table = core.CustomResource(self, "GetS3FromTable", 
			service_token = get_s3_from_table_fn.function_arn,
			resource_type = "Custom::GetS3FromTable",
			properties = {
				"GlueDatabase": glue_db_name.value_as_string,
				"GlueTable" : glue_table_name.value_as_string
			} 
		)

		get_s3_from_table.node.add_dependency(lf_wait_condition_handle)

		table_bucket = get_s3_from_table.get_att_string("TableBucket")
		table_prefix = get_s3_from_table.get_att_string("TablePrefix")

	# Create S3 Access Point

		table_name_normalized = core.Fn.join("-", core.Fn.split("_", glue_table_name.value_as_string))
		random_suffix = core.Fn.select(0, core.Fn.split("-", core.Fn.select(2, core.Fn.split("/", core.Aws.STACK_ID))))

		s3_accesspoint_name = f"{table_name_normalized}-{random_suffix}"

		s3_accesspoint_arn = f"arn:aws:s3:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:accesspoint/{s3_accesspoint_name}"

		glue_table_accesspoint_path = f"{s3_accesspoint_arn}/object/{table_prefix}"

		# s3_accesspoint_block_config = s3.CfnAccessPoint.PublicAccessBlockConfigurationProperty(block_public_acls=True, block_public_policy=True, ignore_public_acls=True, restrict_public_buckets=True)

		s3_accesspoint_policy = iam.PolicyDocument( 
				statements = [
					iam.PolicyStatement(
						effect=iam.Effect.ALLOW,
						principals = [
							iam.ArnPrincipal(arn = grantee_role.role_arn)
						],
						actions=[
							"s3:GetObject*"
						],
						resources=[
							f"{glue_table_accesspoint_path}*"
						]),
					iam.PolicyStatement(
						effect=iam.Effect.ALLOW,
						principals = [
							iam.ArnPrincipal(arn = grantee_role.role_arn)
						],
						actions=[
							"s3:ListBucket*"
						],
						resources=[s3_accesspoint_arn],
						conditions = {
             				"StringLike" : {
                 				"s3:prefix": f"{table_prefix}*"
							}
						}
					)
				]
			)

		s3_accesspoint = s3.CfnAccessPoint(self, "S3AccessPoint", 
			bucket = f"{table_bucket}",
			name = s3_accesspoint_name,
			# network_origin = "Internet",
			policy = s3_accesspoint_policy,
			vpc_configuration = s3.CfnAccessPoint.VpcConfigurationProperty(
				vpc_id = grantee_vpc.value_as_string
			)
		)

		glue_table_accesspoint_path_output = f"arn:aws:s3:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:accesspoint/{s3_accesspoint.name}/object/{table_prefix}"

	# Output

		core.CfnOutput(self, "IAMRoleArnOutput", 
			value=grantee_role.role_arn, 
			description="IAM Role Arn")

		core.CfnOutput(self, "GlueTableOutput", 
			value=glue_table.table_arn, 
			description="Glue Table ARN")

		core.CfnOutput(self, "S3AccessPointPathOutput", 
			value=glue_table_accesspoint_path_output, 
			description="S3 Access Point Path for Glue Table")