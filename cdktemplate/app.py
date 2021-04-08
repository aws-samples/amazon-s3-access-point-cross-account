#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import core
import os

os.environ["AMAZON_REVIEWS_BUCKET_ARN"] = "arn:aws:s3:::amazon-reviews-pds"
os.environ["AMAZON_REVIEWS_OBJECT"] = "tsv/amazon_reviews_us_Camera_v1_00.tsv.gz"

os.environ["NYC_TLC_BUCKET_ARN"] = "arn:aws:s3:::nyc-tlc"
os.environ["NYC_TLC_OBJECT"] = "trip data/green_tripdata_2020-06.csv"

from stacks.amazonreviews_stack import AmazonReviewsDatasetStack
from stacks.nyctlc_stack import NycTlcDatasetStack
from stacks.s3accesspointfromtable import S3AccessPointFromTable

app = core.App()
AmazonReviewsDatasetStack(app, "amazon-reviews-dataset-stack")
NycTlcDatasetStack(app, "nyc-tlc-dataset-stack")
S3AccessPointFromTable(app, "s3-accesspoint-fromtable")

app.synth()
