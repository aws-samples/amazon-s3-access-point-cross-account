# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import setuptools

with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="LakeFormationDatasets",
    version="0.0.1",

    description="Templates to deploy two public datasets to demonstrate cross-account access with AWS Lake Formation",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="Rodrigo Alarcon",

    package_dir={"": "lakeformationdatasets"},
    packages=setuptools.find_packages(where="lakeformationdatasets"),

    install_requires=[
        "aws-cdk.core==1.95.1",
        "aws-cdk.aws-iam==1.95.1",
        "aws-cdk.aws_glue==1.95.1",
        "aws-cdk.aws_s3==1.95.1",
        "aws-cdk.aws_lambda==1.95.1",
        "aws-cdk.aws_lakeformation==1.95.1",
        "aws-cdk.aws_cloudformation==1.95.1",
        "aws-cdk.aws_ec2==1.95.1"
    ],
    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: MIT License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
