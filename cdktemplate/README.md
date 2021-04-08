
# CDK template for "Compartiendo conjuntos de datos entre cuentas de forma segura usando Amazon S3 Access Points"

This is the CDK template for "Compartiendo conjuntos de datos entre cuentas de forma segura usando Amazon S3 Access Points"

The CDK template allows you to synthesize three independent CloudFormation templates:

* `AmazonReviewsDatasetStack`: Deploys the dataset containing Amazon Customer Reviews (a.k.a. Product Reviews). Sample data is copied from the public dataset into a local S3 bucket, a database and table are created in AWS Glue, and the S3 location is registered with AWS Lake Formation.

* `NycTlcDatasetStack`: Deploys the dataset containing New York City Taxi and Limousine Commission (TLC) Trip Record Data. Sample data is copied from the public dataset into a local S3 bucket, a database and table are created in AWS Glue, and the S3 location is registered with AWS Lake Formation.

* `S3AccessPointFromTable`: Deploys an S3 Access Point which provides a given IAM Role access to the underlying data location for a given Glue Table. Main use case for this template is to grant an ETL process in another AWS Account, access to the S3 objects (e.g., Parquet files) associated to a Glue Table.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the .env
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .env
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .env/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .env\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code and save each stack as YAML files.

```
$ cdk synth --version-reporting false --path-metadata false

$ cdk synth --version-reporting false --path-metadata false amazon-reviews-dataset-stack > AmazonReviewsDatasetStack.yaml

$ cdk synth --version-reporting false --path-metadata false nyc-tlc-dataset-stack > NycTlcDatasetStack.yaml

$ cdk synth --version-reporting false --path-metadata true s3-accesspoint-fromtable > S3AccessPointFromTable.yaml
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!

## License

This library is licensed under the MIT-0 License. See the LICENSE file.