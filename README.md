# Welcome-Centre-DataCorps-Code
Code for the DataCorps project with the Welcome Centre



### Code Structure
------------------

The TWC application contains the following core functionality:

1. **Transformers** - Transform json formatted twc database records into a feature
representation `api.model.transformers`
2. **Model Training** - A script which trains an `scikit-learn` implemented
random-forest-like model (`ExtraTreesRegressor`) to predict a score
which indicates how likely a twc client is to exhibit dependent
behaviour, i.e. frequent and/or long lasting usage of the service.
3. **API** - A `Flask`-based REST API which provides endpoints to predict scores
for new datasets, downloading models from Amazon S3, and controlling
model state and retraining
4. **Serverless Deployment** - Configuration for `Zappa`, which converts the API application into
serverless AWS Lambda and sets the appropriate permissions for the IAM
user in AWS
5. **Retraining Docker Application** - A docker image specified in `twc_api/Dockerfile`, which builds an
image which installs the required libraries for running the retraining
script, and which is pushed to the twc ECS docker repository.

### 1. Transformers
----

`api.model.transformers` requires code to transform a twc database records into a
standardised feature format that performs tasks such as the following:

- Join data held in other tables to the Referral table
- Create indicator variables for categorical values
- Add referral context features (e.g. the number in burst)
- Add time windowing functions
- Calculating the target variable (dependency score)

This utilises a `scikit-learn` like transformer pattern, of stateful
objects which implement both a `fit_transform` and `transform` method.
Transformations which are stateful, e.g. creating dummy columns and
maintaining the proper order of columns, store state as attributes in
the transformer class itself. These transformer objects are serialised
using the `pickle` library and used at test time to reliably transform
unseen test datapoints to the exact format that the model was trained
on.

### 2. Model Training
----

`api.model.model` defines the `TWCModel` object, which stores both the
transformer and individual model object, and uses the standard
`scikit-learn` interface, where `fit(X, y)` trains the model, and
`predict(X)` returns predictions for new data.

### 3. API
----

We use the `Flask` framework and the `flask-restplus` library to define
a RESTful API which allows applications to interact with the model.
`flask-restplus` provides swagger documentation for ease of use.

Applications can access the following endpoints:

### 4. Serverless Deployment
----

To save on server costs, the API can be converted into AWS Lambda
functions with the use of the `Zappa` library. This ensures that the
machine provisioning is handled by Amazon and we do not require always
on instances.

> Running `zappa update dev` inside the `twc_api` directory will push any
new code to the staging environment.

Additional permissions are granted to the zappa AWS IAM role to permit
the API to launch new, larger, EC2 instances to retrain the model.

These are:
- ec2.*
- iam.PassRoles

The API once deployed, will be deployed in AWS' API Gateway, and the
endpoint for access will be found there.

### 5. Retraining Docker Application
----

As inference requires fewer compute resources than training, and Lambda
functions have a hard cap on permitted resources, we run the retraining
process on a separate server instance which is ephemeral and deleted
after training is completed.

In order to maintain a stable environment and versioning of the retrain
logic, we use Docker to package up the dependencies of the retraining
script and AWS Elastic Container Repository to store the image. The flow
for retraining is as follows:

1. Build and push the retraining docker image by following the
instructions in the AWS ECR push commands (copied out below)
2. Post a retraining request to the retrain endpoint
3. API spins up a new EC2 instance which is large enough to complete the
retraining and runs the following script which:
    1. Logs into ECR
    2. Pulls the latest Docker image
    3. Runs the retrain script and saves the results and any messages to
    S3
    4. Sets the current model to the new model


> `$(aws ecr get-login --no-include-email --region eu-west-1)`

> `docker build -t twc .`

> `docker tag twc:latest 213288821174.dkr.ecr.eu-west-1.amazonaws.com/twc:latest`

> `docker push 213288821174.dkr.ecr.eu-west-1.amazonaws.com/twc:latest`