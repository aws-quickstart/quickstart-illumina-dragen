# quickstart-illumina-dragen

This git repo contains the scripts needed to setup the 'Quickstart' DRAGEN environment.

## Step 1
Customize the Quickstart parameters within the repo based on user’s requirements:

- ci/batch-params.t1.json
  - The “KeyPairName” parameter should be set to match a KeyPair that has been set in the user’s AWS account, i.e. “ParameterValue”: “user-key-pair-name”
- ci/config.yml
  - The “s3bucket” parameter should be set to the bucket where the genomics input datasets are stored and output datasets are to be saved. The script uses this to configure Batch IAM roles and policies
  - The “regions” list (specified 2 times) may need to be changed. The default currently replicates the environment in us-east-1, eu-west-1 and us-west-2 regions. Generally only one region (where the user data resides) may be needed.  
- templates/dragen.template
  - The current default AMI is a Dragen AWS MarketPlace AMI (i.e. ami-34c3444b for us-east-1 region). If a customized AMI is used, then this should be specified for each region, i.e. in section Mappings => AWSAMIRegionMap => region:ami-id

## Step 2
Run TaskCat to execute the CloudFormation scripts for deploying a new environment to run the genomic jobs:
```
> taskcat -c quickstart-edico-genome/ci/config.yml -nx` 
```
The “-n” flag indicates that the environment should not be torn down and maintained to run batch jobs. The full taskcat script takes about 15 minutes to run, and creates these new AWS components:
-	Virtual Private Cloud (VPC) with Private/Public Subnets in specified regions and AZs
-	An Internet Gateway to access Public subnets in the VPC
-	NAT Gateways for each subnet to allow EC2 instances outbound access to the internet
-	Code pipeline to build the Dragen Docker image that is used to run the genomics jobs via Batch, and an ECR repo to store the image
-	Two Batch Compute Environments – on-demand and spot
-	A Batch Job Queue that dispatches jobs to the compute environments, prioritizing spot to reduce the compute cost
-	A Batch Job Definition for running the Dragen jobs
-	IAM roles and policies to enable Batch jobs to run and access the S3 genomic data

## Step 3
Once the deployment is complete, the environment can be verified by running a DRAGEN job that reads the input datasets and the reference HT from the aforementioned S3 genomics data bucket (specified when configuring the Quickstart launch script), and outputs the analysis results into a designated output location in the S3 bucket.  To do a quick sanity test, a small input dataset consisting of a partial FASTQ pair (i.e. no larger than 100MB each) is suggested.

AWS Batch is used to launch the job, either through the AWS CLI or Console. 
The DRAGEN job parameters are provided as “Commands”. To see the full list of options, refer to the DRAGEN User Guide or Quickstart Guide (links are provided in the reference section).
Most of the options function exactly as they are described in the guide. However, for the the options that refer to local files or directories, either an HTTPS PS URL (in case of files) or S3 URL containing the name of the genomics bucket and path (in the case of a directory) should be provided.

Below is an example given where a user wants to run a DRAGEN end-to-end job to do Map/Align/Sorting/De-dup and Variant Calling. An input dataset with a paired FASTQ file is used to generate a VCF output.

The simplest way to run via CLI is to create an input JSON file that describes the job being run, i.e. e2e-job.json
```
{
    "jobName": "e2e-job1",
    "jobQueue": "dragen-queue",
    "jobDefinition": "dragen",
    "containerOverrides": {
        "vcpus": 8,
        "memory": 120000,
        "command": [
  "--ref-dir",
  "s3://<bucket/path>",
  "-1",
  "https://<S3_URL>",
  "-2",
  "https://<S3_URL>",
  "--output-directory",
  "s3://<bucket/path>",
  "--enable-duplicate-marking",
  "true",
  "--output-file-prefix",
  "NA12878_S1_L004_R1_001",
  "--enable-map-align",
  "true",
  "--output-format",
  "BAM",
  "--enable-variant-caller",
  "true",
  "--vc-sample-name",
  "DRAGEN_RGSM",
  "--lic-server",
  "https://XXXXXXXXXXXX:YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY@license.edicogenome.com"
        ]
    },
    "retryStrategy": {
        "attempts": 1
    }
}
```
Then launch the job using the command line (i.e. using the file above):
```
> aws batch submit-job --cli-input-json file://e2e-job.json
```

