# quickstart-edico-genome

This git repo is used to build the executable "Dragen Quickstart" wrapper docker image that is used to run Dragen
jobs for Genomic Analysis. This Dockerfile is used to build the 'dragen' docker image which should then be
pushed to an ECR docker repo.

The Cloud Formation scripts in this repo are run to setup the Batch environment to run Dragen i.e.:
- 'dragen' job description which points to the ARN of the docker image in the ECS repo
- 'dragen-queue' job queue
- 'dragen-ondemand' and 'dragen-spot' compute environments


A new job is then initiated by creating a new Batch job using the 'dragen' job definition and 'dragen-queue' job queue.

The parameters for the command are:

```
usage: dragen_qs.py [-h] --fastq1_s3_url FASTQ1_S3_URL
                    [--fastq2_s3_url FASTQ2_S3_URL] --ref_s3_url REF_S3_URL
                    --output_s3_url OUTPUT_S3_URL [--enable_map] [--enable_vc]

optional arguments:
  -h, --help            show this help message and exit

File paths:
  --fastq1_s3_url FASTQ1_S3_URL
                        Input FASTQ1 S3 URL
  --fastq2_s3_url FASTQ2_S3_URL
                        Input FASTQ2 S3 URL
  --ref_s3_url REF_S3_URL
                        Reference Hash Table S3 URL
  --output_s3_url OUTPUT_S3_URL
                        Output Prefix S3 URL

DRAGEN run command args:
  --enable_map          Enable MAP/Align and ouput BAM
  --enable_vc           Enable Variant Calling and ouput VCF
```
For example, a JSON description of an end-to-end run is show below:
```
[
  "--fastq1_s3_url",
  "https://s3.amazonaws.com/your-input-bucket/FASTQs/NA12878_S1_L004_R1_001.fastq.gz",
  "--fastq2_s3_url",
  "https://s3.amazonaws.com/your-input-bucket/FASTQs/NA12878_S1_L004_R2_001.fastq.gz",
  "--ref_s3_url",
  "s3://your-reference-data/references/v5/hg19",
  "--output_s3_url",
  "s3://your-output-bucket/test-output",
  "--enable_map",
  "--enable_vc"
]
```
