# quickstart-illumina-dragen
## DRAGEN on the AWS Cloud


This Quick Start deploys Dynamic Read Analysis for GENomics Complete Suite (DRAGEN CS), a data analysis platform by Illumina, on the AWS Cloud in about 15 minutes.

DRAGEN CS enables ultra-rapid analysis of next-generation sequencing (NGS) data, significantly reduces the time required to analyze genomic data, and improves accuracy. It includes bioinformatics pipelines that provide highly optimized algorithms for mapping, aligning, sorting, duplicate marking, and haplotype variant calling. These pipelines include DRAGEN Germline V2, DRAGEN Somatic V2 (Tumor and Tumor/Normal), DRAGEN Virtual Long Read Detection (VLRD), DRAGEN RNA Gene Fusion, DRAGEN Joint Genotyping, and GATK Best Practices.

The Quick Start builds an AWS environment that spans two Availability Zones for high availability, and provisions two AWS Batch compute environments for Spot Instances and On-Demand Instances. These environments include DRAGEN F1 instances that are connected to field-programmable gate arrays (FPGAs) for hardware acceleration.

The Quick Start offers two deployment options:

- Deploying DRAGEN into a new virtual private cloud (VPC) on AWS
- Deploying DRAGEN into an existing VPC on AWS

You can also use the AWS CloudFormation templates as a starting point for your own implementation.

![Quick Start architecture for DRAGEN on AWS](https://d0.awsstatic.com/partner-network/QuickStart/datasheets/quickstart-architecture-for-dragen-on-aws.png)

For architectural details, best practices, step-by-step instructions, and customization options, see the 
[deployment guide](https://fwd.aws/YqKNQ).

To post feedback, submit feature ideas, or report bugs, use the **Issues** section of this GitHub repo.
If you'd like to submit code for this Quick Start, please review the [AWS Quick Start Contributor's Kit](https://aws-quickstart.github.io/). 
