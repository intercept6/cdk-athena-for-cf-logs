import {RemovalPolicy, Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {Bucket} from 'aws-cdk-lib/aws-s3';
import {AthenaTableForCloudFront} from './athena-table-for-cloudfront';

export class CdkAthenaForCfLogsStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const srcBucket = new Bucket(this, 'SrcBucket', {
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const destBucket = new Bucket(this, 'DestBucket', {
      removalPolicy: RemovalPolicy.DESTROY,
    });

    new AthenaTableForCloudFront(this, 'AthenaCloudFront', {
      srcBucket,
      destBucket,
    });
  }
}
