#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import {CdkAthenaForCfLogsStack} from '../lib/cdk-athena-for-cf-logs-stack';

const app = new cdk.App();
new CdkAthenaForCfLogsStack(app, 'CdkAthenaForCfLogsStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
