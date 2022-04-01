// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

// Copyright 2022 Kato Ryo

import {
  S3Client,
  CopyObjectCommand,
  DeleteObjectCommand,
} from '@aws-sdk/client-s3';
import {EventBridgeHandler} from 'aws-lambda';
import {cleanEnv, str} from 'envalid';

const env = cleanEnv(process.env, {
  TARGET_BUCKET: str(),
  // prefix to copy partitioned data to w/o leading but w/ trailing slash
  TARGET_KEY_PREFIX: str(),
});

// regex for filenames by Amazon CloudFront access logs. Groups:
// - 1.	year
// - 2.	month
// - 3.	day
// - 4.	hour
const datePattern = '[^\\d](\\d{4})-(\\d{2})-(\\d{2})-(\\d{2})[^\\d]';
const filenamePattern = '[^/]+$';

const s3 = new S3Client({apiVersion: '2006-03-01'});

export const handler: EventBridgeHandler<
  'Object Created',
  {
    version: string;
    bucket: {
      name: string;
    };
    object: {
      key: string;
      size: number;
      etag: string;
      sequencer: string;
    };
    'request-id': string;
    requester: string; // aws account id
    'source-ip-address': string;
    reason: 'PutObject';
  },
  void
> = async event => {
  const bucket = event.detail.bucket.name;
  const sourceKey = event.detail.object.key;

  const sourceRegex = new RegExp(datePattern, 'g');
  const match = sourceRegex.exec(sourceKey);
  if (match === null) {
    console.log(
      `Object key ${sourceKey} does not look like an access log file, so it will not be moved.`
    );
    return;
  }

  const [, year, month, day, hour] = match;

  const filenameRegex = new RegExp(filenamePattern, 'g');
  const splited = filenameRegex.exec(sourceKey);
  if (splited === null) {
    `Object key ${sourceKey} does not look like an access log file, so it will not be moved.`;
    return;
  }
  const filename = splited[0];

  const targetKey = `${env.TARGET_KEY_PREFIX}${year}/${month}/${day}/${hour}/${filename}`;
  console.log(
    `Copying s3://${bucket}/${sourceKey} to s3://${env.TARGET_BUCKET}/${targetKey}.`
  );

  await s3.send(
    new CopyObjectCommand({
      CopySource: `${bucket}/${sourceKey}`,
      Bucket: env.TARGET_BUCKET,
      Key: targetKey,
    })
  );

  console.log(`Copied. Now deleting ${sourceKey}.`);
  await s3.send(new DeleteObjectCommand({Bucket: bucket, Key: sourceKey}));
  console.log(`Deleted ${sourceKey}.`);
};
