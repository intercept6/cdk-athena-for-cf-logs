import {Stack} from 'aws-cdk-lib';
import {Rule} from 'aws-cdk-lib/aws-events';
import {LambdaFunction} from 'aws-cdk-lib/aws-events-targets';
import {CfnDatabase, CfnTable} from 'aws-cdk-lib/aws-glue';
import {NodejsFunction} from 'aws-cdk-lib/aws-lambda-nodejs';
import {CfnBucket, IBucket} from 'aws-cdk-lib/aws-s3';
import {Construct} from 'constructs';
import {resolve} from 'path';

export interface AthenaTableForCloudFrontProps {
  /**
   * @default cloudfront
   */
  databaseName?: string;
  /**
   * @default access_logs
   */
  tableName?: string;
  /**
   * オリジナルのCloudFrontアクセスログが出力されるバケット
   */
  srcBucket: IBucket;
  /**
   * パーティション射影可能なオブジェクト名に変更後に出力されるバケット
   * srcBucketと同じバケットは無限ループが発生するため使用不可
   */
  destBucket: IBucket;
  /**
   * @default cloudfront/
   */
  destPrifix?: string;
}

export class AthenaTableForCloudFront extends Construct {
  public readonly cfnDatabase: CfnDatabase;
  public readonly cfnTable: CfnTable;
  public readonly handler: NodejsFunction;
  public readonly rule: Rule;

  constructor(
    scope: Construct,
    id: string,
    props: AthenaTableForCloudFrontProps
  ) {
    super(scope, id);

    const {
      databaseName = 'cloudfront',
      tableName = 'access_logs',
      srcBucket,
      destBucket,
      destPrifix = 'cloudfront/',
    } = props;

    if (srcBucket.bucketArn === destBucket.bucketArn) {
      throw new Error(
        'srcBucketとdestBucketに同じバケットを指定するとLambdaの無限実行が発生します。'
      );
    }

    const cfnSrcBucket = srcBucket.node.defaultChild as CfnBucket;
    cfnSrcBucket.addPropertyOverride(
      'NotificationConfiguration.EventBridgeConfiguration.EventBridgeEnabled',
      true
    );

    const cfnDatabase = new CfnDatabase(this, 'Database', {
      catalogId: Stack.of(this).account,
      databaseInput: {name: databaseName},
    });
    this.cfnDatabase = cfnDatabase;

    const cfnTable = new CfnTable(this, 'Table', {
      catalogId: Stack.of(this).account,
      databaseName: cfnDatabase.ref,
      tableInput: {
        name: tableName,
        tableType: 'EXTERNAL_TABLE',
        partitionKeys: [{name: 'date', type: 'string'}],
        parameters: {
          'skip.header.line.count': '2',
          'projection.enabled': true,
          'projection.date.type': 'date',
          'projection.date.range': '2021/01/01/00,NOW',
          'projection.date.format': 'yyyy/MM/dd/HH',
          'projection.date.interval': 1,
          'projection.date.interval.unit': 'HOURS',
          'storage.location.template':
            `s3://${destBucket.bucketName}/${destPrifix}` + '${date}',
        },
        storageDescriptor: {
          columns: [
            {name: 'log_date', type: 'date'},
            {name: 'time', type: 'string'},
            {name: 'x_edge_location', type: 'string'},
            {name: 'sc_bytes', type: 'bigint'},
            {name: 'c_ip', type: 'string'},
            {name: 'cs_method', type: 'string'},
            {name: 'cs_host', type: 'string'},
            {name: 'cs_uri_stem', type: 'string'},
            {name: 'sc_status', type: 'int'},
            {name: 'cs_referer', type: 'string'},
            {name: 'cs_user_agent', type: 'string'},
            {name: 'cs_uri_query', type: 'string'},
            {name: 'cs_cookie', type: 'string'},
            {name: 'x_edge_result_type', type: 'string'},
            {name: 'x_edge_request_id', type: 'string'},
            {name: 'x_host_header', type: 'string'},
            {name: 'cs_protocol', type: 'string'},
            {name: 'cs_bytes', type: 'bigint'},
            {name: 'time_taken', type: 'float'},
            {name: 'x_forwarded_for', type: 'string'},
            {name: 'ssl_protocol', type: 'string'},
            {name: 'ssl_cipher', type: 'string'},
            {name: 'x_edge_response_result_type', type: 'string'},
            {name: 'cs_protocol_version', type: 'string'},
            {name: 'fle_status', type: 'string'},
            {name: 'fle_encrypted_fields', type: 'string'},
            {name: 'c_port', type: 'int'},
            {name: 'time_to_first_byte', type: 'float'},
            {name: 'x_edge_detailed_result_type', type: 'string'},
            {name: 'sc_content_type', type: 'string'},
            {name: 'sc_content_len', type: 'bigint'},
            {name: 'sc_range_start', type: 'bigint'},
            {name: 'sc_range_end', type: 'bigint'},
          ],
          inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
          outputFormat:
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
          serdeInfo: {
            serializationLibrary:
              'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
            parameters: {
              'field.delim': '\t',
              'serialization.format': '\t',
            },
          },
          location: `s3://${destBucket.bucketName}/${destPrifix}`,
        },
      },
    });
    this.cfnTable = cfnTable;

    const handler = new NodejsFunction(this, 'MoveAccessLog', {
      entry: resolve(__dirname, 'handler/move-access-log.ts'),
      bundling: {
        sourceMap: true,
        minify: true,
      },
      environment: {
        TARGET_BUCKET: destBucket.bucketName,
        TARGET_KEY_PREFIX: destPrifix,
      },
      memorySize: 256,
    });
    srcBucket.grantRead(handler);
    srcBucket.grantDelete(handler);
    destBucket.grantWrite(handler);

    this.handler = handler;

    const rule = new Rule(this, 'CreatedEvent', {
      eventPattern: {
        source: ['aws.s3'],
        resources: [srcBucket.bucketArn],
        detailType: ['Object Created'],
      },
      targets: [new LambdaFunction(handler)],
    });
    this.rule = rule;
  }
}
