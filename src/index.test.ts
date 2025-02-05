import { handler, LogRecord } from './index'

import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { FirehoseClient, PutRecordCommand } from '@aws-sdk/client-firehose'
import { mockClient } from 'aws-sdk-client-mock'
import 'aws-sdk-client-mock-jest'
import { Readable } from 'stream'
import { promisify } from 'util'
import { createGunzip, gzip } from 'zlib'
import { describe } from 'node:test'
import readline from "node:readline"

const gzipPromise = promisify(gzip)

const s3Mock = mockClient(S3Client)
const firehoseMock = mockClient(FirehoseClient)

let testS3EventNotification

function getSQSEvent() {
  return {
    "Records": [
      {"body": JSON.stringify(testS3EventNotification)}
    ]
  }
}

describe('Test S3 to Firehose delivery lambda', () => {
  beforeEach(() => {
    process.env.FIREHOSE_STREAM_NAME = 'test-stream'
    process.env.AWS_ACCOUNT_ID = 'test-account-id'
    process.env.AWS_ACCOUNT_NAME = 'test-account-name'

    firehoseMock.reset()
    s3Mock.reset()

    testS3EventNotification = {
      "Records": [
        {
          "eventVersion": "2.2",
          "eventSource": "aws:s3",
          "eventName": "ObjectCreated:Put",
          "s3": {
            "bucket": {
              "name": "source-bucket-name",
            },
            "object": {
              "key": "[replace-me]"
            }
          }
        }
      ]
    }
  })

  test('should process event notification for S3 access log', async () => {
    testS3EventNotification.Records[0].s3.object.key = 's3/test-bucket/2022-01-21-12-11-52-BEA6D759403DE528.gz'
    const s3ObjectStream = await Readable.from(createLogStream(1, 10))
    s3Mock.on(GetObjectCommand).resolves({Body: s3ObjectStream})

    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await handler(sqsEvent)

    expect(s3Mock.call(0).firstArg.input).toStrictEqual({
      'Bucket': 'source-bucket-name',
      'Key': 's3/test-bucket/2022-01-21-12-11-52-BEA6D759403DE528.gz'
    })

    const rawData = await getStreamDataAsArray(Readable.from(createLogStream(1, 10)))
    verifyFirehoseCallParameters(testS3EventNotification, false, rawData,
      firehoseMock.call(0).firstArg.input
    )
  })

  test('should process event notification for ALB logs', async () => {
    testS3EventNotification.Records[0].s3.object.key = 'alb/env-2/app-ecs-alb-name/AWSLogs/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    const s3ObjectStream = await Readable.from(createLogStream(1, 10))
    s3Mock.on(GetObjectCommand).resolves({Body: s3ObjectStream})

    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await handler(sqsEvent)

    expect(s3Mock.call(0).firstArg.input).toStrictEqual({
      'Bucket': 'source-bucket-name',
      'Key': 'alb/env-2/app-ecs-alb-name/AWSLogs/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    })

    const rawData = await getStreamDataAsArray(Readable.from(createLogStream(1, 10)))
    verifyFirehoseCallParameters(testS3EventNotification, true, rawData,
      firehoseMock.call(0).firstArg.input
    )
  })

  test('should split large data into batches when sending to Firehose', async () => {
    testS3EventNotification.Records[0].s3.object.key = 'alb/env-2/app-ecs-alb-name/AWSLogs/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    const s3ObjectStream = await Readable.from(createLogStream(1, 1000))
    s3Mock.on(GetObjectCommand).resolves({Body: s3ObjectStream})

    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await handler(sqsEvent)

    expect(s3Mock.call(0).firstArg.input).toStrictEqual({
      'Bucket': 'source-bucket-name',
      'Key': 'alb/env-2/app-ecs-alb-name/AWSLogs/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    })

    const rawData = await getStreamDataAsArray(Readable.from(createLogStream(1, 500)))
    verifyFirehoseCallParameters(testS3EventNotification, true, rawData,
      firehoseMock.call(0).firstArg.input
    )

    const rawDataForSecondBatch = await getStreamDataAsArray(Readable.from(createLogStream(501, 1000)))
    verifyFirehoseCallParameters(testS3EventNotification, true, rawDataForSecondBatch,
      firehoseMock.call(1).firstArg.input
    )
  })

  test('should ignore non ObjectCreated events', async () => {
    testS3EventNotification.Records[0].eventName = 'ObjectRemoved:*'

    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await expect(handler(sqsEvent))

    expect(s3Mock.commandCalls(GetObjectCommand)).toHaveLength(0)
    expect(firehoseMock.commandCalls(PutRecordCommand)).toHaveLength(0)
  })

  test('should error when S3Event does not have Records', async () => {
    delete testS3EventNotification.Records

    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await expect(handler(sqsEvent)).rejects.toThrow('Error processing SQS message: Invalid S3 event format')

    expect(s3Mock.commandCalls(GetObjectCommand)).toHaveLength(0)
    expect(firehoseMock.commandCalls(PutRecordCommand)).toHaveLength(0)
  })

  test('should error when S3 client returns error', async () => {
    testS3EventNotification.Records[0].s3.object.key = 'alb/env-2/app-ecs-alb-name/AWSLogs/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    s3Mock.rejects('Error getting S3 object')

    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await expect(handler(sqsEvent)).rejects.toThrow('Error processing SQS message: Error getting S3 object')

    expect(s3Mock.call(0).firstArg.input).toStrictEqual({
      'Bucket': 'source-bucket-name',
      'Key': 'alb/env-2/app-ecs-alb-name/AWSLogs/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    })

    expect(firehoseMock.commandCalls(PutRecordCommand)).toHaveLength(0)
  })

  test('should error when ALB name cannot be derived', async () => {
    testS3EventNotification.Records[0].s3.object.key = 'alb/env-2/app-ecs-alb-name/AWS---non-standard-name---/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    const s3ObjectStream = await Readable.from(createLogStream(1, 10))
    s3Mock.on(GetObjectCommand).resolves({Body: s3ObjectStream})

    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await expect(handler(sqsEvent)).rejects.toThrow('Error processing SQS message: Error deriving ALB name')

    expect(s3Mock.call(0).firstArg.input).toStrictEqual({
      'Bucket': 'source-bucket-name',
      'Key': 'alb/env-2/app-ecs-alb-name/AWS---non-standard-name---/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    })

    expect(firehoseMock.commandCalls(PutRecordCommand)).toHaveLength(0)
  })

  test('should error when S3 bucket name cannot be derived', async () => {
    testS3EventNotification.Records[0].s3.object.key = 'key-without-any-prefix.gz'
    const s3ObjectStream = await Readable.from(createLogStream(1, 10))
    s3Mock.on(GetObjectCommand).resolves({Body: s3ObjectStream})

    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await expect(handler(sqsEvent)).rejects.toThrow('Error processing SQS message: Error deriving S3 bucket name')

    expect(s3Mock.call(0).firstArg.input).toStrictEqual({
      'Bucket': 'source-bucket-name',
      'Key': 'key-without-any-prefix.gz'
    })
    expect(firehoseMock.commandCalls(PutRecordCommand)).toHaveLength(0)
  })

  test('should error when Firehose returns error', async () => {

    testS3EventNotification.Records[0].s3.object.key = 'alb/env-2/app-ecs-alb-name/AWSLogs/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    const s3ObjectStream = await Readable.from(createLogStream(1, 10))
    s3Mock.on(GetObjectCommand).resolves({Body: s3ObjectStream})
    firehoseMock.rejects('Error sending logs to Firehose')

    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await expect(handler(sqsEvent)).rejects.toThrow('Error processing SQS message: Error sending logs to Firehose')

    expect(s3Mock.call(0).firstArg.input).toStrictEqual({
      'Bucket': 'source-bucket-name',
      'Key': 'alb/env-2/app-ecs-alb-name/AWSLogs/1231241241/elasticloadbalancing/2025/01/01/log.gz'
    })

    const rawData = await getStreamDataAsArray(Readable.from(createLogStream(1, 10)))
    verifyFirehoseCallParameters(testS3EventNotification, true, rawData,
      firehoseMock.call(0).firstArg.input
    )
  })

  test('should error when FIREHOSE_STREAM_NAME environment variable is not set', async () => {
    delete process.env.FIREHOSE_STREAM_NAME
    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await expect(handler(sqsEvent)).rejects.toThrow('Environment variable FIREHOSE_STREAM_NAME is missing')

    expect(s3Mock.commandCalls(GetObjectCommand)).toHaveLength(0)
    expect(firehoseMock.commandCalls(PutRecordCommand)).toHaveLength(0)
  })

  test('should error when AWS_ACCOUNT_ID environment variable is not set', async () => {
    delete process.env.AWS_ACCOUNT_ID
    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await expect(handler(sqsEvent)).rejects.toThrow('Environment variable AWS_ACCOUNT_ID is missing')

    expect(s3Mock.commandCalls(GetObjectCommand)).toHaveLength(0)
    expect(firehoseMock.commandCalls(PutRecordCommand)).toHaveLength(0)
  })

  test('should error when AWS_ACCOUNT_NAME environment variable is not set', async () => {
    delete process.env.AWS_ACCOUNT_NAME
    const sqsEvent = getSQSEvent()

    // noinspection TypeScriptValidateTypes
    await expect(handler(sqsEvent)).rejects.toThrow('Environment variable AWS_ACCOUNT_NAME is missing')

    expect(s3Mock.commandCalls(GetObjectCommand)).toHaveLength(0)
    expect(firehoseMock.commandCalls(PutRecordCommand)).toHaveLength(0)
  })

})

async function getStreamDataAsArray(stream: Readable): Promise<string[]> {
  const batchRecords: string[] = []
  const readStream = readline.createInterface({
    input: stream.pipe(createGunzip())
  })
  for await (const line of readStream) {
    batchRecords.push(line)
  }

  return batchRecords
}

async function* createLogStream(startingRange = 1, endingRange = 500) {
  for (let i = startingRange; i <= endingRange; i++) {
    yield await gzipPromise(`This is log line ${i} - 6f977bede alb-logs-delivery [01/Dec/2020:17:00:15 +0000] 111.111.111.11x - "GET /?logging= HTTP/1.1"${i != endingRange ? '\n' : ''}`)
  }
}

const verifyFirehoseCallParameters = (s3Event, isAlbLog, logs, callParameters) => {

  const expectedData: LogRecord = {
    SourceFile: {
      S3Bucket: s3Event.Records[0].s3.bucket.name,
      S3Key: s3Event.Records[0].s3.object.key
    },
    AWSAccountID: 'test-account-id',
    AWSAccountName: 'test-account-name',
    Logs: logs,
  }
  if (isAlbLog) {
    expectedData.ALB = 'app-ecs-alb-name'
  } else {
    expectedData.S3Bucket = 'test-bucket'
  }

  expect(callParameters.DeliveryStreamName).toBe('test-stream')
  expect(callParameters.Record.Data).toStrictEqual(
    Buffer.from(JSON.stringify(expectedData))
  )
}
