import { S3ObjectCreatedNotificationEvent, SQSEvent, SQSHandler, SQSRecord } from 'aws-lambda'
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { FirehoseClient, PutRecordCommand } from '@aws-sdk/client-firehose'
import { createGunzip } from 'node:zlib'
import { Readable } from 'node:stream'
import * as readline from 'node:readline'

const DEBUG = process.env['DEBUG'] === 'true'

const awsRegion = { region: 'eu-west-1' }
const s3Client: S3Client = new S3Client(awsRegion)
const firehoseClient = new FirehoseClient(awsRegion)

export type LogRecord = {
  SourceFile: {
    S3Bucket: string
    S3Key: string
  }
  S3Bucket?: string
  ALB?: string
  AWSAccountID: string
  AWSAccountName: string
  Logs: string[]
}

let FIREHOSE_STREAM_NAME: string, AWS_ACCOUNT_ID: string, AWS_ACCOUNT_NAME: string

export const handler: SQSHandler = async (sqsEvent: SQSEvent) => {
  try {
    ({ AWS_ACCOUNT_ID, AWS_ACCOUNT_NAME, FIREHOSE_STREAM_NAME } = checkAndGetEnvironmentVariables())

    for (const sqsRecord of sqsEvent.Records) {
      await processSqsRecord(sqsRecord)
    }
  } catch (error: unknown) {
    if (error instanceof Error) {
      throw new Error(`Error processing SQS message: ${error.message}`)
    } else {
      throw new Error(`Error processing SQS message: ${error}`)
    }
  }
}

function checkAndGetEnvironmentVariables() {
  const FIREHOSE_STREAM_NAME = process.env.FIREHOSE_STREAM_NAME
  if (!FIREHOSE_STREAM_NAME) {
    throw new Error('Environment variable FIREHOSE_STREAM_NAME is missing')
  }

  const AWS_ACCOUNT_ID = process.env.AWS_ACCOUNT_ID
  if (!AWS_ACCOUNT_ID) {
    throw new Error('Environment variable AWS_ACCOUNT_ID is missing')
  }

  const AWS_ACCOUNT_NAME = process.env.AWS_ACCOUNT_NAME
  if (!AWS_ACCOUNT_NAME) {
    throw new Error('Environment variable AWS_ACCOUNT_NAME is missing')
  }

  return { FIREHOSE_STREAM_NAME, AWS_ACCOUNT_ID, AWS_ACCOUNT_NAME }
}

async function processSqsRecord(sqsRecord: SQSRecord) {
  const eventBridgeEvent: S3ObjectCreatedNotificationEvent = JSON.parse(sqsRecord.body)
  debug(`Received message: ${sqsRecord.body}`)

  if (eventBridgeEvent.source != 'aws.s3') {
    debug('Invalid S3 event format')
    throw new Error('Invalid S3 event format')
  }

  if (eventBridgeEvent['detail-type'] !== 'Object Created') {
    debug(`Ignoring non object created event - ${eventBridgeEvent['detail-type']}`)
    return
  }

  const sourceBucketName = eventBridgeEvent.detail.bucket.name
  const sourceObjectKey = decodeURIComponent(eventBridgeEvent.detail.object.key.replace(/\+/g, ' '))

  const params = {
    Bucket: sourceBucketName,
    Key: sourceObjectKey
  }
  debug(`Getting S3 object ${JSON.stringify(params)}`)

  const command: GetObjectCommand = new GetObjectCommand(params)
  const { Body } = await s3Client.send(command)

  debug(`Sending logs to Firehose`)
  await sendLogsToFirehose(sourceBucketName, sourceObjectKey, Body as Readable)
}

async function sendLogsToFirehose(sourceS3BucketName: string, sourceS3ObjectKey: string, logDataStream: Readable) {
  const isCompressed = sourceS3ObjectKey.endsWith('.gz')
  let readStream
  let batchRecords: string[] = []

  if (isCompressed) {
    readStream = readline.createInterface({
      input: logDataStream.pipe(createGunzip())
    })
  } else {
    readStream = readline.createInterface({
      input: logDataStream
    })
  }

  for await (const line of readStream) {
    batchRecords.push(line)
    if (batchRecords.length === 500) {
      await sendBatchToFirehose(sourceS3BucketName, sourceS3ObjectKey, batchRecords)
      batchRecords = []
    }
  }
  if (batchRecords.length !== 0) {
    await sendBatchToFirehose(sourceS3BucketName, sourceS3ObjectKey, batchRecords)
  }
}

async function sendBatchToFirehose(sourceS3BucketName: string, sourceS3ObjectKey: string, batchRecords: string[]) {
  const recordData: LogRecord = getFirehoseRecordData(sourceS3BucketName, sourceS3ObjectKey, batchRecords)

  debug(`Data to send to Firehose - ${JSON.stringify(recordData)}`)

  const params = {
    DeliveryStreamName: FIREHOSE_STREAM_NAME,
    Record: {
      Data: Buffer.from(JSON.stringify(recordData))
    }
  }

  const command: PutRecordCommand = new PutRecordCommand(params)
  await firehoseClient.send(command)
}

function getFirehoseRecordData(sourceS3BucketName: string, sourceS3ObjectKey: string, batchRecords: string[]) {
  const logRecord: LogRecord = {
    SourceFile: {
      S3Bucket: sourceS3BucketName,
      S3Key: sourceS3ObjectKey
    },
    AWSAccountID: AWS_ACCOUNT_ID,
    AWSAccountName: AWS_ACCOUNT_NAME,
    Logs: batchRecords
  }

  if (isALBLog(sourceS3ObjectKey)) {
    logRecord.ALB = getALBName(sourceS3ObjectKey)
  } else {
    logRecord.S3Bucket = getS3bucketName(sourceS3ObjectKey)
  }

  return logRecord
}

function isALBLog(sourceS3ObjectKey: string): boolean {
  return sourceS3ObjectKey.includes('elasticloadbalancing')
}

function getALBName(sourceS3ObjectKey: string) {
  const name = sourceS3ObjectKey.match(/^(.*?)\/AWSLogs\//)
  const albName = name ? name[1].split('/').pop() : ''

  debug(`Deriving ALB name from source S3 object key - ${sourceS3ObjectKey}`)
  if (albName === '') {
    throw new Error('Error deriving ALB name')
  }
  return albName
}

function getS3bucketName(s3ObjectKey: string): string {
  const subStrings = s3ObjectKey.split('/')
  const s3BucketName = subStrings.length > 1 ? subStrings[1] : ''

  debug(`Deriving S3 bucket name from source S3 object key - ${s3ObjectKey}`)
  if (s3BucketName === '') {
    throw new Error('Error deriving S3 bucket name')
  }
  return s3BucketName
}

function debug(message: string) {
  if (DEBUG) {
    console.log(message)
  }
}
