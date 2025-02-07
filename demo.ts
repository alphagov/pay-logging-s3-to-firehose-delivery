import { handler } from './src'

import { Context, SQSEvent } from 'aws-lambda'

const context: Context = {
  awsRequestId: '',
  callbackWaitsForEmptyEventLoop: true,
  functionName: '',
  functionVersion: '',
  invokedFunctionArn: '',
  logGroupName: '',
  logStreamName: '',
  memoryLimitInMB: '',
  getRemainingTimeInMillis: () => 0,
  done: () => {
  },
  fail: () => {
  },
  succeed: () => {
  },
}

const event: SQSEvent = {
  Records: [],
}

async function runDemo() {
  console.log('Starting demo...')

  process.env.FIREHOSE_STREAM_NAME = 'TEST_STREAM'
  process.env.AWS_ACCOUNT_ID = '12345'
  process.env.AWS_ACCOUNT_NAME = 'local'
  process.env.DEBUG = 'true'

  await handler(event, context, () => {
  })

  console.log('Finished demo...')
}

runDemo()
