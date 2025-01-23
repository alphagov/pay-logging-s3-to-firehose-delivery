import { handler } from './src'

import { Context, S3Event } from 'aws-lambda'

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
  }
}

const event: S3Event = {
  Records: []
}

async function runDemo() {
  console.log('Starting demo...')

  await handler(event, context, () => {
  })

  console.log('Finished demo...')
}

runDemo()
