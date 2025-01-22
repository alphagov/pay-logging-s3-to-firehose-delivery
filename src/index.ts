import { Handler, S3Event } from 'aws-lambda'

export const handler: Handler = async (event: S3Event) => {
  return true
}
