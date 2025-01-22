import { handler } from './index'

describe('Test S3 event processor', () => {
  test('should process S3 events', async () => {

    const event = {}
    // noinspection TypeScriptValidateTypes
    const result = await handler(event)
    expect(result).toStrictEqual(true)
  })
})
