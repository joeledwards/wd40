const {Reader, Writer} = require('nsqjs')
const Squeaky = require('squeaky')
const throttle = require('@buzuli/throttle')

require('./child')({setup, logic})

async function setup (context) {
  const {
    alias,
    batchSize,
    messageSize,
    logMode,
    brightenMyDay
  } = context.config

  require('log-a-log').init({alias, mode: brightenMyDay ? 'pony' : logMode})

  const message = 'z'.repeat(messageSize)
  context.batch = []
  if (batchSize > 1) {
    for (i = 0; i < batchSize; i++) {
      context.batch.push(message)
    }
  } else {
    context.batch = message
  }

  context.pub = await publisher(context.config)
}
  
async function logic (context) {
  const {
    batch,
    pub,
    config: {
      batchSize,
      topic
    }
  } = context

  let sent = 0

  const notify = throttle({
    minDelay: 100,
    maxDelay: 1000,
    reportFunc: () => {
      context.report({sent})
      sent = 0
    }
  })

  const more = async () => {
    try {
      await pub(topic, batch)
      sent += batchSize
      notify()
      setImmediate(more)
    } catch (error) {
      console.error('Error in publisher:', error)
      context.halt(error)
    }
  }

  setImmediate(more)

  await context.join()
}

function publisher ({
  host,
  port,
  lib,
  pubLib,
  topic,
  snappy,
  deflate,
  maxReconnects,
  baseReconnectDelay,
  maxReconnectDelay
}) {
  const nsqLib = pubLib || lib
  if (nsqLib === 'nsqjs') {
    return new Promise((resolve, reject) => {
      console.log(`Creating ${nsqLib} publisher ${host}:${port}...`)

      const options = {}

      if (snappy) {
        options.snappy = true
      }

      if (deflate) {
        options.deflate = true
        options.deflateLevel = 6
      }

      const pub = new Writer(host, port, options)
      const publish = (topic, data) => {
        return new Promise((resolve, reject) => {
          pub.publish(topic, data, (error) => {
            if (error) {
              reject(error)
            } else {
              resolve()
            }
          })
        })
      }
      pub.on('error', error => console.error(error))
      pub.once('error', error => reject(error))
      pub.on('closed', () => console.warn('closed'))
      pub.on('ready', () => resolve(publish))
      pub.connect()
    })
  } else {
    console.log(`Creating squeaky publisher ${host}:${port}...`)
    const pub = new Squeaky({
      host,
      port,
      maxConnectAttempts: maxReconnects,
      reconnectDelayFactor: baseReconnectDelay,
      maxReconnectDelay
    })
    pub.on('error', error => console.error(error))
    pub.on('writer.ready', () => console.log('ready'))
    pub.on('writer.disconnecct', () => console.warn('disconnected'))
    pub.on('writer.end', () => console.warn('end'))
    const publish = (topic, data) => {
      return pub.publish(topic, data)
    }
    return Promise.resolve(publish)
  }
}

