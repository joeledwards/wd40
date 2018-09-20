const {Writer} = require('nsqjs')
const {Publisher} = require('squeaky')
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

  require('log-a-log')({alias, mode: brightenMyDay ? 'pony' : logMode})

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

let halted = false
function stop () {
  halted = true
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

      if (halted) {
        context.halt()
      } else {
        notify()
        setImmediate(more)
      }
    } catch (error) {
      console.error('Error in publisher:', error)
    }
  }

  setImmediate(more)

  await context.join()

  notify({force: true, halt: true})
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
      pub.on('ready', () => resolve(publish))
      pub.on('closed', () => {
        console.warn('closed')
        stop()
      })
      pub.connect()
    })
  } else {
    return new Promise((resolve, reject) => {
      console.log(`Creating squeaky publisher ${host}:${port}...`)
      const pub = new Publisher({
        host,
        port,
        maxConnectAttempts: maxReconnects,
        reconnectDelayFactor: baseReconnectDelay,
        maxReconnectDelay
      })

      const publish = (topic, data) => {
        return pub.publish(topic, data)
      }

      pub.on('error', error => console.error(error))
      pub.on('disconnect', () => console.warn('disconnected'))
      pub.on('reconnect', () => console.warn('reconnected'))
      pub.on('connect', () => console.info('connected'))
      pub.on('ready', () => console.log('ready'))
      pub.on('close', () => {
        console.warn('close')
        pub.unref()
        stop()
      })

      resolve(publish)
    })
  }
}
