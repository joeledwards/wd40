const {Reader, Writer} = require('nsqjs')
const Squeaky = require('squeaky')
const throttle = require('@buzuli/throttle')

require('./child')({setup, logic})

async function setup (context) {
  const {
    alias,
    logMode,
    brightenMyDay
  } = context.config

  require('log-a-log').init({alias, mode: brightenMyDay ? 'pony' : logMode})
  context.rcvd = 0
  context.retry = 0
  context.notify = () => {}

  await subscriber(context.config, msg => {
    if (msg.attempts > 1) {
      context.retry++
    }
    context.rcvd++
    context.notify()
    msg.finish()
  })
}

async function logic (context) {
  context.notify = throttle({
    minDelay: 100,
    maxDelay: 1000,
    reportFunc: () => {
      context.report({rcvd: context.rcvd, retry: context.retry})
      context.rcvd = 0
      context.retry = 0
    }
  })

  await context.join()
}

function subscriber ({
  id,
  host,
  port,
  qos,
  lib,
  subLib,
  topic,
  channel,
  snappy,
  deflate
}, handler) {
  const nsqLib = subLib || lib
  if (nsqLib === 'nsqjs') {
    return new Promise((resolve, reject) => {
      const address = `${host}:${port}`
      console.log(`Creating ${nsqLib} subscriber ${address}...`)

      const options = {
        nsqdTCPAddresses: [address],
        maxInFlight: qos
      }

      if (snappy) {
        options.snappy = true
      }

      if (deflate) {
        options.deflate = true
        options.deflateLevel = 6
      }

      const sub = new Reader(topic, channel, options)
      sub.on('message', handler)
      sub.on('error', error => reject(error))
      sub.on('nsqd_connected', () => resolve())
      sub.connect()
    })
  } else {
    console.log(`Creating squeaky subscriber ${host}:${port}...`)
    const sub = new Squeaky({host, port, concurrency: qos})
    return sub.subscribe(topic, channel, handler)
  }
}

