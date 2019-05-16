const {Reader} = require('nsqjs')
const {Subscriber} = require('squeaky')
const throttle = require('@buzuli/throttle')
const scheduler = require('@buzuli/scheduler')()

require('./child')({setup, logic})

let stop = () => {}

async function deferredAck (msg, ackDelay, remindInterval) {
  return new Promise(resolve => {
    let cancel

    function resetRemind () {
      ({ cancel } = scheduler.after(remindInterval, remind))
    }

    function remind () {
      msg.touch()
      resetRemind()
    }

    resetRemind()

    scheduler.after(ackDelay, () => {
      if (cancel) {
        cancel()
      }
      msg.finish()
    })
  })
}

async function setup (context) {
  const {
    alias,
    logMode,
    brightenMyDay,
    ackDelay,
    remindInterval
  } = context.config

  require('log-a-log')({alias, mode: brightenMyDay ? 'pony' : logMode})
  context.rcvd = 0
  context.retry = 0
  context.notify = () => {}
  stop = () => {
    console.warn('stop() called')
    context.halt()
  }

  await subscriber(context.config, async msg => {
    if (msg.attempts > 1) {
      context.retry++
    }
    context.rcvd++
    context.notify()

    if (ackDelay > 0) {
      await deferredAck(msg, ackDelay, remindInterval)
    } else {
      msg.finish()
    }
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

  notify({force: true, halt: true})
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
  deflate,
  maxReconnects,
  baseReconnectDelay,
  maxReconnectDelay
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
      sub.on('error', error => console.error(error))
      sub.once('error', error => reject(error))
      sub.on('nsqd_connected', () => resolve())
      sub.on('nsqd_closed', () => {
        console.warn('closed')
        stop()
      })
      sub.connect()
    })
  } else {
    return new Promise((resolve, reject) => {
      console.log(`Creating squeaky subscriber ${host}:${port}...`)
      const sub = new Subscriber({
        topic,
        channel,
        host,
        port,
        concurrency: qos,
        maxConnectAttempts: maxReconnects,
        reconnectDelayFactor: baseReconnectDelay,
        maxReconnectDelay
      })

      let ready = false
      sub.on('error', error => {
        console.error(error)
      })
      sub.on(`ready`, () => {
        ready = true
        console.log('ready')
        resolve(sub)
      })
      sub.on(`disconnect`, () => console.warn('disconnected'))
      sub.on(`reconnect`, () => console.warn('reconnected'))
      sub.on(`connect`, () => console.info('connected'))
      sub.on(`close`, () => {
        console.warn('close')
        sub.unref()
        if (ready) {
          stop()
        } else {
          reject(new Error('initial connection failed'))
        }
      })
      sub.on(`message`, handler)
    })
  }
}
