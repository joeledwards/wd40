#!/usr/bin/env node

const c = require('@buzuli/color')
const durations = require('durations')
const Squeaky = require('squeaky')
const {Reader, Writer} = require('nsqjs')
const throttle = require('@buzuli/throttle')

const defaultHost = 'localhost'
const defaultPort = 4150
const defaultQos = 1
const defaultMessageSize = 64
const defaultBatchSize = 1
const defaultTopic = 'bench#ephemeral'
const defaultChannel = 'wd40#ephemeral'
const defaultLib = 'squeaky'

function args () {
  return require('yargs')
    .env('WD40')
    .option('host', {
      type: 'string',
      desc: 'nsqd host',
      default: defaultHost,
      alias: ['h']
    })
    .option('port', {
      type: 'number',
      desc: 'nsqd port',
      default: defaultPort,
      alias: ['p']
    })
    .option('qos', {
      type: 'number',
      desc: 'max outstanding messages',
      default: defaultQos,
      alias: ['q']
    })
    .option('message-size', {
      type: 'number',
      desc: 'bytes per message',
      default: defaultMessageSize,
      alias: ['m']
    })
    .option('batch-size', {
      type: 'number',
      desc: 'messages per batch',
      default: defaultBatchSize,
      alias: ['b']
    })
    .option('topic', {
      type: 'string',
      desc: 'topic on which to publish/subscribe',
      default: defaultTopic,
      alias: ['t']
    })
    .option('channel', {
      type: 'string',
      desc: 'channel on which to subscribe',
      default: defaultChannel,
      alias: ['c']
    })
    .option('lib', {
      type: 'string',
      desc: 'the client library to use for NSQ (nsqjs | squeaky)',
      default: defaultLib,
      alias: ['l']
    })
    .option('pub-lib', {
      type: 'string',
      desc: 'the client library to use for NSQ publishes (nsqjs | squeaky)',
      alias: ['P']
    })
    .option('sub-lib', {
      type: 'string',
      desc: 'the client library to use for NSQ subscriptions (nsqjs | squeaky)',
      alias: ['S']
    })
    .argv
}

function subscriber ({host, port, qos, lib, subLib, topic, channel}, handler) {
  const nsqLib = subLib || lib
  if (nsqLib === 'nsqjs') {
    return new Promise((resolve, reject) => {
      const address = `${host}:${port}`
      console.log(`Creating ${nsqLib} subscriber ${address}...`)
      const sub = new Reader(topic, channel, {
        nsqdTCPAddresses: [address],
        maxInFlight: qos
      })
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

function publisher ({host, port, lib, pubLib, topic}) {
  const nsqLib = pubLib || lib
  if (nsqLib === 'nsqjs') {
    return new Promise((resolve, reject) => {
      console.log(`Creating ${nsqLib} publisher ${host}:${port}...`)
      const pub = new Writer(host, port)
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
      pub.on('error', error => reject(error))
      pub.on('ready', () => resolve(publish))
      pub.connect()
    })
  } else {
    console.log(`Creating squeaky publisher ${host}:${port}...`)
    const pub = new Squeaky({host, port})
    return (topic, data) => {
      return pub.publish(topic, data)
    }
  }
}

async function bench () {
  const config = args()
  const {
    host,
    port,
    qos,
    messageSize,
    batchSize,
    topic,
    channel,
    lib,
    pubLib,
    subLib
  } = config

  require('log-a-log')

  console.info(`Benchmarking:`)
  console.info(`          host : ${host}`)
  console.info(`          port : ${port}`)
  console.info(`           qos : ${qos}`)
  console.info(`  message size : ${messageSize}`)
  console.info(`    batch size : ${batchSize}`)
  console.info(`         topic : ${topic}`)
  console.info(`       channel : ${channel}`)
  console.info(`       pub-lib : ${pubLib || lib}`)
  console.info(`       sub-lib : ${subLib || lib}`)

  const message = 'z'.repeat(messageSize)

  let batch = []
  if (batchSize > 1) {
    for (i = 0; i < batchSize; i++) {
      batch.push(message)
    }
  } else {
    batch = message
  }

  let watch = durations.stopwatch()
  let rcvd = 0
  let sent = 0
  let retry = 0

  const notify = throttle({
    reportFunc: () => {
      console.log(`sent=${sent} rcvd=${rcvd} retry=${retry} offset=${sent-rcvd} (${watch} elapsed)`)
    }
  })

  await subscriber(config, msg => {
    if (msg.attempts > 1) {
      retry++
    }
    rcvd++
    notify()
    msg.finish()
  })

  const pub = await publisher(config)
  const more = async () => {
    try {
      await pub(topic, batch)
      sent+=batchSize
      notify()
      more()
    } catch (error) {
      console.error('Error in publisher:', error)
      process.exit(1)
    }
  }

  watch.start()
  more()
}

async function run () {
  try {
    await bench()
  } catch (error) {
    console.error('Error running benchmark:', error)
    process.exit(1)
  }
}

run()
