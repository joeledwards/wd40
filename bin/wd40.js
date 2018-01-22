#!/usr/bin/env node

const c = require('@buzuli/color')
const childProcess = require('child_process')
const durations = require('durations')
const {blue, green, orange, yellow} = require('@buzuli/color')
const throttle = require('@buzuli/throttle')

const defaultHost = 'localhost'
const defaultPort = 4150
const defaultQos = 1
const defaultMessageSize = 64
const defaultBatchSize = 1
const defaultTopic = 'bench#ephemeral'
const defaultChannel = 'wd40#ephemeral'
const defaultLib = 'squeaky'
const defaultPubCount = 1
const defaultSubCount = 1
const defaultMinReportDelay = 1000
const defaultMaxReportDelay = 5000

const config = require('yargs')
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
  .option('publisher-count', {
    type: 'number',
    desc: 'number of publisher processes to launch',
    default: defaultPubCount,
    alias: ['pub-count', 'pc']
  })
  .option('subscriber-count', {
    type: 'number',
    desc: 'number of subscriber processes to launch',
    default: defaultSubCount,
    alias: ['sub-count', 'sc']
  })
  .option('min-report-delay', {
    type: 'number',
    desc: 'minimum delay (in ms) between reports',
    default: defaultMinReportDelay,
    alias: ['d']
  })
  .option('max-report-delay', {
    type: 'number',
    desc: 'maximum delay (in ms) between reports',
    default: defaultMaxReportDelay,
    alias: ['D']
  })
  .argv

let nextId = 0
const children = {}
const spawnPub = () => spawn({type: 'pub', module: './lib/pub'})
const spawnSub = () => spawn({type: 'sub', module: './lib/sub'})

// Spwan child process
function spawn (meta) {
  const child = {
    ...meta,
    id: nextId++,
    process: childProcess.fork(meta.module)
  }
  child.process.on('message', messageHandler(child))
  children[child.id] = child
}

// Handle messages from child processes
function messageHandler (child) {
  return ({channel, message}) => {
    switch (channel) {
      case 'start': return startHandler(child)(message)
      case 'report': return reportHandler(child)(message)
      case 'end': return endHandler(child)(message)
      default: return defaultHandler(child)(channel, message)
    }
  }
}

function startHandler (child) {
  return () => {
    console.info(`[${child.type}-${child.id}] Child process started.`)
    child.process.send({channel: 'config', message: {...config, id: child.id}})
  }
}

const watch = durations.stopwatch().start()
let rcvd = 0
let sent = 0
let retry = 0
const notify = throttle({
  minDelay: config.minReportDelay,
  maxDelay: config.maxReportDelay,
  reportFunc: () => {
    console.log(
      `sent=${orange(sent)} rcvd=${orange(rcvd)} retry=${orange(retry)} offset=${orange(sent - rcvd)} (${blue(watch)})`
    )
  }
})

function reportHandler (child) {
  return report => {
    // TODO: info -> debug
    console.debug(`[${child.type}-${child.id}] Child report received.`)
    if (child.type === 'pub') {
      sent += report.sent
    } else if (child.type === 'sub') {
      rcvd += report.rcvd
      retry += report.retry
    } else {
      console.error(`Unrecognized type '${child.type}'!`)
    }
    notify()
  }
}

function endHandler (child) {
  return reason => {
    console.info(`[${child.type}-${child.id}] Child process ended.`, reason || '')
  }
}

function defaultHandler (child) {
  return (channel, message) => {
    console.error(
      `[${child.type}-${child.id}] Received a notification from unrecognized channel '${channel}':\n`,
      message
    )
  }
}

// Halt child process
async function halt (child) {
  child.send({channel: 'halt'})
}

async function benchmark () {
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
    subLib,
    pubCount,
    subCount,
    minReportDelay,
    maxReportDelay
  } = config

  require('log-a-log')

  console.info(`Benchmarking:`)
  console.info(`             host : ${yellow(host)}`)
  console.info(`             port : ${orange(port)}`)
  console.info(`              qos : ${orange(qos)}`)
  console.info(`     message size : ${orange(messageSize)}`)
  console.info(`       batch size : ${orange(batchSize)}`)
  console.info(`            topic : ${green(topic)}`)
  console.info(`          channel : ${green(channel)}`)
  console.info(`          pub lib : ${green(pubLib || lib)}`)
  console.info(`          sub lib : ${green(subLib || lib)}`)
  console.info(`        pub count : ${orange(pubCount)}`)
  console.info(`        sub count : ${orange(subCount)}`)
  console.info(` min report delay : ${orange(durations.millis(minReportDelay))}`)
  console.info(` max report delay : ${orange(durations.millis(maxReportDelay))}`)

  for (let s of new Array(subCount).fill(1)) {
    await spawnSub()
  }

  for (let p of new Array(pubCount).fill(1)) {
    await spawnPub()
  }
}

async function run () {
  try {
    await benchmark()
  } catch (error) {
    console.error('Error running benchmark:', error)
    process.exit(1)
  }
}

run()

