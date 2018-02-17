#!/usr/bin/env node

const childProcess = require('child_process')
const {blue, green, orange, yellow} = require('@buzuli/color')
const durations = require('durations')
const path = require('path')
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
  .option('deflate', {
    type: 'boolean',
    desc: 'enable zlib level 6 compression',
    default: false
  })
  .option('snappy', {
    type: 'boolean',
    desc: 'enable snappy compression',
    default: false,
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
  .option('log-mode', {
    type: 'string',
    desc: 'set log-a-log mode',
    default: 'utc',
    alias: ['L']
  })
  .option('brighten-my-day', {
    type: 'boolean',
    desc: 'brighten my day (overrides --log-mode)',
    default: false,
    alias: ['B']
  })
  .option('max-reconnects', {
    type: 'number',
    desc: 'maximum (re)connect attempts before giving up (squeaky only)',
    default: 10,
    alias: ['mr']
  })
  .option('base-reconnect-delay', {
    desc: 'base backoff delay in milliseconds (squeaky only)',
    default: 1000,
    alias: ['brd']
  })
  .option('max-reconnect-delay', {
    desc: 'max computed backoff delay in milliseconds (squeaky only)',
    default: 15000,
    alias: ['mrd']
  })
  .argv

function childModule (name) {
  return path.resolve(__dirname, '..', 'lib', name)
}

let nextId = 0
const children = {}
const spawnPub = () => spawn({type: 'pub', module: childModule('pub')})
const spawnSub = () => spawn({type: 'sub', module: childModule('sub')})

// Spwan child process
function spawn (meta) {
  const id = nextId++
  const alias = `${id}-${meta.type}`
  const child = {
    ...meta,
    id,
    alias,
    ready: false,
    process: childProcess.fork(meta.module)
  }
  child.process.on('message', messageHandler(child))
  children[child.id] = child
}

// Handle messages from child processes
function messageHandler (child) {
  return ({channel, message}) => {
    switch (channel) {
      case 'ready': return readyHandler(child)(message)
      case 'start': return startHandler(child)(message)
      case 'report': return reportHandler(child)(message)
      case 'end': return endHandler(child)(message)
      default: return defaultHandler(child)(channel, message)
    }
  }
}

function childrenReady () {
  return Object.values(children).reduce((acc, child) => acc && child.ready, true)
}

function readyHandler (child) {
  return () => {
    console.info(`[${child.alias}] Child process is ready to run.`)
    child.ready = true

    if (childrenReady()) {
      console.info(`All child processes ready. Sending run instruction...`)
      Object.values(children).forEach(child => child.process.send({channel: 'run', message: 'run'}))
    }
  }
}

function startHandler (child) {
  return () => {
    console.info(`[${child.alias}] Child process started.`)
    child.process.send({channel: 'config', message: {...config, id: child.id, alias: child.alias}})
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
    console.debug(`[${child.alias}] Child report received.`)
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
    console.info(`[${child.alias}] Child process ended.`, reason || '')
  }
}

function defaultHandler (child) {
  return (channel, message) => {
    console.error(
      `[${child.alias}] Received a notification from unrecognized channel '${channel}':\n`,
      message
    )
  }
}

// Halt child process
async function halt (child) {
  child.send({channel: 'halt'})
}

async function benchmark () {
  const alias = 'main'
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
    maxReportDelay,
    logMode,
    brightenMyDay,
    snappy,
    deflate,
    maxReconnects,
    baseReconnectDelay,
    maxReconnectDelay
  } = config

  require('log-a-log').init({alias, mode: brightenMyDay ? 'pony' : logMode})

  console.info(`Benchmarking:`)
  console.info(`                host : ${yellow(host)}`)
  console.info(`                port : ${orange(port)}`)
  console.info(`                 qos : ${orange(qos)}`)
  console.info(`               topic : ${green(topic)}`)
  console.info(`             channel : ${green(channel)}`)
  console.info(`        message size : ${orange(messageSize)}`)
  console.info(`          batch size : ${orange(batchSize)}`)
  console.info(`         compression : ${yellow(snappy ? 'snappy' : deflate ? 'zlib' : 'none')}`)
  console.info(`     publisher count : ${orange(pubCount)}`)
  console.info(`    subscriber count : ${orange(subCount)}`)
  console.info(`             pub lib : ${green(pubLib || lib)}`)
  console.info(`             sub lib : ${green(subLib || lib)}`)
  console.info(`    min report delay : ${blue(durations.millis(minReportDelay))}`)
  console.info(`    max report delay : ${blue(durations.millis(maxReportDelay))}`)
  console.info(`      max reconnects : ${orange(maxReconnects)}`)
  console.info(`base reconnect delay : ${blue(durations.millis(baseReconnectDelay))}`)
  console.info(` max reconnect delay : ${blue(durations.millis(maxReconnectDelay))}`)

  for (let s of new Array(subCount).fill(1)) {
    spawnSub()
  }

  for (let p of new Array(pubCount).fill(1)) {
    spawnPub()
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

