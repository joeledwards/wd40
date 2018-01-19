require('log-a-log')

const c = require('@buzuli/color')
const durations = require('durations')
const Squeaky = require('squeaky')
const throttle = require('@buzuli/throttle')

const defaultHost = 'localhost'
const defaultPort = 4150
const defaultQos = 1
const defaultMessageSize = 64
const defaultBatchSize = 1
const defaultTopic = 'bench#ephemeral'
const defaultChannel = 'wd40#ephemeral'

function args () {
  return require('yargs')
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
    .argv
}

async function run () {
  const {host, port, qos, messageSize, batchSize, topic, channel} = args()
  console.info(`Benchmarking:`)
  console.info(`          host : ${host}`)
  console.info(`          port : ${port}`)
  console.info(`           qos : ${qos}`)
  console.info(`  message size : ${messageSize}`)
  console.info(`    batch size : ${batchSize}`)
  console.info(`         topic : ${topic}`)
  console.info(`       channel : ${channel}`)

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

  const notify = throttle({
    reportFunc: () => {
      console.log(`sent=${sent} rcvd=${rcvd} offset=${sent-rcvd} (${watch} elapsed)`)
    }
  })
  const sub = new Squeaky({host})
  const pub = new Squeaky({host})

  await sub.subscribe(topic, channel, msg => {
    rcvd++
    notify()
    msg.finish()
  })

  const more = async () => {
    await pub.publish(topic, batch)
    sent+=batchSize
    more()
  }

  watch.start()
  more()
}

run()
