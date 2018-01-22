const {Reader, Writer} = require('nsqjs')
const Squeaky = require('squeaky')
const throttle = require('@buzuli/throttle')

require('./child')(async context => {
  const {
    alias,
    batchSize,
    messageSize,
    topic
  } = context.config

  require('log-a-log').init({alias: `${alias}`})

  const message = 'z'.repeat(messageSize)

  let batch = []
  if (batchSize > 1) {
    for (i = 0; i < batchSize; i++) {
      batch.push(message)
    }
  } else {
    batch = message
  }

  let sent = 0

  const notify = throttle({
    minDelay: 250,
    maxDelay: 1000,
    reportFunc: () => {
      context.report({sent})
      sent = 0
    }
  })

  const pub = await publisher(context.config)
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
})

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

