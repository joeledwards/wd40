const {Reader, Writer} = require('nsqjs')
const Squeaky = require('squeaky')
const throttle = require('@buzuli/throttle')

require('./child')(async context => {
  let rcvd = 0
  let retry = 0

  const notify = throttle({
    minDelay: 250,
    maxDelay: 1000,
    reportFunc: () => {
      context.report({rcvd, retry})
      rcvd = 0
      retry = 0
    }
  })

  await subscriber(context.config, msg => {
    if (msg.attempts > 1) {
      retry++
    }
    rcvd++
    notify()
    msg.finish()
  })

  await context.join()
})

function subscriber ({id, host, port, qos, lib, subLib, topic, channel}, handler) {
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

