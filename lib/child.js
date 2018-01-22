module.exports = child

const completion = new Promise((resolve, reject) => {
  this.resolve = resolve
  this.reject = reject
})

async function child (logic) {
  let reason = undefined;
  try {
    const config = await init()

    const context = {
      config,
      halt: error => completion.reject(error),
      join: () => completion,
      report: info => process.send({channel: 'report', message: info})
    }

    process.on('message', ({channel, message}) => {
      switch (channel) {
        case halt: completion.resolve(); break;
      }
    })

    await logic(context)
  } catch (error) {
    reason = error
  } finally {
    process.send({channel: 'end', reason})
  }
}

async function init() {
  return new Promise(resolve => {
    // Receive the config from the main process after start.
    process.once('message', ({message: config}) => resolve(config))
    process.send({channel: 'start'})
  })
}

