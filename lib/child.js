module.exports = child

const EvenEmitter = require('events')

const completion = new Promise((resolve, reject) => {
  this.resolve = resolve
  this.reject = reject
})

async function child ({setup, logic}) {
  let reason = undefined;
  try {
    const config = await init()

    const context = {
      config,
      halt: error => error ? completion.reject(error) : completion.resolve(),
      join: () => completion,
      report: info => process.send({channel: 'report', message: info})
    }

    process.on('message', ({channel, message}) => {
      switch (channel) {
        case 'halt': completion.resolve(); break
      }
    })

    // Perform setup logic if supplied.
    if (setup) {
      await setup(context)
    }

    // Await instruction to run.
    console.log('ready; awaiting run command from parent')
    await sync()

    // Run the child's logic.
    console.log('running')
    await logic(context)
  } catch (error) {
    reason = error
  } finally {
    process.send({channel: 'end', reason})
  }
}

// Await run signal from parent (once other children are ready).
async function sync () {
  return new Promise(resolve => {
    process.once('message', () => resolve())
    process.send({channel: 'ready'})
  })
}

// Inform parent that we are online and receive our config.
async function init () {
  return new Promise(resolve => {
    // Receive the config from the main process after start.
    process.once('message', ({message: config}) => resolve(config))
    process.send({channel: 'start'})
  })
}

