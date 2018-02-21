module.exports = child

function defer () {
  let deferred = {}

  deferred.promise = new Promise((resolve, reject) => {
    deferred.resolve = resolve
    deferred.reject = reject
  })

  return deferred
}

async function child ({setup, logic}) {
  let reason = undefined;
  const completion = defer()
  try {
    const config = await init()

    let setupComplete = false
    const context = {
      config,
      join: () => completion.promise,
      report: info => process.send({channel: 'report', message: info}),
      halt: error => {
        console.warn('halt() called')
        if (setupComplete) {
          if (error) {
            completion.reject(error) 
          } else {
            completion.resolve()
          }
        } else {
          process.exit(1)
        }
      },
    }

    process.on('message', ({channel, message}) => {
      switch (channel) {
        case 'halt': return context.halt()
      }
    })

    // Perform setup logic if supplied.
    if (setup) {
      await setup(context)
    }

    // Await instruction to run.
    console.log('ready; awaiting run command from parent')
    await sync()

    setupComplete = true

    // Run the child's logic.
    console.log('running')
    await logic(context)

    // Done running.
    console.log('done')
  } catch (error) {
    reason = error
    process.send({channel: 'error', message: reason})
  } finally {
    console.log('exiting')
    process.exit(reason ? 1 : 0)
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

