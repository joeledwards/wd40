# wd40

[![NPM version][npm-image]][npm-url]

Benchmark for the `squeaky` NSQ library.

## Installation

```shell
$ npm install -g @buzuli/wd40
```

## Running

If you have NSQ installed and running locally bound to 4150:
```shell
$ wd40
```

If you wish to test against another NSQ:
```shell
$ wd40 --host <custom-nsq-host>
```

## Options
```shell
$ wd40  --help
Options:
  --help                                 Show help                     [boolean]
  --version                              Show version number           [boolean]
  --host, -h                             nsqd host
                                                 [string] [default: "localhost"]
  --port, -p                             nsqd port      [number] [default: 4150]
  --qos, -q                              max outstanding messages
                                                           [number] [default: 1]
  --message-size, -m                     bytes per message[number] [default: 64]
  --batch-size, -b                       messages per batch[number] [default: 1]
  --topic, -t                            topic on which to publish/subscribe
                                           [string] [default: "bench#ephemeral"]
  --channel, -c                          channel on which to subscribe
                                            [string] [default: "wd40#ephemeral"]
  --lib, -l                              the client library to use for NSQ
                                         (nsqjs | squeaky)
                                                   [string] [default: "squeaky"]
  --pub-lib, -P                          the client library to use for NSQ
                                         publishes (nsqjs | squeaky)    [string]
  --sub-lib, -S                          the client library to use for NSQ
                                         subscriptions (nsqjs | squeaky)[string]
  --publisher-count, --pub-count, --pc   number of publisher processes to launch
                                                           [number] [default: 1]
  --subscriber-count, --sub-count, --sc  number of subscriber processes to
                                         launch            [number] [default: 1]
  --min-report-delay, -d                 minimum delay (in ms) between reports
                                                        [number] [default: 1000]
  --max-report-delay, -D                 maximum delay (in ms) between reports
                                                        [number] [default: 5000]
  --log-mode, -L                         set log-a-log mode
                                                       [string] [default: "utc"]
  --brighten-my-day, -B                  brighten my day (overrides --log-mode)
                                                      [boolean] [default: false]
```


![](https://github.com/joeledwards/wd40/raw/master/screenshot.png)

[npm-url]: https://www.npmjs.com/package/@buzuli/wd40
[npm-image]: https://img.shields.io/npm/v/@buzuli/wd40.svg
