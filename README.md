# wd40

Benchmark for the `squeaky` NSQ library.

## Setup

* Install Node.js 8+
* Install dependencies `npm i`
* Install NSQ or Docker

## Running

If you have NSQ installed and running locally:
```shell
$ npm start
```

If you have Docker installed:
```shell
$ docker compose up
```

If you wish to test against another NSQ:
```shell
$ node benchmark.js --host <custom-nsq-host>
```

## Options
```shell
$ node benchmark.js  --help
Options:
  --help              Show help                                        [boolean]
  --version           Show version number                              [boolean]
  --host, -h          nsqd host                  [string] [default: "localhost"]
  --port, -p          nsqd port                         [number] [default: 4150]
  --qos, -q           max outstanding messages             [number] [default: 1]
  --message-size, -m  bytes per message                   [number] [default: 64]
  --batch-size, -b    messages per batch                   [number] [default: 1]
  --topic, -t         topic on which to publish/subscribe
                                           [string] [default: "bench#ephemeral"]
  --channel, -c       channel on which to subscribe
                                            [string] [default: "wd40#ephemeral"]
```
