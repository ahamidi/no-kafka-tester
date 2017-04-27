const kafka = require('no-kafka')
const promise = require('bluebird')

const kafkaConnectionString = process.env.KAFKA_URL || 'kafka://localhost:9092'
var topicName = process.env.KAFKA_TOPIC || 'messages'
var groupName = process.env.KAFKA_GROUP || 'messages'
if (process.env.KAFKA_PREFIX) {
  topicName = `${process.env.KAFKA_PREFIX}${topicName}`
  groupName = `${process.env.KAFKA_PREFIX}${groupName}`
}
const kafkaOptions = {
  connectionString: kafkaConnectionString,
  topic: topicName,
  groupId: groupName
}

if (process.env.KAFKA_CLIENT_CERT) {
  kafkaOptions.ssl = {
    cert: process.env.KAFKA_CLIENT_CERT,
    key: process.env.KAFKA_CLIENT_CERT_KEY
  }
}

class SimpleConsumer {
    constructor (options) {
        this.connectionString = options.connectionString
        this.topic = options.topic || 'messages'
        this.groupId = options.groupId
        this.consumer = new kafka.GroupConsumer({connectionString: this.connectionString, groupId: this.groupId})

        const self = this
        this.dataHandler = function (messageSet, topic, partition) {
            if (!self.timeout) {
              self.timeout = setInterval(function () {
                self.consumer.commitOffset({topic: self.topic, partition: 0, offset: 0})
              }, 200)
            }

            return promise.each(messageSet, function(m) {
                console.log(topic, partition, m.offset, m.message.value.toString('utf8'))
                return self.consumer.commitOffset({topic: topic, partition: partition, offset: m.offset})
            })
        }
    }

    start () {
        const strategies = [{
            subscriptions: [this.topic],
            handler: this.dataHandler
        }]
        this.consumer.init(strategies)
    }
}

const simpleConsumer = new SimpleConsumer(kafkaOptions)
simpleConsumer.start()
