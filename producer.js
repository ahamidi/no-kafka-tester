const kafka = require('no-kafka')
const UUID = require('uuid')

const kafkaConnectionString = process.env.KAFKA_URL || 'kafka://localhost:9092'
var topicName = process.env.KAFKA_TOPIC || 'messages'
if (process.env.KAFKA_PREFIX) {
  topicName = `${process.env.KAFKA_PREFIX}${topicName}`
}
const kafkaOptions = {
  connectionString: kafkaConnectionString,
  topic: topicName
}

if (process.env.KAFKA_CLIENT_CERT) {
  kafkaOptions.ssl = {
    cert: process.env.KAFKA_CLIENT_CERT,
    key: process.env.KAFKA_CLIENT_CERT_KEY
  }
}

class SimpleProducer {
    constructor (options) {
        this.topic = options.topic || 'messages'
        this.producer = new kafka.Producer({ connectionString: options.connectionString})
        this.producer.init()
    }

    sendMessage (message) {
        return this.producer.send({
            topic: this.topic,
            partition: Math.floor(Math.random() * 2),
            message: {
                value: message
            }
        })
    }
}

const producer = new SimpleProducer(kafkaOptions)
setInterval(function () {
    producer.sendMessage(UUID.v4())

}, 500)
