import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class KafkaHelper(private val appId: String, private val producerId: String? = null, private val enableAutoCommit: Boolean? = false) {
    companion object {
        const val KAFKA_BOOTSTRAP_SERVERS = "localhost:32886,localhost:32887,localhost:32888"
        const val INPUT_TOPIC = "input-topic"
        const val OUTPUT_TOPIC = "output-topic"
        const val NB_RECORDS = 1_000_000
    }

    fun buildKafkaConsumerProps(): Properties {
        val consumerProps = Properties()
        val clientId = producerId ?: UUID.randomUUID().toString()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KAFKA_BOOTSTRAP_SERVERS
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "$appId-group"
        consumerProps[ConsumerConfig.CLIENT_ID_CONFIG] = "$appId-$clientId"
        consumerProps[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_committed"
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        consumerProps[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = enableAutoCommit
        return consumerProps
    }

    fun buildKafkaProducerProps(): Properties {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KAFKA_BOOTSTRAP_SERVERS
        producerProps[ProducerConfig.ACKS_CONFIG] = "all"
        producerProps[ProducerConfig.RETRIES_CONFIG] = "3"
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.canonicalName
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.canonicalName
        if (producerId != null) {
            producerProps[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = "sameTxIdForAll-$producerId"
            producerProps[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = "true"
        }
        return producerProps
    }
}