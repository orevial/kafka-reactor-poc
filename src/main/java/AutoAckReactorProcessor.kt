import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.time.Duration
import kotlin.system.exitProcess


object AutoAckReactorProcessor {

    private var sender: KafkaSender<String, String>? = null
    private var receiver: KafkaReceiver<String, String>? = null
    private var producerId: String? = null

    @JvmStatic
    fun main(args: Array<String>) {
        producerId = producerId()
        if (producerId == null) {
            println("Did not found producer id, exiting...")
            exitProcess(1)
        } else {
            println("Starting with producer id $producerId")

            val helper = KafkaHelper(appId = "at-least-once-reactor-processor", enableAutoCommit = true)
            val consumerProps = helper.buildKafkaConsumerProps()
            val producerProps = helper.buildKafkaProducerProps()

            val receiverOptions = ReceiverOptions.create<String, String>(consumerProps)
                    .subscription(listOf(KafkaHelper.INPUT_TOPIC))
            receiver = KafkaReceiver.create(receiverOptions)
            sender = KafkaSender.create(SenderOptions.create(producerProps))

            doStuff()
        }
    }

    private fun producerId(): String {
        return System.getenv("PRODUCER_ID")
    }

    private fun doStuff() {
        receiver!!.receiveAutoAck()
                .concatMap(this::processBatch)
                .take(Duration.ofDays(1))
                .blockLast()
    }

    private fun processBatch(recordBatch: Flux<ConsumerRecord<String, String>>): Flux<SenderResult<Any>> {
        val senderRecordFlux = recordBatch
                //.doOnNext { record -> println("Received record : " + record.value()) }
                .map { record ->
                    SenderRecord.create<String, String, Any>(
                            ProducerRecord(KafkaHelper.OUTPUT_TOPIC, record.key(), "Producer " + producerId + " - " + record.value()), null)
                }

        return sender!!.send(senderRecordFlux)
                .doOnNext { print("...") }
    }

    private fun transform(record: ReceiverRecord<String, String>): ProducerRecord<String, String> {
        return ProducerRecord(KafkaHelper.OUTPUT_TOPIC, record.key(), "Producer $producerId - ${record.value()}")
    }
}