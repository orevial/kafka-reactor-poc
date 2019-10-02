import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.time.Duration
import kotlin.system.exitProcess

object ExactlyOnceReactorProcessor {

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

            val helper = KafkaHelper(
                    appId = "exactly-once-reactor-processor",
                    producerId = producerId,
                    enableAutoCommit = true
            )
            val consumerProps = helper.buildKafkaConsumerProps()
            val producerProps = helper.buildKafkaProducerProps()

            val receiverOptions = ReceiverOptions.create<String, String>(consumerProps)
                    .subscription(listOf(KafkaHelper.INPUT_TOPIC))
//                    .commitBatchSize(0)
//                    .commitInterval(Duration.ZERO)
            receiver = KafkaReceiver.create(receiverOptions)
            sender = KafkaSender.create(SenderOptions.create(producerProps))

            doStuff()
        }
    }

    private fun producerId(): String {
        return System.getenv("PRODUCER_ID")
    }

    private fun doStuff() {
        receiver!!.receiveExactlyOnce(sender!!.transactionManager())
                .concatMap(this::processBatch)
                .onErrorResume { e ->
                    println("Unable to process batch of record: $e")
                    sender!!.transactionManager().abort<Any>().then(Mono.error(e))
                }
                .take(Duration.ofDays(1))
                .blockLast()
    }

    private fun processBatch(recordBatch: Flux<ConsumerRecord<String, String>>): Mono<String> {
        val senderRecordFlux = recordBatch
                //.doOnNext { record -> println("Received record : " + record.value()) }
                .map { record ->
                    SenderRecord.create<String, String, Any>(
                            ProducerRecord(KafkaHelper.OUTPUT_TOPIC, record.key(), "Producer " + producerId + " - " + record.value()), null)
                }

        return sender!!.send(senderRecordFlux)
                .last()
                .flatMap { sender!!.transactionManager().commit<Any>().map { "SUCCESS" } }
                .switchIfEmpty(Mono.just("SUCCESS"))
                .doOnNext { print("...") }
    }
}
