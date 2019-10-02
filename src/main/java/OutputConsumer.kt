import KafkaHelper.Companion.NB_RECORDS
import KafkaHelper.Companion.OUTPUT_TOPIC
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

object OutputConsumer {

    private val helper = KafkaHelper(appId = "output-consumer", enableAutoCommit = true)

    @JvmStatic
    fun main(args: Array<String>) {

        val consumerProps = helper.buildKafkaConsumerProps()

        val consumer = KafkaConsumer<String, String>(consumerProps)
        consumer.subscribe(listOf(OUTPUT_TOPIC))

        var totalRead = 0
        var lastIntermediateStep = 0
        var lastPrint = 0
        while (true) {
            val read = consumer.poll(Duration.ofSeconds(10)).count()

            if (read > 0) {
                totalRead += read
                if (totalRead > lastIntermediateStep + 10000) {
                    lastIntermediateStep = totalRead
                    println("Read $totalRead records...")
                }
            }

            if (totalRead == NB_RECORDS && lastPrint != totalRead) {
                lastPrint = totalRead
                println("Read exactly expected number of records: $totalRead")
            }

            if (totalRead > NB_RECORDS && lastPrint != totalRead) {
                lastPrint = totalRead
                println("Read more records than expected: read $totalRead where expected $NB_RECORDS")
            }
        }
    }
}
