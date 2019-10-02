import KafkaHelper.Companion.INPUT_TOPIC
import KafkaHelper.Companion.NB_RECORDS
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*
import java.util.stream.IntStream

object InputProducer {

    private val helper = KafkaHelper(appId = "input-producer")

    @JvmStatic
    fun main(args: Array<String>) {

        val producerProps = helper.buildKafkaProducerProps()

        val producer = KafkaProducer<String, String>(producerProps)

        println("Starting insertions of $NB_RECORDS records...")
        IntStream.range(0, NB_RECORDS)
                .forEach { idx -> producer.send(ProducerRecord(INPUT_TOPIC, "key-$idx", "${UUID.randomUUID()} - at index $idx")) }
        println("Done inserting $NB_RECORDS records !")
    }
}
