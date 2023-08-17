package edu.kafka.lost_update;

import static org.assertj.core.api.Assertions.assertThat;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SpringBootTest
@Slf4j
class LostUpdateApplicationTests {

    String bootstrapServers = "localhost:19092";
    String groupId = "" + System.nanoTime();

    AtomicReference<String> state = new AtomicReference<>();

    /**
     * Тест демонстрирует откат состояния при задержках в системе
     *
     * События идут так:
     * - П отправляет v1
     * - С1 получает v1(не коммитит)
     * - П отправляет v2
     * - С2 получает v1,v2 и коммитит
     * - C1 коммитит v1
     *
     * Результат: откат состояния.
     */
    @SneakyThrows
    @Test
    void contextLoads() {
        AdminClient adminClient = AdminClient.create(
            ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        );
        String topicName = "msg-" + System.nanoTime();
        Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, 2, (short) 1));
        adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

        KafkaProducer<String, String> producer = producer();
        producer.send(new ProducerRecord<>(topicName, "1", "v1")).get();

        AtomicBoolean v1ByC1WasRead=new AtomicBoolean();
        AtomicBoolean transactionV1ByC1WasCommitted=new AtomicBoolean();
        AtomicBoolean v1ByC2WasRead=new AtomicBoolean();
        AtomicBoolean v2ByC2WasRead=new AtomicBoolean();

        var c1Thread=new Thread(()->{
            AtomicReference<String> uncommittedData=new AtomicReference<>();
            KafkaConsumer<String, String> consumer1 = consumer(UUID.randomUUID().toString(), 100);
            consumer1.subscribe(Collections.singletonList(topicName));
            while (true){
                log.info("read mgs c1 {}", consumer1.assignment());
                var records=consumer1.poll(Duration.ofMillis(1000));

                records.forEach(r -> {
                    log.info("by {} from partition {}, msg: {}", "c1",
                        r.partition(), r.value());
                    v1ByC1WasRead.set(true);
                    uncommittedData.set(r.value());
                });
                if(!records.isEmpty()){
                    while (!v1ByC2WasRead.get()||!v2ByC2WasRead.get()){
                    }
                    break;
                }
            }
            state.set(uncommittedData.get());//commit
            transactionV1ByC1WasCommitted.set(true);
        });

        var c2Thread=new Thread(()->{
            log.info("C2 is connecting");
            KafkaConsumer<String, String> consumer2 = consumer(UUID.randomUUID().toString(), 1000);
            consumer2.subscribe(Collections.singletonList(topicName));
            log.info("C2 connected");
            while (true){
                log.info("read mgs c2 {}", consumer2.assignment());
                var records=consumer2.poll(Duration.ofMillis(1000));
                records.forEach(r -> {
                    log.info("by {} from partition {}, msg: {}", "c2",
                        r.partition(), r.value());
                    state.set(r.value());
                    if(r.value().equals("v1")){
                        v1ByC2WasRead.set(true);
                    }
                    if(r.value().equals("v2")){
                        v2ByC2WasRead.set(true);
                    }
                });
            }
        });

        c1Thread.start();
        while (!v1ByC1WasRead.get()){
        }
        c2Thread.start();

        System.out.println("sent to producer v2");
        producer.send(new ProducerRecord<>(topicName, "1", "v2")).get();


        while (!transactionV1ByC1WasCommitted.get()){
        }

        Assertions.assertEquals("v2", state.get());
    }

    KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(
            ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
            ),
            new StringSerializer(),
            new StringSerializer()
        );
    }

    KafkaConsumer<String, String> consumer(String memberId, Integer maxPoolInterval) {
        return new KafkaConsumer<>(
            ImmutableMap.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, memberId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPoolInterval
            ),
            new StringDeserializer(),
            new StringDeserializer()
        );
    }

}
