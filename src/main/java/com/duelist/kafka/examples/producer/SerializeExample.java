package com.duelist.kafka.examples.producer;

import com.duelist.kafka.examples.producer.serializers.Customer;
import com.duelist.kafka.examples.producer.serializers.CustomerSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class SerializeExample {
    public static void main(String[] args) throws InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "customerContacts";
        int wait = 500;

        KafkaProducer<String, Customer> producer = new KafkaProducer<>(kafkaProps, new StringSerializer(), new CustomerSerializer());
// We keep producing new events until someone ctrl-c
        while (true) {
            Customer customer = new Customer(new Random().nextInt(), "kek");
            System.out.println("Generated customer " + customer.toString());
            ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, String.valueOf(customer.getID()), customer);
            producer.send(record);
            TimeUnit.MILLISECONDS.sleep(wait);
        }
    }
}
