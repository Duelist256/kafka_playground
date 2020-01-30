package com.duelist.kafka.examples.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducesWithAvroSerializationExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
//        props.put("schema.registry.url", url); TODO ADD url

        String schemaString =
                "{\"namespace\": \"customerManagement.avro\"," +
                        "\"type\": \"record\", " +
                        "\"name\": \"Customer\"," +
                        "\"fields\": [" +
                            "{\"name\": \"id\", \"type\": \"int\"}," +
                            "{\"name\": \"name\", \"type\": \"string\"}," +
                            "{\"name\": \"email\", \"type\": [\"null\",\"string\"], \"default\":\"null\" }" +
                        "]}";
        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        int customers = 10;

        for (int nCustomers = 0; nCustomers < customers; nCustomers++) {
            String name = "exampleCustomer" + nCustomers;
            String email = "example " + nCustomers + "@example.com";
            GenericRecord customer = new GenericData.Record(schema);
            customer.put("id", nCustomers);
            customer.put("name", name);
            customer.put("email", email);
            ProducerRecord<String, GenericRecord> data =
                    new ProducerRecord<>("customerContacts", name, customer);
            producer.send(data);
        }
    }
}