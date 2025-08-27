package org.beam;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.*;
import org.beam.dto.NearbyDTO;
import org.beam.models.Nearby;
import org.beam.fn.NearbyCombiner;
import org.beam.fn.PointCombiner;
import org.beam.fn.ExtractPointFn;
import org.beam.models.Location;
import org.joda.time.Duration;

import java.io.IOException;

public class Main {

    private static final String KAFKA_TOPIC_IN = "in-gps";
    private static final String KAFKA_NODE = "broker:9092";
    private static final String KAFKA_TOPIC_OUT = "out-nearby-person";
    private static final String SCHEMA_REGISTRY_URL = "http://schemaregistry:8085";

    public static void main(String[] args) {
        ImmutableMap<String, Object> SERIALIZER_CONF;
        try (SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 10)){
            int schemaId = schemaRegistryClient.getLatestSchemaMetadata(KAFKA_TOPIC_OUT + "-value").getId();
            SERIALIZER_CONF = ImmutableMap.of(
                    "schema.registry.url", SCHEMA_REGISTRY_URL,
                    "auto.register.schemas", "false",
                    "use.schema.id", schemaId
            );
        } catch (RestClientException | IOException e) {
            throw new RuntimeException(e);
        }
        ConfluentSchemaRegistryDeserializerProvider<GenericRecord> providerIn = ConfluentSchemaRegistryDeserializerProvider.of(
                SCHEMA_REGISTRY_URL,
                10,
                KAFKA_TOPIC_IN+"-value");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(KafkaIO.<String, GenericRecord>read()
                        .withRedistribute()
                        .withRedistributeNumKeys(8)
                        .withBootstrapServers(KAFKA_NODE)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(providerIn)
                        .withTopic(KAFKA_TOPIC_IN)
                        .withTimestampPolicyFactory(TimestampPolicyFactory.withCreateTime(Duration.standardMinutes(10)))
                        .withoutMetadata()
                )
                .apply(ParDo.of(new ExtractPointFn()))
                .setCoder(KvCoder.of(BigEndianLongCoder.of(), AvroCoder.of(Location.class)))
                .apply(
                        Window.<KV<Long, Location>>into(
                                SlidingWindows
                                        .of(Duration.standardMinutes(5))
                                        .every(Duration.standardSeconds(150))
                                )
                                .triggering(
                                        Repeatedly.forever(
                                                AfterWatermark.pastEndOfWindow()
                                                        .withEarlyFirings(
                                                                AfterProcessingTime
                                                                        .pastFirstElementInPane()
                                                                        .plusDelayOf(Duration.standardSeconds(1))
                                                        )
                                                        .withLateFirings(AfterPane.elementCountAtLeast(1))
                                        )
                                )
                                .withAllowedLateness(Duration.standardMinutes(5))
                                .discardingFiredPanes()
                )
                .apply(GroupByKey.create())
                .apply(ParDo.of(new PointCombiner()))
                .setCoder(KvCoder.of(BigEndianLongCoder.of(), AvroCoder.of(Nearby.class)))
                .apply(GroupByKey.create())
                .apply(ParDo.of(new NearbyCombiner()))
                .setCoder(KvCoder.of(BigEndianLongCoder.of(), AvroCoder.of(NearbyDTO.class)))
                .apply(
                        KafkaIO.<Long, NearbyDTO>write()
                        .withBootstrapServers(KAFKA_NODE)
                        .withTopic(KAFKA_TOPIC_OUT)
                        .withKeySerializer(LongSerializer.class)
                        .withValueSerializer((Class) KafkaAvroSerializer.class)
                        .withProducerConfigUpdates(SERIALIZER_CONF)
                );
        pipeline.run();
    }
}