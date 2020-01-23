package profiles.verticles;

import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import profiles.model.Config;
import profiles.model.ConfigMessageCodec;

import vertx.common.MicroserviceVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import java.util.*;

import static profiles.verticles.ConfigurationVerticle.EBA_CONFIG_FETCH;
import static profiles.verticles.ConfigurationVerticle.EBA_CONFIG_UPDATE;

// verticle for communicating with kafka and internal implementation
public class ApiVerticle extends MicroserviceVerticle {

  // Overrides

  private KafkaConsumer<String, String> mConsumer;
  private KafkaProducer<String, String> mProducer;

  private String mKafkaHost;
  private String mKafkaPort;
  private String mGeoSearchTopic;
  private String mTagSearchTopic;
  private String mGeoOutputTopic;
  private String mTagOutputTopic;

  private String mGeoIndex;
  private String mTagIndex;

  // Overrides

  @Override
  public void start(Promise<Void> startPromise) {
    registerCodecs();
    setupConfigListener();
    setupConfig(startPromise);
  }

  // Private


  private void setupFromConfig(@Nonnull Config config) {
    mKafkaHost = config.getKafkaHost();
    mKafkaPort = config.getKafkaPort();
    mGeoSearchTopic = config.getGeoSearchTopic();
    mTagSearchTopic = config.getTagSearchTopic();
    mGeoOutputTopic = config.getGeoOutputTopic();
    mTagOutputTopic = config.getTagOutputTopic();

    if (mConsumer != null) mConsumer.unsubscribe();

    setupKafkaConsumer();
    setupKafkaProducer();
  }


  private void setupKafkaProducer() {
    Map<String, String> kafkaConfig = new HashMap<>();
    kafkaConfig.put("bootstrap.servers", String.join(":", mKafkaHost, mKafkaPort));
    kafkaConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaConfig.put("group.id", "my_group");

    mProducer = KafkaProducer.create(vertx, kafkaConfig);
  }


  private void setupKafkaConsumer() {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", String.join(":", mKafkaHost, mKafkaPort));
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "latest");
    config.put("enable.auto.commit", "true");

    mConsumer = KafkaConsumer.create(getVertx(), config);
    mConsumer.handler(record -> {
      vinfo("Handling record: " + record.topic() + "  " + ":" + record.value());

      if (record.topic().equals(mGeoSearchTopic)) {
        // geo data is being searched
        searchRequest(mGeoOutputTopic, mGeoIndex, record.value());
      } else {
        // tag data is being searched
        searchRequest(mTagOutputTopic, mTagIndex, record.value());
      }
    });

    Set<String> topics = new HashSet<>();
    topics.add(mGeoSearchTopic);
    topics.add(mTagSearchTopic);

    mConsumer.subscribe(topics, ar -> {
      if (ar.succeeded()) {
        vsuccess(String.format("Subscribed to %s and %s successfully", mGeoSearchTopic, mTagSearchTopic));
      } else {
        verror(
                String.format("Could not subscribe to %s or %s: ", mGeoSearchTopic, mTagSearchTopic) + ar.cause().getMessage()
        );
      }
    });
  }

  private void searchRequest(String outputTopic, String index, String name) {
    JsonObject searchConfig = new JsonObject()
            .put("action", "search")
            .put("_index", index)
            .put("_type", "kafka-connect")
            .put("query", new JsonObject()
                    .put("match", new JsonObject()
                            .put("name", name)
                    )
            );

    vertx.eventBus().request("et.vertx.elasticsearch", searchConfig, ar -> {
      if (ar.failed()) {
        verror("Searching '" + name + "' in '" + index + "' index: " + ar.cause());
        return;
      }

      vsuccess("Searching '" + name + "' in '" + index + "' index");
      mProducer.write(
              KafkaProducerRecord.create(
                      outputTopic,
                      assembleHits((JsonObject) ar.result().body()).toString())
      );
    });
  }

  private JsonArray assembleHits(@Nonnull JsonObject whole) {
    JsonArray hits = whole.getJsonObject("hits").getJsonArray("hits");
    JsonArray result = new JsonArray();
    for (int i = 0; i < hits.size(); i++) {
      JsonObject current = hits.getJsonObject(i);
      result.addAll(
              current.getJsonObject("_source")
                      .getJsonArray("timelapses")
      );
    }

    return result;
  }


  /**
   * Set our channels of communication using Config and Profile classes
   * and codecs for them
   */
  private void registerCodecs() {
    try {
      vertx.eventBus().registerDefaultCodec(Config.class, new ConfigMessageCodec());
    } catch (IllegalStateException ignored) {
    }
  }


  /**
   * Listen on configuration changes and update sizes accordingly
   */
  private void setupConfigListener() {
    vertx.eventBus().<Config>consumer(EBA_CONFIG_UPDATE, configAr -> {
      setupFromConfig(configAr.body());
      vinfo("New kafka setup came up: ");
    });
  }

  private void setupConfig(Promise<Void> startPromise) {
    Promise<Config> promise = Promise.promise();
    promise.future().setHandler(configAr -> {
      if (configAr.failed()) {
        verror("Config fetch: " + configAr.cause().getMessage());
      } else {
        vsuccess("Config fetch, kafka: " +
                configAr.result().getKafkaHost() + ":" + configAr.result().getKafkaPort());
      }
    });
    fetchConfig(promise, startPromise);
  }

  /**
   * Get sizes from eventbus and pass it to promise
   */
  private void fetchConfig(Promise<Config> promise, Promise<Void> startPromise) {
    vertx.eventBus().<Config>request(EBA_CONFIG_FETCH, new JsonObject(), configAr -> {
      if (configAr.failed()) {
        promise.fail(configAr.cause());
        startPromise.fail(configAr.cause());
        verror("Setup");
        return;
      }

      setupFromConfig(configAr.result().body());
      startPromise.complete();
      vsuccess("Setup");
    });
  }
}