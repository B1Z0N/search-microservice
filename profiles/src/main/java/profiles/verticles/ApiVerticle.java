package scales.verticles;

import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import scales.model.Config;
import scales.model.ConfigMessageCodec;
import scales.model.OriginID;
import scales.model.OriginID.photoType;
import scales.model.OriginIDCodec;

import vertx.common.MicroserviceVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import java.util.*;

import static scales.verticles.ConfigurationVerticle.EBA_CONFIG_FETCH;
import static scales.verticles.ConfigurationVerticle.EBA_CONFIG_UPDATE;
import static scales.verticles.ScaleVerticle.EBA_DELETE_ORIGIN;
import static scales.verticles.ScaleVerticle.EBA_SCALE_ORIGIN;

// verticle for communicating with kafka and internal implementation
public class ApiVerticle extends MicroserviceVerticle {

  // Overrides

  private KafkaConsumer<String, String> mConsumer;
  private KafkaProducer<String, String> mSagasProducer;

  private String mKafkaHost;
  private String mKafkaPort;
  private String mScaleRequest;
  private String mDeleteRequest;
  private String mUserpicsTopic;
  private String mPhotosTopic;
  private String mSagasTopic;


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
    mUserpicsTopic = config.getUserpicsTopic();
    mPhotosTopic = config.getPhotosTopic();
    mDeleteRequest = config.getDeleteRequest();
    mScaleRequest = config.getScaleRequest();
    mSagasTopic = config.getSagasTopic();

    if (mConsumer != null) mConsumer.unsubscribe();
    setupSagasProducer();
    setupKafkaConsumers();
  }


  private void setupSagasProducer() {
    Map<String, String> kafkaConfig = new HashMap<>();
    kafkaConfig.put("bootstrap.servers", String.join(":", mKafkaHost, mKafkaPort));
    kafkaConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaConfig.put("group.id", "my_group");

    mSagasProducer = KafkaProducer.create(vertx, kafkaConfig);
  }


  private void setupKafkaConsumers() {
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

      photoType type = record.topic().equals(mUserpicsTopic) ? photoType.USERPIC : photoType.PHOTO;

      if (record.value().startsWith(mScaleRequest))
        photoScale(record.value().substring(mScaleRequest.length()), type);
      else if (record.value().startsWith(mDeleteRequest))
        photoDelete(record.value().substring(mDeleteRequest.length()), type);
    });

    Set<String> topics = new HashSet<>();
    topics.add(mUserpicsTopic);
    topics.add(mPhotosTopic);

    mConsumer.subscribe(topics, ar -> {
      if (ar.succeeded()) {
        vsuccess(String.format("Subscribed to %s and %s successfully", mUserpicsTopic, mPhotosTopic));
      } else {
        verror(
                String.format("Could not subscribe to %s or %s: ", mUserpicsTopic, mPhotosTopic) + ar.cause().getMessage()
        );
      }
    });

    new TopicPartition(mUserpicsTopic, 0);
    Set<TopicPartition> topicPs = new HashSet<>();
    topicPs.add(new TopicPartition(mUserpicsTopic, 0));
    topicPs.add(new TopicPartition(mPhotosTopic, 0));
  }


  private void sagas(String msg) {
    mSagasProducer.write(
            KafkaProducerRecord.create(mSagasTopic, msg)
    );
  }


  private void photoScale(@Nonnull String ID, photoType type) {
    vertx.eventBus().<OriginID>request(EBA_SCALE_ORIGIN, new OriginID(ID, type), ar -> {
      if (ar.failed()) {
        // send "ERR" to sagas
        verror("Scaling, " + type.toString() + " : " + ID + " | " + ar.cause());
        sagas("photo-scale:" + mScaleRequest + "err:" + ID);
        return;
      }

      // send "OK" to sagas
      vsuccess("Scaling, " + type.toString() + " : " + ID);
      sagas("photo-scale:" + mScaleRequest + "ok:" + ID);
    });
  }


  private void photoDelete(@Nonnull String ID, photoType type) {
    vertx.eventBus().<OriginID>request(EBA_DELETE_ORIGIN, new OriginID(ID, type), ar -> {
      if (ar.failed()) {
        // send "ERR" to sagas
        verror("Deleting, " + type.toString() + " : " + ID + " | " + ar.cause());
        sagas("photo-scale:" + mDeleteRequest + ":err:" + ID);
        return;
      }

      // send "OK" to sagas
      vsuccess("" + type.toString() + " : " + ID);
      sagas("photo-scale:" + mDeleteRequest + "ok:" + ID);
    });
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
    try {
      vertx.eventBus().registerDefaultCodec(OriginID.class, new OriginIDCodec());
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