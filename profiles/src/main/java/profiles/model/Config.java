package profiles.model;

import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;

/**
 * Data class: representation of conf/config.json in plain java class
 */
public class Config {

  // Constants

  private static final String KAFKA = "kafka";
  private static final String KAFKA_HOST = "host";
  private static final String KAFKA_PORT = "port";
  private static final String GEO_SEARCH_TOPIC = "geoSearchTopic";
  private static final String TAG_SEARCH_TOPIC = "tagSearchTopic";
  private static final String GEO_OUTPUT_TOPIC = "geoOutputTopic";
  private static final String TAG_OUTPUT_TOPIC = "tagOutputTopic";

  // Variables

  private final JsonObject mConfigObject;

  private final String mKafkaHost;
  private final String mKafkaPort;
  private final String mGeoSearchTopic;

  private final String mTagSearchTopic;
  private final String mGeoOutputTopic;
  private final String mTagOutputTopic;

  // Constructors

  public Config(@Nonnull JsonObject config) {
    mConfigObject = config;

    JsonObject kafka = config.getJsonObject(KAFKA);
    mKafkaHost = kafka.getString(KAFKA_HOST);
    mKafkaPort = kafka.getString(KAFKA_PORT);
    mGeoSearchTopic = kafka.getString(GEO_SEARCH_TOPIC);
    mTagSearchTopic = kafka.getString(TAG_SEARCH_TOPIC);
    mGeoOutputTopic = kafka.getString(GEO_OUTPUT_TOPIC);
    mTagOutputTopic = kafka.getString(TAG_OUTPUT_TOPIC);
  }

  // Public

  JsonObject toJson() {
    JsonObject kafka = new JsonObject()
            .put(KAFKA_HOST, mKafkaHost)
            .put(KAFKA_PORT, mKafkaPort)
            .put(GEO_SEARCH_TOPIC, mGeoSearchTopic)
            .put(TAG_SEARCH_TOPIC, mTagSearchTopic)
            .put(GEO_OUTPUT_TOPIC, mGeoOutputTopic)
            .put(TAG_OUTPUT_TOPIC, mTagOutputTopic);

    return new JsonObject()
            .put(KAFKA, kafka);
  }

  // Accessors

  JsonObject getConfigObject() {
    return mConfigObject;
  }

  public String getKafkaHost() {
    return mKafkaHost;
  }

  public String getKafkaPort() {
    return mKafkaPort;
  }

  public String getGeoSearchTopic() {
    return mGeoSearchTopic;
  }

  public String getTagSearchTopic() {
    return mTagSearchTopic;
  }

  public String getGeoOutputTopic() {
    return mGeoOutputTopic;
  }

  public String getTagOutputTopic() {
    return mTagOutputTopic;
  }
}