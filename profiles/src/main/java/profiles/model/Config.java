package profiles.model;

import io.vertx.core.json.JsonObject;
import javax.annotation.Nonnull;

/** Data class: representation of conf/config.json in plain java class */
public class Config {

  // Constants

  private static final String ENDPOINT = "endpoint";
  private static final String ENDPOINT_HOST = "host";
  private static final String ENDPOINT_PORT = "port";
  private static final String TLS = "tls";
  private static final String TLS_CERT_CHAIN = "cert_chain";
  private static final String TLS_PRIV_KEY = "priv_key";
  private static final String TLS_CA = "ca";

  // Variables

  private final JsonObject mConfigObject;
  private final String mEndpointHost;
  private final String mEndpointPort;
  private final String mTlsCertChain;
  private final String mTlsPrivKey;
  private final String mTlsCa;

  // Constructors

  public Config(@Nonnull JsonObject config) {
    mConfigObject = config;

    JsonObject endpoint = config.getJsonObject(ENDPOINT);
    mEndpointHost = endpoint.getString(ENDPOINT_HOST);
    mEndpointPort = endpoint.getString(ENDPOINT_PORT);

    JsonObject tls = config.getJsonObject(TLS);
    mTlsCertChain = tls.getString(TLS_CERT_CHAIN);
    mTlsPrivKey = tls.getString(TLS_PRIV_KEY);
    mTlsCa = tls.getString(TLS_CA);
  }

  // Public

  public JsonObject toJson() {
    JsonObject endpoint = new JsonObject()
      .put(ENDPOINT_HOST, mEndpointHost)
      .put(ENDPOINT_PORT, mEndpointPort);

    JsonObject tls = new JsonObject()
      .put(TLS_CERT_CHAIN, mTlsCertChain)
      .put(TLS_PRIV_KEY, mTlsPrivKey)
      .put(TLS_CA, mTlsCa);

    return new JsonObject()
      .put(ENDPOINT, endpoint)
      .put(TLS, tls);
  }

  // Accessors

  JsonObject getConfigObject() {
    return mConfigObject;
  }

  public String getEndpointHost() {
    return mEndpointHost;
  }

  public String getEndpointPort() {
    return mEndpointPort;
  }

  public String getTlsCertChain() {
    return mTlsCertChain;
  }

  public String getTlsPrivKey() {
    return mTlsPrivKey;
  }

  public String getTlsCa() {
    return mTlsCa;
  }

  // Utils

  @Override
  public String toString() {
    return "Config{" +
      "mEndpointHost='" + mEndpointHost + '\'' +
      ", mEndpointPort=" + mEndpointPort +
      ", mTlsCertChain='" + mTlsCertChain + '\'' +
      ", mTlsPrivKey='" + mTlsPrivKey + '\'' +
      ", mTlsCa='" + mTlsCa + '\'' +
      '}';
  }
}
