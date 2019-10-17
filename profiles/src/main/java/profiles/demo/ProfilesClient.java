package profiles.demo;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import profile.ProfileObject;
import profile.ProfileRequest;
import profile.ProfileServiceGrpc;
import profiles.model.Config;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import java.io.File;

/** Client that connects to our server and prints response */
public class ProfilesClient {

  // Constants

  private static final String configPath = "conf/config.json";

  // Main

  public static void main(String[] args) {
    /** Getting JSon object from file conf/config.json */
    getRetriever(configPath).getConfig(ar -> {
      if (ar.failed()) throw new RuntimeException("Configuration failed: " + ar.cause().getMessage());

      /**
       * Transforming it's configuration to a data class
       * (located in profiles/src/main/java/profiles/model/Config.java)
       */
      Config config = new Config(ar.result());
      /**
       * Connect to server via gRPC, stub is just a synonym of client
       * (read https://grpc.io/docs/guides/ for more info)
       */
      ProfileServiceGrpc.ProfileServiceBlockingStub stub = getService(config);

      try {
        /** prepare request */
        ProfileRequest request = ProfileRequest
          .newBuilder()
          .setId("some id")
          .build();

        /**
         * send it and get the answer, the path is:
         * ProfilesServiceImpl(sends request via .getProfile) ->
         *    ProfilesVerticle(giving response with "Ivanov" ...)
         *
         * API verticle settled service ProfileServiceImpl on start.
         */
        ProfileObject profile = stub
          .getProfile(request)
          .getProfile();

        System.out.println("RESPONSE IS: " + profile.toString());
      } catch (StatusRuntimeException e) {
        System.out.println("ERROR: " + e.getStatus().toString() + ", " + e.getMessage());
      }
    });
  }

  // Private

  private static ConfigRetriever getRetriever(@Nonnull String path) {
    JsonObject configPath = new JsonObject()
      .put("path", path);

    ConfigStoreOptions configStoreOptions = new ConfigStoreOptions()
      .setType("file")
      .setConfig(configPath);

    ConfigRetrieverOptions retrieverOptions = new ConfigRetrieverOptions()
      .addStore(configStoreOptions);

    return ConfigRetriever.create(Vertx.vertx(), retrieverOptions);
  }

  private static SslContext getSslContext(@Nonnull Config config) {
    File trustCert = new File(config.getTlsCa());

    try {
      return GrpcSslContexts.forClient()
        .trustManager(trustCert)
        .build();
    } catch (SSLException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  private static ProfileServiceGrpc.ProfileServiceBlockingStub getService(@Nonnull Config config) {
    String endpointHost = config.getEndpointHost();
    int endpointPort = Integer.parseInt(config.getEndpointPort());
    SslContext sslContext = getSslContext(config);

    ManagedChannel channel = NettyChannelBuilder
      .forAddress(endpointHost, endpointPort)
      .sslContext(sslContext)
      .build();

    return ProfileServiceGrpc.newBlockingStub(channel);
  }
}
