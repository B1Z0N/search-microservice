package profiles.verticles;

import profiles.model.Config;
import profiles.model.ConfigMessageCodec;
import profiles.model.Profile;
import profiles.model.ProfileMessageCodec;
import vertx.common.MicroserviceVerticle;
import io.vertx.config.ConfigChange;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

/** Verticle that replies to Profile requests */
public class ProfilesVerticle extends MicroserviceVerticle {

  // Constants

  public static final String EBA_PROFILE_FETCH = "profiles:fetch";

  // Overrides

  @Override
  public void start(Promise<Void> startPromise) {
    createServiceDiscovery();
    registerCodecs();
    setupListener();
  }

  // Private

  private void registerCodecs() {
    try {
      vertx.eventBus().registerDefaultCodec(Profile.class, new ProfileMessageCodec());
    } catch (IllegalStateException ignored) {}
  }

  private void setupListener() {
    vertx.eventBus().<JsonObject>consumer(EBA_PROFILE_FETCH, handler -> {
      Profile profile = new Profile
        .Builder("Petr", "Ivanov")
        .setAge(25)
        .build();

      handler.reply(profile);
    });
  }
}
