package profiles.model;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

/** Class for transfering profile messages over EventBus */
public class ProfileMessageCodec implements MessageCodec<Profile, Profile> {

  // Overrides

  @Override
  public void encodeToWire(Buffer buffer, Profile contracts) {
    String str = contracts.toJson()
      .encode();
    int length = str.getBytes().length;
    buffer.appendInt(length);
    buffer.appendString(str);
  }

  @Override
  public Profile decodeFromWire(int pos, Buffer buffer) {
    int position = pos;
    int length = buffer.getInt(position);
    String jsonStr = buffer.getString(position += 4, position += length);
    JsonObject json = new JsonObject(jsonStr);

    return new Profile(json);
  }

  @Override
  public Profile transform(Profile profile) {
    return profile;
  }

  @Override
  public String name() {
    return getClass().getSimpleName();
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }
}
