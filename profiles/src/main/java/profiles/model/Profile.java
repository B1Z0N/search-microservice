package profiles.model;

import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/** Data class: representation of profile info in plain java class */
public class Profile {

  // Constants

  private static final String FIRST_NAME = "firstName";
  private static final String LAST_NAME = "lastName";
  private static final String AGE = "age";

  private static final int DEFAULT_AGE = 0;

  // Variables

  private final JsonObject mJson;
  private final String mFirstName;
  private final String mLastName;
  private final Integer mAge;

  // Constructors

  public Profile(@Nonnull JsonObject json) {
    mJson = json;

    mFirstName = json.getString(FIRST_NAME);
    mLastName = json.getString(LAST_NAME);
    mAge = json.getInteger(AGE, DEFAULT_AGE);
  }

  private Profile(@Nonnull String firstName, @Nonnull String lastName, @Nullable Integer age) {
    mJson = new JsonObject()
      .put(FIRST_NAME, firstName)
      .put(LAST_NAME, lastName);
    if (age != null) mJson.put(AGE, age);

    mFirstName = firstName;
    mLastName = lastName;
    mAge = age;
  }

  // Accessors

  public JsonObject toJson() {
    return mJson;
  }

  public String getFirstName() {
    return mFirstName;
  }

  public String getLastName() {
    return mLastName;
  }

  public Integer getAge() {
    return mAge;
  }

  // Utils

  @Override
  public String toString() {
    return "Profile{" +
      "mFirstName='" + mFirstName + '\'' +
      ", mLastName='" + mLastName + '\'' +
      ", mAge=" + mAge +
      '}';
  }

  // Builder

  /** Search `builder pattern` */
  public static class Builder {

    private final String mFirstName;
    private final String mLastName;
    private int mAge = 0;

    public Builder(String firstName, String lastName) {
      mFirstName = firstName;
      mLastName = lastName;
    }

    public Builder setAge(int age) {
      mAge = age;

      return this;
    }

    public Profile build() {
      return new Profile(mFirstName, mLastName, mAge);
    }
  }
}
