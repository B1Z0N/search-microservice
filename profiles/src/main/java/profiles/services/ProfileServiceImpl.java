package profiles.services;

import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import profile.ProfileObject;
import profile.ProfileRequest;
import profile.ProfileResponse;
import profile.ProfileServiceGrpc;
import profiles.model.Profile;

import javax.annotation.Nonnull;

import static profiles.verticles.ProfilesVerticle.EBA_PROFILE_FETCH;

public class ProfileServiceImpl extends ProfileServiceGrpc.ProfileServiceImplBase {

  // Variables

  private final Vertx mVertx;

  // Constructors

  public ProfileServiceImpl(@Nonnull Vertx vertx) {
    mVertx = vertx;
  }

  // Public

  @Override
  public void getProfile(ProfileRequest request, StreamObserver<ProfileResponse> responseObserver) {
    mVertx.eventBus().request(EBA_PROFILE_FETCH, new JsonObject(), ar -> {
      if (ar.failed()) {
        StatusException failure = failed(ar.cause());
        responseObserver.onError(failure);
        return;
      }

      Profile profile = (Profile) ar.result().body();
      ProfileResponse response = transformToProfileResponse(profile);

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    });
  }

  // Private

  private ProfileResponse transformToProfileResponse(Profile profile) {
    ProfileObject profileObject = ProfileObject.newBuilder()
      .setFirstName(profile.getFirstName())
      .setLastName(profile.getLastName())
      .setAge(profile.getAge())
      .build();

    return ProfileResponse.newBuilder()
      .setProfile(profileObject)
      .build();
  }

  private StatusException failed(Throwable cause) {
    Status status = Status.newBuilder()
      .setCode(Code.UNAVAILABLE_VALUE)
      .setMessage(cause.getMessage())
      .build();

    return StatusProto.toStatusException(status);
  }
}
