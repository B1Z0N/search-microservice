# vertx-starter-pack
Starter pack for Vert.x development. Read a [wiki](https://github.com/IASA-HUB/vertx-starter-pack/wiki) for preparation.

# Files

There are some important for understanding of this project files. To main directories is `common` and `profiles`.

## common

Look at `build.gradle`, it is the first project configuration file. You should see project dependencies, 
their's meanings one by one:

- Service discovery - component, that provides an infrastructure to publish and discover various services
- Hazlecast - default custer manager
- Web - http server
- Web client - asynchronous http client
- Circuit breaker - implementation of [`Circuit breaker pattern`](https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern)
- Health check - testing the health of the application

*Check [Useful `vert.x` concepts](#useful-vertx-concepts) section for more.

Now turn your attention to `src/main/java/vertx/common` folder. There are two files.
Start reading `Launcher.java` first and `MicroserviceVerticle.java` second.
**This whole part is just two classes intended for future reuse by inheriting from them**.
That is why it is `common` - because all this code is common for our project(and lots of other `vert.x` projects, perhaps)

# Resources

This part is all about useful links to understand `vert.x` and this project of course(be patient):

## Beginner

- Unofficial [tutorial](http://tutorials.jenkov.com/vert.x/index.html)
- Official [tutorial](https://vertx.io/blog/posts/introduction-to-vertx.html)
- [Guide](https://vertx.io/docs/guide-for-java-devs/) to asynchronous programming, actually all about `vert.x`(read the first three items)

## Useful `vert.x` concepts

- [ServiceDiscovery](https://vertx.io/docs/vertx-service-discovery/java/)(read the first eight items)
- [Hazelcast](https://vertx.io/docs/vertx-hazelcast/java/)
- [Web](https://vertx.io/docs/vertx-web/java/)
- [Web client](https://vertx.io/docs/vertx-web-client/java/)
- [Circuit breaker](https://vertx.io/docs/vertx-circuit-breaker/java/)
- [Health check](https://vertx.io/docs/vertx-health-check/java/)

## Other

- What is REST, [part 1](https://medium.com/extend/what-is-rest-a-simple-explanation-for-beginners-part-1-introduction-b4a072f8740f) and [part 2](https://medium.com/extend/what-is-rest-a-simple-explanation-for-beginners-part-2-rest-constraints-129a4b69a582)

---

**Google a lot and read a lot, other info is comments in files.**
