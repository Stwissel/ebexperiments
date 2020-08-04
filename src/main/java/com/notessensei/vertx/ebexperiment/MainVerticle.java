package com.notessensei.vertx.ebexperiment;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class MainVerticle extends AbstractVerticle {

  public static void main(final String[] args) {
    final VertxOptions options = new VertxOptions();
    options.setBlockedThreadCheckInterval(1000 * 60 * 60);
    final Vertx vertx = Vertx.vertx(options);
    final Consumer<Vertx> runner = vx -> {
      vx.deployVerticle(new MainVerticle());
    };
    runner.accept(vertx);

  }

  private Buffer buffer;
  private RoutingContext globalCtx;
  private Date startDate;
  private AtomicBoolean isNext;
  private MessageConsumer<JsonObject> currentConsumer;

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {

    final Router router = Router.router(this.getVertx());
    router.route("/").handler(this::helloWorld);
    router.route("/test1").handler(this::test1);
    router.route("/test2").handler(this::test2);
    router.route("/test3").handler(this::test3);
    router.route("/test4").handler(this::test4);
    router.route("/test5").handler(this::test5);

    final SourceVerticle sv = new SourceVerticle();
    final DeploymentOptions options = new DeploymentOptions();
    options.setWorker(true);
    this.getVertx().deployVerticle(sv, options)
        .compose(v -> this.vertx.createHttpServer().requestHandler(router).listen(8888))
        .onSuccess(v -> {
          startPromise.complete();
          System.out.println("HTTP server started on port 8888");
        })
        .onFailure(err -> startPromise.fail(err));
  }


  private void helloWorld(final RoutingContext ctx) {
    ctx.response().putHeader("content-type", "text/plain").end("Hello World");
  }

  private void test1(final RoutingContext ctx) {
    final Date start = new Date();
    this.getVertx().eventBus().request("test1", "Test1")
        .onSuccess(msg -> {
          if ("Done".equals(msg.headers().get("Status"))) {
            System.out.print("Test 1: ");
          }
          final JsonArray data = (JsonArray) msg.body();
          final Date end = new Date();
          ctx.response().putHeader("content-type", "application/json")
              .end(data.encode());

          System.out.format("Runtime is %sms\n", end.getTime() - start.getTime());
        })
        .onFailure(Throwable::printStackTrace);
  }

  private void test2(final RoutingContext ctx) {
    final Date start = new Date();
    this.isNext = new AtomicBoolean(false);
    final Future<Message<Object>> firstShot = this.getVertx().eventBus().request("test2", "Test2");
    this.test2MessageDance(ctx, start, Buffer.buffer().appendString("["), firstShot);
  }

  private void test2MessageDance(final RoutingContext ctx, final Date start, final Buffer result,
      final Future<Message<Object>> messageFuture) {
    messageFuture
        .onFailure(err -> {
          err.printStackTrace();
          ctx.response().setStatusCode(500).end(err.getMessage());
        })
        .onSuccess(msg -> {
          if ("Done".equals(msg.headers().get("Status"))) {
            System.out.print("Test 2: ");
            result.appendString("]");
            final Date end = new Date();
            ctx.response().putHeader("content-type", "application/json").end(result);
            System.out.format("Test 2 Runtime is %sms\n", end.getTime() - start.getTime());
          } else {
            if (this.isNext.getAndSet(true)) {
              result.appendString(",\n");
            }
            result.appendBuffer(((JsonObject) msg.body()).toBuffer());
            final Future<Message<Object>> nextShot =
                this.getVertx().eventBus().request("test2", "Test2");
            this.test2MessageDance(ctx, start, result, nextShot);
          }
        });
  }

  private void test3(final RoutingContext ctx) {
    this.buffer = Buffer.buffer();
    this.startDate = new Date();
    this.globalCtx = ctx;
    this.buffer.appendString("[");
    this.isNext = new AtomicBoolean(false);
    this.currentConsumer = this.getVertx().eventBus().consumer("test3back", this::test3incoming);
    this.getVertx().eventBus().send("test3", "GO");
  }

  private void test3incoming(final Message<JsonObject> msg) {
    final String status = msg.headers().get("Status");
    if ("Done".equals(status)) {
      final Date end = new Date();
      this.buffer.appendString("]");
      this.globalCtx.response().putHeader("content-type", "application/json").end(this.buffer);
      System.out.format("Test 3 Runtime is %sms\n", end.getTime() - this.startDate.getTime());
      this.currentConsumer.unregister();
      return;
    }

    final JsonObject payload = msg.body();
    if (this.isNext.getAndSet(true)) {
      this.buffer.appendString(",\n");
    }
    this.buffer.appendBuffer(payload.toBuffer());
    msg.reply("OK");
  }

  private void test4(final RoutingContext ctx) {
    this.buffer = Buffer.buffer();
    this.startDate = new Date();
    this.globalCtx = ctx;
    this.buffer.appendString("[");
    this.isNext = new AtomicBoolean(false);
    this.currentConsumer = this.getVertx().eventBus().consumer("test4back", this::test4incoming);
    this.getVertx().eventBus().send("test4", "GO");
  }

  private void test4incoming(final Message<JsonObject> msg) {
    final String status = msg.headers().get("Status");
    if ("Done".equals(status)) {
      final Date end = new Date();
      this.buffer.appendString("]");
      this.globalCtx.response().putHeader("content-type", "application/json").end(this.buffer);
      System.out.format("Test 4 Runtime is %sms\n", end.getTime() - this.startDate.getTime());
      this.currentConsumer.unregister();
      return;
    }

    final JsonObject payload = msg.body();
    if (this.isNext.getAndSet(true)) {
      this.buffer.appendString(",\n");
    }
    this.buffer.appendBuffer(payload.toBuffer());
    msg.reply("OK");
  }

  private void test5(final RoutingContext ctx) {
    this.buffer = Buffer.buffer();
    this.startDate = new Date();
    this.globalCtx = ctx;
    this.buffer.appendString("[");
    this.isNext = new AtomicBoolean(false);
    this.currentConsumer = this.getVertx().eventBus().consumer("test5back", this::test5incoming);
    this.getVertx().eventBus().send("test5", "GO");
  }

  private void test5incoming(final Message<JsonObject> msg) {
    final String status = msg.headers().get("Status");
    if ("Done".equals(status)) {
      final Date end = new Date();
      this.buffer.appendString("]");
      this.globalCtx.response().putHeader("content-type", "application/json").end(this.buffer);
      System.out.format("Test 5 Runtime is %sms\n", end.getTime() - this.startDate.getTime());
      this.currentConsumer.unregister();
      return;
    }

    final JsonObject payload = msg.body();
    if (this.isNext.getAndSet(true)) {
      this.buffer.appendString(",\n");
    }
    this.buffer.appendBuffer(payload.toBuffer());
    msg.reply("OK");
  }
}
