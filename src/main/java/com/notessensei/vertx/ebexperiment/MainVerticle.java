package com.notessensei.vertx.ebexperiment;

import java.util.Date;
import java.util.function.Consumer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class MainVerticle extends AbstractVerticle {

  public static void main(String[] args) {
    final VertxOptions options = new VertxOptions();
    options.setBlockedThreadCheckInterval(1000 * 60 * 60);
    Vertx vertx = Vertx.vertx(options);
    final Consumer<Vertx> runner = vx -> {
      vx.deployVerticle(new MainVerticle());
    };
    runner.accept(vertx);

  }

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {

    Router router = Router.router(getVertx());
    router.route("/").handler(this::helloWorld);
    router.route("/test1").handler(this::test1);
    router.route("/test2").handler(this::test2);

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
          JsonArray data = (JsonArray) msg.body();
          final Date end = new Date();
          ctx.response().putHeader("content-type", "application/json")
              .end(data.encode());

          System.out.format("Runtime is %sms\n", end.getTime() - start.getTime());
        })
        .onFailure(err -> err.printStackTrace());
  }

  private void test2(final RoutingContext ctx) {
    final Date start = new Date();
    Future<Message<Object>> firstShot = this.getVertx().eventBus().request("test2", "Test2");
    this.messageDance(ctx, start, Buffer.buffer().appendString("["), firstShot);
  }

  private void messageDance(RoutingContext ctx, Date start, Buffer result,
      Future<Message<Object>> messageFuture) {
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
            System.out.format("Runtime is %sms\n", end.getTime() - start.getTime());
          } else {
            result.appendBuffer(((JsonObject) msg.body()).toBuffer());
            Future<Message<Object>> nextShot = this.getVertx().eventBus().request("test2", "Test2");
            this.messageDance(ctx, start, result, nextShot);
          }
        });
  }
}
