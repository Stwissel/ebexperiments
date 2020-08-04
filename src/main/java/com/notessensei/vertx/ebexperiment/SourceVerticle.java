package com.notessensei.vertx.ebexperiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class SourceVerticle extends AbstractVerticle {

  final JsonArray data = new JsonArray();
  final AtomicInteger counter = new AtomicInteger();

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {
    this.generateSampleData();
    this.getVertx().eventBus().consumer("test1", this::serveTest1);
    this.getVertx().eventBus().consumer("test2", this::serveTest2);
    this.getVertx().eventBus().consumer("test3", this::serveTest3);
    this.getVertx().eventBus().consumer("test4", this::serveTest4);
    this.getVertx().eventBus().consumer("test5", this::serveTest5);
    startPromise.complete();
  }


  private void generateSampleData() {
    int itemCount = 10000;
    ArrayList<String> colors = new ArrayList<>();
    colors.addAll(Arrays.asList("Red", "Blue", "Green", "Purple", "Black", "White", "Yellow"));

    ArrayList<String> shapes = new ArrayList<>();
    shapes.addAll(Arrays.asList("Circle", "Square", "Triangle", "Cube", "Sphere", "Dodekaeter"));

    ArrayList<String> tastes = new ArrayList<>();
    tastes.addAll(Arrays.asList("Sweet", "Sour", "Bitter", "Salty"));

    Random random = new Random();

    for (int i = 0; i < itemCount; i++) {
      data.add(new JsonObject()
          .put("sequence", i)
          .put("color", colors.get(random.nextInt(colors.size())))
          .put("shape", shapes.get(random.nextInt(shapes.size())))
          .put("taste", tastes.get(random.nextInt(tastes.size()))));
    }

  }

  private void serveTest1(final Message<String> msg) {
    final DeliveryOptions options = new DeliveryOptions().addHeader("Status", "Done");
    msg.reply(this.data, options);
  }

  private void serveTest2(final Message<String> msg) {

    // When all is done
    if (this.counter.get() >= this.data.size()) {
      final DeliveryOptions options = new DeliveryOptions().addHeader("Status", "Done");
      msg.reply(null, options);
      this.counter.set(0);
      return;
    }

    // Individual messages
    final DeliveryOptions options = new DeliveryOptions().addHeader("Status", "Data");
    msg.reply(this.data.getValue(this.counter.getAndIncrement()), options);

  }

  // Just trigger a full send, one by one
  private void serveTest3(final Message<String> msg) {
    final EventBus eb = this.getVertx().eventBus();
    // No forEach since sequence wouldn't be guaranteed
    for (int i = 0; i < this.data.size(); i++) {
      final DeliveryOptions options = new DeliveryOptions();
      options.addHeader("Status", "Data");
      eb.send("test3back", this.data.getValue(i), options);
    }
    DeliveryOptions options = new DeliveryOptions();
    options.addHeader("Status", "Done");
    eb.send("test3back", null, options);
  }


  // Same show as Test3, but inside execute blocking
  private void serveTest4(final Message<String> msg) {
    final EventBus eb = this.getVertx().eventBus();

    this.getVertx().executeBlocking(promise -> {
      for (int i = 0; i < this.data.size(); i++) {
        DeliveryOptions options = new DeliveryOptions();
        options.addHeader("Status", "Data");
        eb.send("test4back", this.data.getValue(i), options);
      }
      promise.complete();
    }, result -> {
      DeliveryOptions options = new DeliveryOptions();
      options.addHeader("Status", "Done");
      eb.send("test4back", null, options);
    });

  }

  private void serveTest5(final Message<String> msg) {
    final EventBus eb = this.getVertx().eventBus();
    this.sendOneAndWait(eb, 0, "test5back");
  }


  /**
   * @param eb
   * @param count
   * @param address
   */
  private void sendOneAndWait(final EventBus eb, final int count, final String address) {
    if (count < this.data.size()) {
      final int countNext = count + 1;
      DeliveryOptions options = new DeliveryOptions();
      options.addHeader("Status", "Data");
      eb.request(address, this.data.getValue(count), options)
          .onSuccess(msg -> this.sendOneAndWait(eb, countNext, address))
          .onFailure(err -> err.printStackTrace());

    } else {
      DeliveryOptions options = new DeliveryOptions();
      options.addHeader("Status", "Done");
      eb.send(address, null, options);
    }

  }

}
