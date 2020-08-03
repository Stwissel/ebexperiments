package com.notessensei.vertx.ebexperiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
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
    startPromise.complete();
  }


  private void generateSampleData() {
    int itemCount = 10000;
    ArrayList<String> colors = new ArrayList<>();
    colors.addAll(Arrays.asList("Red", "Blue", "Green", "Purple"));

    ArrayList<String> shapes = new ArrayList<>();
    shapes.addAll(Arrays.asList("Circle", "Square", "Triangle", "Cube"));

    ArrayList<String> tastes = new ArrayList<>();
    tastes.addAll(Arrays.asList("Sweet", "Sour", "Bitter", "Salty"));

    Random random = new Random();

    for (int i = 0; i < itemCount; i++) {
      data.add(new JsonObject()
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



}
