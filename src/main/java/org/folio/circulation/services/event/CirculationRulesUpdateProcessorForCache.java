package org.folio.circulation.services.event;

import static io.vertx.core.Future.succeededFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public class CirculationRulesUpdateProcessorForCache {
  private static final Logger log = LogManager.getLogger(CirculationRulesUpdateProcessorForCache.class);

  public Future<String> run(String eventKey, JsonObject payload) {
    log.info("run:: received event {}", eventKey);

    return processEvent(payload)
      .onSuccess(r -> log.info("handle:: event {} processed successfully", eventKey))
      .onFailure(t -> log.error("handle:: failed to process event", t))
      .map(eventKey);
  }

  private Future<Void> processEvent(JsonObject payload) {
    return succeededFuture();
  }
}
