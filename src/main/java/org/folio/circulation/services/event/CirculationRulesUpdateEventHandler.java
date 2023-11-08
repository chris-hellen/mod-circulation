package org.folio.circulation.services.event;

import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.folio.kafka.AsyncRecordHandler;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public class CirculationRulesUpdateEventHandler implements AsyncRecordHandler<String, String> {
  private final Context context;

  public CirculationRulesUpdateEventHandler(Context context) {
    this.context = context;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> kafkaConsumerRecord) {
    JsonObject payload = new JsonObject(kafkaConsumerRecord.value());
    CaseInsensitiveMap<String, String> headers =
      new CaseInsensitiveMap<>(kafkaHeadersToMap(kafkaConsumerRecord.headers()));

    CirculationRulesUpdateProcessorForCache circulationRulesUpdateProcessorForCache =
      new CirculationRulesUpdateProcessorForCache();

    return circulationRulesUpdateProcessorForCache.run(kafkaConsumerRecord.key(), payload);
  }
}
