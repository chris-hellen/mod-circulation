package org.folio.circulation.services.event;

import static org.folio.circulation.services.event.CirculationEventType.CIRCULATION_RULES_UPDATED;
import static org.folio.circulation.services.event.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.circulation.services.event.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.circulation.services.event.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.circulation.services.event.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.circulation.services.event.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.circulation.services.event.KafkaConfigConstants.OKAPI_URL;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.kafka.services.KafkaTopic;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class KafkaConsumerService {
  private static final Logger log = LogManager.getLogger(KafkaConsumerService.class);
  private static final int DEFAULT_LOAD_LIMIT = 5;
  private static final String TENANT_ID_PATTERN = "\\w+";
  private static final String MODULE_ID = "mod-circulation";
  private Vertx vertx;
  private Context context;
  private final List<KafkaConsumerWrapper<String, String>> consumers = new ArrayList<>();

  public Future<Void> createConsumersPerInstance() {
      final KafkaConfig config = getKafkaConfig();

      return createCirculationEventConsumer(CIRCULATION_RULES_UPDATED, config,
        new CirculationRulesUpdateEventHandler(context))
        .mapEmpty();
  }

  private Future<KafkaConsumerWrapper<String, String>> createCirculationEventConsumer(
    CirculationEventType eventType, KafkaConfig kafkaConfig,
    AsyncRecordHandler<String, String> handler) {

    SubscriptionDefinition subscriptionDefinition = SubscriptionDefinition.builder()
      .eventType(eventType.name())
      .subscriptionPattern(buildSubscriptionPattern(eventType.getKafkaTopic(), kafkaConfig))
      .build();

    return createConsumer(kafkaConfig, subscriptionDefinition, handler);
  }

  private Future<KafkaConsumerWrapper<String, String>> createConsumer(KafkaConfig kafkaConfig,
    SubscriptionDefinition subscriptionDefinition, AsyncRecordHandler<String, String> recordHandler) {

    var consumer = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(DEFAULT_LOAD_LIMIT)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    return consumer.start(recordHandler, MODULE_ID + "_" + UUID.randomUUID().toString())
      .onSuccess(v -> consumers.add(consumer))
      .map(consumer);
  }

  private KafkaConfig getKafkaConfig() {
    log.info("getKafkaConfig:: getting Kafka config");

    JsonObject vertxConfig = vertx.getOrCreateContext().config();

    KafkaConfig config = KafkaConfig.builder()
      .envId(vertxConfig.getString(KAFKA_ENV))
      .kafkaHost(vertxConfig.getString(KAFKA_HOST))
      .kafkaPort(vertxConfig.getString(KAFKA_PORT))
      .okapiUrl(vertxConfig.getString(OKAPI_URL))
      .replicationFactor(Integer.parseInt(vertxConfig.getString(KAFKA_REPLICATION_FACTOR)))
      .maxRequestSize(Integer.parseInt(vertxConfig.getString(KAFKA_MAX_REQUEST_SIZE)))
      .build();

    log.info("getKafkaConfig:: {}", config);

    return config;
  }

  private static String buildSubscriptionPattern(KafkaTopic kafkaTopic, KafkaConfig kafkaConfig) {
    return kafkaTopic.fullTopicName(kafkaConfig, TENANT_ID_PATTERN);
  }
}
