package org.folio.circulation.services.event;

import static org.folio.circulation.services.event.CirculationStorageKafkaTopic.CIRCULATION_RULES;

import org.folio.kafka.services.KafkaTopic;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum CirculationEventType {
  CIRCULATION_RULES_UPDATED(CIRCULATION_RULES, PayloadType.UPDATE);

  private final KafkaTopic kafkaTopic;
  private final PayloadType payloadType;

  public enum PayloadType {
    UPDATE, DELETE, CREATE, DELETE_ALL
  }
}
