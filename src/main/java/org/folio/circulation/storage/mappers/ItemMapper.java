package org.folio.circulation.storage.mappers;

import static org.apache.commons.lang3.StringUtils.firstNonBlank;
import static org.folio.circulation.support.json.JsonPropertyFetcher.getNestedStringProperty;
import static org.folio.circulation.support.json.JsonPropertyFetcher.getProperty;
import static org.folio.circulation.support.json.JsonStringArrayPropertyFetcher.toStream;

import java.util.stream.Collectors;

import org.folio.circulation.domain.CallNumberComponents;
import org.folio.circulation.domain.Holdings;
import org.folio.circulation.domain.Instance;
import org.folio.circulation.domain.Item;
import org.folio.circulation.domain.ItemDescription;
import org.folio.circulation.domain.ItemStatus;
import org.folio.circulation.domain.ItemStatusName;
import org.folio.circulation.domain.LastCheckIn;
import org.folio.circulation.domain.LoanType;
import org.folio.circulation.domain.Location;
import org.folio.circulation.domain.MaterialType;
import org.folio.circulation.domain.ServicePoint;

import io.vertx.core.json.JsonObject;

public class ItemMapper {
  public Item toDomain(JsonObject representation) {
    return new Item(getProperty(representation, "id"),
      Location.unknown(getProperty(representation, "effectiveLocationId")),
      LastCheckIn.fromItemJson(representation),
      CallNumberComponents.fromItemJson(representation),
      Location.unknown(getProperty(representation, "permanentLocationId")),
      getInTransitServicePoint(representation), false,
      Holdings.unknown(getProperty(representation, "holdingsRecordId")),
      Instance.unknown(),
      MaterialType.unknown(getProperty(representation, "materialTypeId")),
      LoanType.unknown(getLoanTypeId(representation)), getDescription(representation),
      getItemStatus(representation));
  }

  private ItemDescription getDescription(JsonObject representation) {
    return new ItemDescription(
      getProperty(representation, "barcode"),
      getProperty(representation, "enumeration"),
      getProperty(representation, "copyNumber"),
      getProperty(representation, "volume"),
      getProperty(representation, "chronology"),
      getProperty(representation, "numberOfPieces"),
      getProperty(representation, "descriptionOfPieces"),
      toStream(representation, "yearCaption")
        .collect(Collectors.toList()));
  }

  private ItemStatus getItemStatus(JsonObject representation) {
    final String STATUS_PROPERTY = "status";

    return new ItemStatus(ItemStatusName
      .from(getNestedStringProperty(representation, STATUS_PROPERTY, "name")),
        getNestedStringProperty(representation, STATUS_PROPERTY, "date"));
  }

  private ServicePoint getInTransitServicePoint(JsonObject representation) {
    final var inTransitDestinationServicePointId
      = getProperty(representation, "inTransitDestinationServicePointId");

    if (inTransitDestinationServicePointId == null) {
      return null;
    }
    else {
      return ServicePoint.unknown(inTransitDestinationServicePointId);
    }
  }

  public String getLoanTypeId(JsonObject representation) {
    return firstNonBlank(
      getProperty(representation, "temporaryLoanTypeId"),
      getProperty(representation, "permanentLoanTypeId"));
  }
}
