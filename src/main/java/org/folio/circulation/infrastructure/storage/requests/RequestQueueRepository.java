package org.folio.circulation.infrastructure.storage.requests;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.circulation.domain.RequestLevel.ITEM;
import static org.folio.circulation.domain.RequestLevel.TITLE;
import static org.folio.circulation.support.CqlSortBy.ascending;
import static org.folio.circulation.support.http.client.CqlQuery.exactMatch;
import static org.folio.circulation.support.http.client.CqlQuery.exactMatchAny;
import static org.folio.circulation.support.http.client.PageLimit.oneThousand;
import static org.folio.circulation.support.results.Result.succeeded;
import static org.folio.circulation.support.results.ResultBinding.mapResult;
import static org.folio.circulation.support.utils.LogUtil.collectionAsString;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.circulation.domain.Item;
import org.folio.circulation.domain.LoanAndRelatedRecords;
import org.folio.circulation.domain.MultipleRecords;
import org.folio.circulation.domain.Request;
import org.folio.circulation.domain.RequestAndRelatedRecords;
import org.folio.circulation.domain.RequestLevel;
import org.folio.circulation.domain.RequestQueue;
import org.folio.circulation.domain.RequestStatus;
import org.folio.circulation.domain.configuration.TlrSettingsConfiguration;
import org.folio.circulation.resources.context.RenewalContext;
import org.folio.circulation.support.http.client.CqlQuery;
import org.folio.circulation.support.http.client.PageLimit;
import org.folio.circulation.support.results.Result;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class RequestQueueRepository {
  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  private static final PageLimit MAXIMUM_SUPPORTED_REQUEST_QUEUE_SIZE = oneThousand();
  private final RequestRepository requestRepository;

  public CompletableFuture<Result<LoanAndRelatedRecords>> get(LoanAndRelatedRecords records) {
    log.debug("get:: parameters loanAndRelatedRecords: {}", records);
    Item item = records.getItem();
    return getQueue(records.getTlrSettings(), item.getInstanceId(), item.getItemId())
      .thenApply(mapResult(records::withRequestQueue));
  }

  public CompletableFuture<Result<LoanAndRelatedRecords>> getQueueForCheckOut(LoanAndRelatedRecords records) {
    log.debug("get:: parameters loanAndRelatedRecords: {}", records);
    Item item = records.getItem();
    return getQueueForCheckOut(records.getTlrSettings(), item.getInstanceId(), item.getItemId())
      .thenApply(mapResult(records::withRequestQueue));
  }

  public CompletableFuture<Result<RequestAndRelatedRecords>> get(RequestAndRelatedRecords records) {
    log.debug("get:: parameters requestAndRelatedRecords: {}", records);
    Request request = records.getRequest();

    return getQueue(request.getTlrSettingsConfiguration(), request.getInstanceId(), request.getItemId())
      .thenApply(mapResult(records::withRequestQueue));
  }

  public CompletableFuture<Result<RequestQueue>> getQueueForCheckOut(TlrSettingsConfiguration tlrSettings,
    String instanceId, String itemId) {

    return tlrSettings != null && tlrSettings.isTitleLevelRequestsFeatureEnabled()
      ? getByInstanceAndItemId(instanceId, itemId)
      : getByItemId(itemId);
  }

  public CompletableFuture<Result<RequestQueue>> getQueue(TlrSettingsConfiguration tlrSettings,
    String instanceId, String itemId) {

    return tlrSettings != null && tlrSettings.isTitleLevelRequestsFeatureEnabled()
      ? getByInstanceId(instanceId)
      : getByItemId(itemId);
  }

  public CompletableFuture<Result<RenewalContext>> get(RenewalContext context) {
    log.debug("get:: parameters renewalContext: {}", context);
    return getQueue(
      context.getTlrSettings(),
      context.getLoan().getItem().getInstanceId(),
      context.getLoan().getItemId()
    ).thenApply(result -> result.map(context::withRequestQueue));
  }

  public CompletableFuture<Result<RequestQueue>> getByInstanceAndItemId(String instanceId, String itemId) {
    return get("instanceId", instanceId, List.of(ITEM, TITLE))
      .thenCompose(r -> r.combineAfter(() -> get("itemId", itemId, List.of(ITEM, TITLE)), (byInstanceId, byItemId) -> {
        Collection<Request> result = byInstanceId.getRecords();
        Map<String, Request> idToRequestByInstance = byInstanceId.toMap(Request::getId);
        byItemId.getRecords()
          .stream()
          .filter(requestByItemId -> !idToRequestByInstance.containsKey(requestByItemId.getId()))
          .forEach(result::add);

        log.info("getByInstanceAndItemId:: requests: {}", result.stream().map(Request::getId).toList());
        return new RequestQueue(result);
      }));
  }

  public CompletableFuture<Result<RequestQueue>> getByInstanceId(String instanceId) {
    return get("instanceId", instanceId, List.of(ITEM, TITLE))
      .thenApply(r -> r.map(MultipleRecords::getRecords))
      .thenApply(r -> r.map(RequestQueue::new));
  }

  public CompletableFuture<Result<RequestQueue>> getByItemId(String itemId) {
    return get("itemId", itemId, List.of(ITEM))
      .thenApply(r -> r.map(MultipleRecords::getRecords))
      .thenApply(r -> r.map(RequestQueue::new));
  }

  private CompletableFuture<Result<MultipleRecords<Request>>> get(String idFieldName, String id,
    Collection<RequestLevel> requestLevels) {

    log.debug("get:: parameters idFieldName: {}, id: {}, requestLevels: {}",
      () -> idFieldName, () -> id, () -> collectionAsString(requestLevels));

    List<String> requestLevelStrings = requestLevels.stream()
      .map(RequestLevel::getValue)
      .collect(Collectors.toList());

    final Result<CqlQuery> itemIdQuery = exactMatch(idFieldName, id);
    final Result<CqlQuery> statusQuery = exactMatchAny("status", RequestStatus.openStates());
    final Result<CqlQuery> requestLevelQuery = exactMatchAny("requestLevel", requestLevelStrings);

    return itemIdQuery.combine(statusQuery, CqlQuery::and)
      .combine(requestLevelQuery, CqlQuery::and)
      .map(q -> q.sortBy(ascending("position")))
      .after(query -> requestRepository.findBy(query, MAXIMUM_SUPPORTED_REQUEST_QUEUE_SIZE));
  }

  public CompletableFuture<Result<RequestQueue>> getRequestQueueWithoutItemLookup(String itemId) {
    log.debug("getRequestQueueWithoutItemLookup:: parameters itemId: {}", itemId);
    final Result<CqlQuery> itemIdQuery = exactMatch("itemId", itemId);
    final Result<CqlQuery> statusQuery = exactMatchAny("status", RequestStatus.openStates());

    return itemIdQuery.combine(statusQuery, CqlQuery::and)
      .map(q -> q.sortBy(ascending("position")))
      .after(query -> requestRepository.findByWithoutItems(query,
          MAXIMUM_SUPPORTED_REQUEST_QUEUE_SIZE))
      .thenApply(r -> r.map(MultipleRecords::getRecords))
      .thenApply(r -> r.map(RequestQueue::new));
  }

  public CompletableFuture<Result<RequestQueue>> updateRequestsWithChangedPositions(
    RequestQueue requestQueue) {

    log.debug("updateRequestsWithChangedPositions:: parameters requestQueue: {}", requestQueue);

    Collection<Request> requestsWithChangedPosition = requestQueue
      .getRequestsWithChangedPosition();

    if (requestsWithChangedPosition.isEmpty()) {
      log.info("No requests with changed positions found");
      return completedFuture(succeeded(requestQueue));
    }

    return requestRepository.batchUpdate(requestsWithChangedPosition)
      .thenApply(r -> r.map(result -> requestQueue));
  }
}
