package org.folio.circulation.infrastructure.storage;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.circulation.domain.CheckOutLock;
import org.folio.circulation.domain.LoanAndRelatedRecords;
import org.folio.circulation.support.Clients;
import org.folio.circulation.support.CollectionResourceClient;
import org.folio.circulation.support.http.client.Response;
import org.folio.circulation.support.http.client.ResponseInterpreter;
import org.folio.circulation.support.results.Result;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import static org.folio.circulation.support.http.ResponseMapping.forwardOnFailure;
import static org.folio.circulation.support.http.ResponseMapping.mapUsingJson;

public class CheckOutLockRepository {
  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());
  private final CollectionResourceClient checkOutLockClient;
  private final Vertx vertx;

  public CheckOutLockRepository(Clients clients, RoutingContext routingContext) {
    this.checkOutLockClient = clients.checkOutLockClient();
    this.vertx = routingContext.vertx();
  }

  public void createLockWithRetry(int noOfAttempts, CompletableFuture<CheckOutLock> future, LoanAndRelatedRecords records) {
    int maxRetryAttempts = records.getCheckoutLockConfiguration().getNoOfRetryAttempts();
    int retryInterval = records.getCheckoutLockConfiguration().getRetryInterval();
    log.debug("createLockWithRetry:: Retrying lock creation {} ", noOfAttempts);
    try {
      create(records)
        .whenComplete((res, err) -> {
          if (res.succeeded()) {
            log.info("createLockWithRetry:: checkOutLock object {} ", res.value());
            future.complete(res.value());
          } else {
            if (noOfAttempts <= maxRetryAttempts) {
              log.info("createLockWithRetry:: Retry attempt {} for lock creation with delay {}", noOfAttempts, retryInterval);
              vertx.setTimer(retryInterval, h -> createLockWithRetry(noOfAttempts + 1, future, records));
            } else {
              String error = res.cause() != null ? res.cause().toString() : "";
              log.warn("createLockWithRetry:: Completing exceptionally {} ", error);
              future.completeExceptionally(new RuntimeException(error));
            }
          }
        });
    } catch (Exception ex) {
      log.warn("createLockWithRetry:: exception  ", ex);
      future.completeExceptionally(ex);
    }
  }


  public CompletableFuture<Result<CheckOutLock>> create(LoanAndRelatedRecords records) {
    log.debug("create:: trying to create lock for userId {} ", records.getUserId());
    final ResponseInterpreter<CheckOutLock> interpreter =
      new ResponseInterpreter<CheckOutLock>()
        .flatMapOn(201, mapUsingJson(CheckOutLock::from))
        .otherwise(forwardOnFailure());

    return checkOutLockClient.post(buildCheckOutLockPayload(records))
      .thenApply(interpreter::flatMap);

  }

  public CompletableFuture<Result<Response>> deleteCheckoutLockById(String checkOutLockId) {
    log.info("deleteCheckoutLockById:: deleting the lock for userId {} ", checkOutLockId);
    return checkOutLockClient.delete(checkOutLockId);
  }

  private JsonObject buildCheckOutLockPayload(LoanAndRelatedRecords records) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.put("userId", records.getUserId());
    jsonObject.put("ttlMs", records.getCheckoutLockConfiguration().getLockTtl());
    return jsonObject;
  }

}
