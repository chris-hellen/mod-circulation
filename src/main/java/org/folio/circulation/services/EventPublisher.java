package org.folio.circulation.services;

import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.folio.circulation.domain.EventType.ITEM_AGED_TO_LOST;
import static org.folio.circulation.domain.EventType.ITEM_CHECKED_IN;
import static org.folio.circulation.domain.EventType.ITEM_CHECKED_OUT;
import static org.folio.circulation.domain.EventType.ITEM_CLAIMED_RETURNED;
import static org.folio.circulation.domain.EventType.ITEM_DECLARED_LOST;
import static org.folio.circulation.domain.EventType.LOAN_CLOSED;
import static org.folio.circulation.domain.EventType.LOAN_DUE_DATE_CHANGED;
import static org.folio.circulation.domain.EventType.LOG_RECORD;
import static org.folio.circulation.domain.LoanAction.CHECKED_IN;
import static org.folio.circulation.domain.LoanAction.DUE_DATE_CHANGED;
import static org.folio.circulation.domain.LoanAction.RECALLREQUESTED;
import static org.folio.circulation.domain.representations.logs.CirculationCheckInCheckOutLogEventMapper.mapToCheckInLogEventContent;
import static org.folio.circulation.domain.representations.logs.CirculationCheckInCheckOutLogEventMapper.mapToCheckOutLogEventContent;
import static org.folio.circulation.domain.representations.logs.LogEventPayloadField.LOG_EVENT_TYPE;
import static org.folio.circulation.domain.representations.logs.LogEventPayloadField.PAYLOAD;
import static org.folio.circulation.domain.representations.logs.LogEventType.LOAN;
import static org.folio.circulation.domain.representations.logs.LogEventType.NOTICE;
import static org.folio.circulation.domain.representations.logs.LogEventType.NOTICE_ERROR;
import static org.folio.circulation.domain.representations.logs.RequestUpdateLogEventMapper.mapToRequestLogEventJson;
import static org.folio.circulation.support.AsyncCoordinationUtil.allOf;
import static org.folio.circulation.support.json.JsonPropertyWriter.write;
import static org.folio.circulation.support.results.CommonFailures.failedDueToServerError;
import static org.folio.circulation.support.results.Result.failed;
import static org.folio.circulation.support.results.Result.ofAsync;
import static org.folio.circulation.support.results.Result.succeeded;
import static org.folio.circulation.support.utils.ClockUtil.getZonedDateTime;
import static org.folio.circulation.support.utils.DateFormatUtil.formatDateTimeOptional;

import java.util.concurrent.CompletableFuture;

import javax.ws.rs.core.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.circulation.domain.CheckInContext;
import org.folio.circulation.domain.EventType;
import org.folio.circulation.domain.Loan;
import org.folio.circulation.domain.LoanAndRelatedRecords;
import org.folio.circulation.domain.Request;
import org.folio.circulation.domain.RequestAndRelatedRecords;
import org.folio.circulation.domain.User;
import org.folio.circulation.domain.anonymization.LoanAnonymizationRecords;
import org.folio.circulation.domain.policy.LoanPolicy;
import org.folio.circulation.domain.policy.Period;
import org.folio.circulation.domain.representations.logs.LoanLogContext;
import org.folio.circulation.domain.representations.logs.LogContextActionResolver;
import org.folio.circulation.domain.representations.logs.LogEventType;
import org.folio.circulation.domain.representations.logs.NoticeLogContext;
import org.folio.circulation.infrastructure.storage.loans.LoanRepository;
import org.folio.circulation.infrastructure.storage.users.UserRepository;
import org.folio.circulation.resources.context.RenewalContext;
import org.folio.circulation.support.Clients;
import org.folio.circulation.support.HttpFailure;
import org.folio.circulation.support.ServerErrorFailure;
import org.folio.circulation.support.ValidationErrorFailure;
import org.folio.circulation.support.http.server.ValidationError;
import org.folio.circulation.support.results.CommonFailures;
import org.folio.circulation.support.results.Result;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class EventPublisher {
  private static final Logger logger = LogManager.getLogger(EventPublisher.class);

  public static final String USER_ID_FIELD = "userId";
  public static final String LOAN_ID_FIELD = "loanId";
  public static final String DUE_DATE_FIELD = "dueDate";
  public static final String RETURN_DATE_FIELD = "returnDate";
  public static final String GRACE_PERIOD_FIELD = "gracePeriod";
  public static final String DUE_DATE_CHANGED_BY_RECALL_FIELD = "dueDateChangedByRecall";
  public static final String FAILED_TO_PUBLISH_LOG_TEMPLATE =
    "Failed to publish {} event: loan is null";

  private final PubSubPublishingService pubSubPublishingService;

  public EventPublisher(RoutingContext routingContext) {
    pubSubPublishingService = new PubSubPublishingService(routingContext);
  }

  public EventPublisher(PubSubPublishingService pubSubPublishingService) {
    this.pubSubPublishingService = pubSubPublishingService;
  }

  public CompletableFuture<Result<LoanAndRelatedRecords>> publishItemCheckedOutEvent(
    LoanAndRelatedRecords loanAndRelatedRecords, UserRepository userRepository) {

    if (loanAndRelatedRecords.getLoan() != null) {
      Loan loan = loanAndRelatedRecords.getLoan();

      JsonObject payloadJsonObject = new JsonObject();
      write(payloadJsonObject, USER_ID_FIELD, loan.getUserId());
      write(payloadJsonObject, LOAN_ID_FIELD, loan.getId());
      write(payloadJsonObject, DUE_DATE_FIELD, loan.getDueDate());

      ofNullable(loan.getLoanPolicy())
        .map(LoanPolicy::getGracePeriod)
        .filter(Period::isValid)
        .map(Period::asJson)
        .ifPresent(json -> write(payloadJsonObject, GRACE_PERIOD_FIELD, json));

      runAsync(() -> userRepository.getUser(loanAndRelatedRecords.getLoggedInUserId())
        .thenApplyAsync(r -> r.after(loggedInUser -> CompletableFuture.completedFuture(
          Result.succeeded(pubSubPublishingService.publishEvent(LOG_RECORD.name(), mapToCheckOutLogEventContent(loanAndRelatedRecords, loggedInUser)))))));

      return pubSubPublishingService.publishEvent(ITEM_CHECKED_OUT.name(), payloadJsonObject.encode())
        .handle((result, error) -> {
          if (error != null) {
            return failedDueToServerError(error.getMessage());
          }
          return succeeded(loanAndRelatedRecords);
        });
    }
    else {
      logger.error(FAILED_TO_PUBLISH_LOG_TEMPLATE, ITEM_CHECKED_OUT.name());
    }

    return completedFuture(succeeded(loanAndRelatedRecords));
  }

  public CompletableFuture<Result<CheckInContext>> publishItemCheckedInEvents(
    CheckInContext checkInContext, UserRepository userRepository) {

    runAsync(() -> userRepository.getUser(checkInContext.getLoggedInUserId())
      .thenApplyAsync(r -> r.after(loggedInUser -> CompletableFuture.completedFuture(
        Result.succeeded(pubSubPublishingService.publishEvent(LOG_RECORD.name(), mapToCheckInLogEventContent(checkInContext, loggedInUser)))))));

    if (checkInContext.getLoan() != null) {
      Loan loan = checkInContext.getLoan();

      JsonObject payloadJsonObject = new JsonObject();
      write(payloadJsonObject, USER_ID_FIELD, loan.getUserId());
      write(payloadJsonObject, LOAN_ID_FIELD, loan.getId());
      write(payloadJsonObject, RETURN_DATE_FIELD, loan.getReturnDate());

      return pubSubPublishingService.publishEvent(ITEM_CHECKED_IN.name(), payloadJsonObject.encode())
        .handle((result, error) -> {
          if (error != null) {
            return failedDueToServerError(error.getMessage());
          }
          return succeeded(checkInContext);
        });
    }
    else {
      logger.error(FAILED_TO_PUBLISH_LOG_TEMPLATE, ITEM_CHECKED_IN.name());
    }

    return completedFuture(succeeded(checkInContext));
  }

  public CompletableFuture<Result<Loan>> publishDeclaredLostEvent(Loan loan) {
    return publishStatusChangeEvent(ITEM_DECLARED_LOST, loan);
  }

  public CompletableFuture<Result<Loan>> publishItemClaimedReturnedEvent(Loan loan) {
    return publishStatusChangeEvent(ITEM_CLAIMED_RETURNED, loan);
  }

  private CompletableFuture<Result<Loan>> publishStatusChangeEvent(EventType eventType, Loan loan) {
    final String eventName = eventType.name();

    if (loan == null) {
      logger.error(FAILED_TO_PUBLISH_LOG_TEMPLATE, eventName);
      return completedFuture(succeeded(null));
    }

    LoanLogContext loanLogContext = LoanLogContext.from(loan)
      .withDescription(String.format("Additional information: %s", loan.getActionComment()));
    runAsync(() -> publishLogRecord(loanLogContext.asJson(), LOAN));

    JsonObject payloadJson = new JsonObject();
    write(payloadJson, USER_ID_FIELD, loan.getUserId());
    write(payloadJson, LOAN_ID_FIELD, loan.getId());

    return pubSubPublishingService.publishEvent(eventName, payloadJson.encode())
      .handle((result, error) -> {
        if (error != null) {
          return failedDueToServerError(error.getMessage());
        }
        return succeeded(loan);
      });
  }

  public CompletableFuture<Result<Loan>> publishLoanClosedEvent(Loan loan) {
    String eventName = LOAN_CLOSED.name();

    if (loan == null) {
      logger.error(FAILED_TO_PUBLISH_LOG_TEMPLATE, eventName);
      return ofAsync(() -> null);
    }

    JsonObject payload = new JsonObject();
    write(payload, USER_ID_FIELD, loan.getUserId());
    write(payload, LOAN_ID_FIELD, loan.getId());

    return pubSubPublishingService.publishEvent(eventName, payload.encode())
      .handle((result, error) -> {
        if (error != null) {
          return failedDueToServerError(error.getMessage());
        }
        return succeeded(loan);
      });
  }

  private CompletableFuture<Result<Loan>> publishDueDateChangedEvent(Loan loan, RequestAndRelatedRecords records) {
    return publishDueDateChangedEvent(loan, records.getRequest().getRequester(), false);
  }

  private CompletableFuture<Result<Loan>> publishDueDateChangedEvent(Loan loan, User user, boolean renewalContext) {
    if (loan != null) {
      JsonObject payloadJsonObject = new JsonObject();
      write(payloadJsonObject, USER_ID_FIELD, loan.getUserId());
      write(payloadJsonObject, LOAN_ID_FIELD, loan.getId());
      write(payloadJsonObject, DUE_DATE_FIELD, loan.getDueDate());
      write(payloadJsonObject, DUE_DATE_CHANGED_BY_RECALL_FIELD, loan.wasDueDateChangedByRecall());

      runAsync(() -> publishDueDateLogEvent(loan));
      if (renewalContext) {
        runAsync(() -> publishRenewedEvent(loan.copy().withUser(user)));
      }

      return pubSubPublishingService.publishEvent(LOAN_DUE_DATE_CHANGED.name(), payloadJsonObject.encode())
        .handle((result, error) -> {
          if (error != null) {
            return failedDueToServerError(error.getMessage());
          }
          return succeeded(loan);
        });
    }
    else {
      logger.error(FAILED_TO_PUBLISH_LOG_TEMPLATE, LOAN_DUE_DATE_CHANGED.name());
    }

    return completedFuture(succeeded(null));
  }

  public CompletableFuture<Result<LoanAndRelatedRecords>> publishDueDateChangedEvent(
    LoanAndRelatedRecords loanAndRelatedRecords) {

    if (loanAndRelatedRecords.getLoan() != null) {
      Loan loan = loanAndRelatedRecords.getLoan();
      publishDueDateChangedEvent(loan, loan.getUser(), false);
    }

    return completedFuture(succeeded(loanAndRelatedRecords));
  }

  public CompletableFuture<Result<RenewalContext>> publishDueDateChangedEvent(
    RenewalContext renewalContext) {

    var loan = renewalContext.getLoan();

    publishDueDateChangedEvent(loan, loan.getUser(), true);

    return completedFuture(succeeded(renewalContext));
  }

  public CompletableFuture<Result<RequestAndRelatedRecords>> publishDueDateChangedEvent(
    RequestAndRelatedRecords requestAndRelatedRecords, Clients clients) {

    LoanRepository loanRepository = new LoanRepository(clients);
    loanRepository.findOpenLoanForRequest(requestAndRelatedRecords.getRequest())
      .thenCompose(r -> r.after(loan -> publishDueDateChangedEvent(loan, requestAndRelatedRecords)));

    return completedFuture(succeeded(requestAndRelatedRecords));
  }

  public CompletableFuture<Result<Loan>> publishAgedToLostEvents(Loan loan) {
    return publishLogRecord(LoanLogContext.from(loan)
      .withDescription(String.format("Due date: %s", formatDateTimeOptional(loan.getAgedToLostDateTime()))).asJson(), LOAN)
      .thenCompose(r -> r.after(v -> publishStatusChangeEvent(ITEM_AGED_TO_LOST, loan)));
  }

  public CompletableFuture<Result<Void>> publishClosedLoanEvent(Loan loan) {
    if (!CHECKED_IN.getValue().equalsIgnoreCase(loan.getAction())) {
      return publishLogRecord(LoanLogContext.from(loan)
        .withServicePointId(loan.getCheckoutServicePointId()).asJson(), LOAN);
    }
    return CompletableFuture.completedFuture(succeeded(null));
  }

  public CompletableFuture<Result<Loan>> publishMarkedAsMissingLoanEvent(Loan loan) {
    return publishLogRecord(LoanLogContext.from(loan)
      .withDescription(String.format("Additional information: %s", loan.getActionComment())).asJson(), LOAN)
      .thenApply(r -> r.map(v -> loan));
  }

  public CompletableFuture<Result<LoanAnonymizationRecords>> publishAnonymizeEvents(LoanAnonymizationRecords records) {
    return allOf(records.getAnonymizedLoans(), this::publishAnonymizeEvent)
      .thenApply(r -> succeeded(records));
  }

  public CompletableFuture<Result<Void>> publishAnonymizeEvent(Loan loan) {
    return publishLogRecord(LoanLogContext.from(loan).withAction("Anonymize").asJson(), LOAN);
  }

  public CompletableFuture<Result<Void>> publishRecallRequestedEvent(Loan loan) {
    return publishLogRecord(LoanLogContext.from(loan)
      .withAction(LogContextActionResolver.resolveAction(RECALLREQUESTED.getValue()))
      .withDescription(String.format("New due date: %s (from %s)", formatDateTimeOptional(loan.getDueDate()), formatDateTimeOptional(loan.getPreviousDueDate()))).asJson(), LOAN);
  }

  public CompletableFuture<Result<Void>> publishDueDateLogEvent(Loan loan) {
    return publishLogRecord(LoanLogContext.from(loan)
      .withAction(LogContextActionResolver.resolveAction(DUE_DATE_CHANGED.getValue()))
      .withDescription(String.format("New due date: %s (from %s)", formatDateTimeOptional(loan.getDueDate()), formatDateTimeOptional(loan.getPreviousDueDate()))).asJson(), LOAN);
  }

  public CompletableFuture<Result<Void>> publishRenewedEvent(Loan loan) {
    return publishLogRecord(LoanLogContext.from(loan)
      .withDescription(String.format("New due date: %s (from %s)", formatDateTimeOptional(loan.getDueDate()), formatDateTimeOptional(loan.getPreviousDueDate()))).asJson(), LOAN);
  }

  public CompletableFuture<Result<Void>> publishNoticeLogEvent(NoticeLogContext noticeLogContext,
    Result<?> previousStepResult, Throwable throwable) {

    return throwable != null
      ? publishNoticeErrorLogEvent(noticeLogContext, throwable)
      : publishNoticeLogEvent(noticeLogContext, previousStepResult);
  }

  public CompletableFuture<Result<Void>> publishNoticeLogEvent(NoticeLogContext noticeLogContext,
    Result<?> previousStepResult) {

    return previousStepResult.succeeded()
      ? publishNoticeLogEvent(noticeLogContext)
      : publishNoticeErrorLogEvent(noticeLogContext, previousStepResult.cause());
  }

  public CompletableFuture<Result<Void>> publishNoticeLogEvent(NoticeLogContext noticeLogContext) {
    return publishNoticeLogEvent(noticeLogContext, NOTICE);
  }

  public CompletableFuture<Result<Void>> publishNoticeLogEvent(NoticeLogContext noticeLogContext,
    LogEventType eventType) {

    return publishLogRecord(noticeLogContext.withDate(getZonedDateTime()).asJson(), eventType);
  }

  public CompletableFuture<Result<Void>> publishNoticeErrorLogEvent(
    NoticeLogContext noticeLogContext, HttpFailure error) {

    return publishNoticeErrorLogEvent(noticeLogContext, error.toString());
  }

  public CompletableFuture<Result<Void>> publishNoticeErrorLogEvent(
    NoticeLogContext noticeLogContext, Throwable throwable) {

    return publishNoticeErrorLogEvent(noticeLogContext, throwable.getClass().getSimpleName());
  }

  public CompletableFuture<Result<Void>> publishNoticeErrorLogEvent(
    NoticeLogContext noticeLogContext, String errorMessage) {

    return publishNoticeLogEvent(noticeLogContext.withErrorMessage(errorMessage), NOTICE_ERROR);
  }

  public CompletableFuture<Result<Void>> publishLogRecord(JsonObject context, LogEventType payloadType) {
    JsonObject eventJson = new JsonObject();
    write(eventJson, LOG_EVENT_TYPE.value(), payloadType.value());
    write(eventJson, PAYLOAD.value(), context);

    return pubSubPublishingService.publishEvent(LOG_RECORD.name(), eventJson.encode())
      .handle((result, error) -> {
        if (error != null) {
          return failedDueToServerError(error.getMessage());
        }
        return succeeded(null);
      });
  }

  public RequestAndRelatedRecords publishLogRecordAsync(RequestAndRelatedRecords requestAndRelatedRecords, Request originalRequest, LogEventType logEventType) {
    runAsync(() -> publishLogRecord(mapToRequestLogEventJson(originalRequest, requestAndRelatedRecords.getRequest()), logEventType));
    return requestAndRelatedRecords;
  }

}
