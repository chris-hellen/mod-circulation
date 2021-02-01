package org.folio.circulation.resources.handlers.error;

public enum CirculationErrorType {
  INVALID_PICKUP_SERVICE_POINT,
  SERVICE_POINT_IS_NOT_PRESENT,

  INVALID_ITEM,
  INVALID_STATUS,
  INVALID_ITEM_ID,
  ITEM_DOES_NOT_EXIST,
  ITEM_ALREADY_REQUESTED_BY_SAME_USER,
  ITEM_ALREADY_LOANED_TO_SAME_USER,
  ITEM_REQUESTED_BY_ANOTHER_PATRON,
  ITEM_IS_NOT_LOANABLE,
  ITEM_IS_NOT_ALLOWED_FOR_CHECK_OUT,
  ITEM_ALREADY_CHECKED_OUT,
  ITEM_HAS_OPEN_LOANS,
  ITEM_LIMIT_IS_REACHED,

  FAILED_TO_FETCH_ITEM,
  FAILED_TO_FETCH_USER,
  FAILED_TO_FETCH_PROXY_USER,
  FAILED_TO_FETCH_SERVICE_POINT,
  FAILED_TO_FETCH_LOAN,
  FAILED_TO_FETCH_USER_FOR_LOAN,
  FAILED_TO_FETCH_REQUEST_QUEUE,
  FAILED_TO_FETCH_REQUEST_POLICY,
  FAILED_TO_FETCH_TIME_ZONE_CONFIG,
  FAILED_TO_FETCH_LOAN_POLICY,

  USER_IS_INACTIVE,
  PROXY_USER_IS_INACTIVE,
  USER_IS_BLOCKED_MANUALLY,
  USER_IS_BLOCKED_AUTOMATICALLY,
  INVALID_USER_OR_PATRON_GROUP_ID,
  INVALID_PROXY_RELATIONSHIP,

  REQUESTING_DISALLOWED,
  REQUESTING_DISALLOWED_BY_POLICY,
  REQUESTING_DISALLOWED_BY_REQUEST_POLICY,
  REQUESTING_DISALLOWED_FOR_ITEM
}