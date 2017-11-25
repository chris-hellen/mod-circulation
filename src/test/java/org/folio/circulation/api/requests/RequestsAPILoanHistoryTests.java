package org.folio.circulation.api.requests;

import io.vertx.core.json.JsonObject;
import org.folio.circulation.api.support.builders.RequestRequestBuilder;
import org.folio.circulation.api.support.builders.UserRequestBuilder;
import org.folio.circulation.api.support.http.ResourceClient;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.folio.circulation.api.support.fixtures.LoanFixture.checkInLoan;
import static org.folio.circulation.api.support.fixtures.LoanFixture.checkOutItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;


public class RequestsAPILoanHistoryTests extends RequestsAPITests {
  private final ResourceClient loansStorageClient = ResourceClient.forLoansStorage(client);

  @Test
  public void creatingHoldRequestChangesTheOpenLoanForTheSameItem()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID id = UUID.randomUUID();

    UUID itemId = itemsFixture.basedUponSmallAngryPlanet().getId();

    UUID loanId = checkOutItem(itemId, loansClient).getId();

    requestsClient.create(new RequestRequestBuilder()
      .hold()
      .withId(id)
      .withItemId(itemId)
      .withRequesterId(usersClient.create(new UserRequestBuilder()).getId()));

    JsonObject loanFromStorage = loansStorageClient.getById(loanId).getJson();

    assertThat("action snapshot in storage is not hold requested",
      loanFromStorage.getString("action"), is("holdrequested"));

    assertThat("item status snapshot in storage is not checked out - held",
      loanFromStorage.getString("itemStatus"), is("Checked out - Held"));
  }

  @Test
  public void creatingRecallRequestChangesTheOpenLoanForTheSameItem()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID id = UUID.randomUUID();

    UUID itemId = itemsFixture.basedUponSmallAngryPlanet().getId();

    UUID loanId = checkOutItem(itemId, loansClient).getId();

    requestsClient.create(new RequestRequestBuilder()
      .recall()
      .withId(id)
      .withItemId(itemId)
      .withRequesterId(usersClient.create(new UserRequestBuilder()).getId()));

    JsonObject loanFromStorage = loansStorageClient.getById(loanId).getJson();

    assertThat("item status snapshot in storage is not checked out - recalled",
      loanFromStorage.getString("itemStatus"), is("Checked out - Recalled"));
  }

  @Test
  public void creatingPageRequestDoesNotChangeTheOpenLoanForSameItem()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID id = UUID.randomUUID();

    UUID itemId = itemsFixture.basedUponSmallAngryPlanet().getId();

    UUID loanId = checkOutItem(itemId, loansClient).getId();

    requestsClient.create(new RequestRequestBuilder()
      .page()
      .withId(id)
      .withItemId(itemId)
      .withRequesterId(usersClient.create(new UserRequestBuilder()).getId()));

    JsonObject loanFromStorage = loansStorageClient.getById(loanId).getJson();

    assertThat("action snapshot in storage is not still checked out",
      loanFromStorage.getString("action"), is("checkedout"));

    assertThat("item status snapshot in storage is not still checked out",
      loanFromStorage.getString("itemStatus"), is("Checked out"));
  }

  @Test
  public void creatingHoldRequestDoesNotChangeClosedLoanForTheSameItem()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID id = UUID.randomUUID();

    UUID itemId = itemsFixture.basedUponSmallAngryPlanet().getId();

    UUID closedLoanId = checkOutItem(itemId, loansClient).getId();

    checkInLoan(closedLoanId, loansClient);

    checkOutItem(itemId, loansClient).getId();

    requestsClient.create(new RequestRequestBuilder()
      .hold()
      .withId(id)
      .withItemId(itemId)
      .withRequesterId(usersClient.create(new UserRequestBuilder()).getId()));

    JsonObject closedLoanFromStorage = loansStorageClient.getById(closedLoanId)
      .getJson();

    assertThat("action snapshot for closed loan should not change",
      closedLoanFromStorage.getString("action"), is("checkedin"));

    assertThat("item status snapshot for closed loan should not change",
      closedLoanFromStorage.getString("itemStatus"), is("Available"));
  }

  @Test
  public void creatingRecallRequestDoesNotChangeClosedLoanForTheSameItem()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID id = UUID.randomUUID();

    UUID itemId = itemsFixture.basedUponSmallAngryPlanet().getId();

    UUID closedLoanId = checkOutItem(itemId, loansClient).getId();

    checkInLoan(closedLoanId, loansClient);

    checkOutItem(itemId, loansClient).getId();

    requestsClient.create(new RequestRequestBuilder()
      .recall()
      .withId(id)
      .withItemId(itemId)
      .withRequesterId(usersClient.create(new UserRequestBuilder()).getId()));

    JsonObject closedLoanFromStorage = loansStorageClient.getById(closedLoanId)
      .getJson();

    assertThat("action snapshot for closed loan should not change",
      closedLoanFromStorage.getString("action"), is("checkedin"));

    assertThat("item status snapshot for closed loan should not change",
      closedLoanFromStorage.getString("itemStatus"), is("Available"));
  }

  @Test
  public void creatingHoldRequestDoesNotChangeOpenLoanForDifferentItem()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID id = UUID.randomUUID();

    UUID itemId = itemsFixture.basedUponSmallAngryPlanet().getId();

    UUID otherItemId = itemsFixture.basedUponNod().getId();

    checkOutItem(itemId, loansClient).getId();
    UUID loanForOtherItemId = checkOutItem(otherItemId, loansClient).getId();

    requestsClient.create(new RequestRequestBuilder()
      .hold()
      .withId(id)
      .withItemId(itemId)
      .withRequesterId(usersClient.create(new UserRequestBuilder()).getId()));

    JsonObject storageLoanForOtherItem = loansStorageClient.getById(loanForOtherItemId)
      .getJson();

    assertThat("action snapshot for open loan for other item should not change",
      storageLoanForOtherItem.getString("action"), is("checkedout"));

    assertThat("item status snapshot for open loan for other item should not change",
      storageLoanForOtherItem.getString("itemStatus"), is("Checked out"));
  }

  @Test
  public void creatingRecallRequestDoesNotChangeOpenLoanForDifferentItem()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID id = UUID.randomUUID();

    UUID itemId = itemsFixture.basedUponSmallAngryPlanet().getId();;

    UUID otherItemId = itemsFixture.basedUponNod().getId();

    checkOutItem(itemId, loansClient).getId();
    UUID loanForOtherItemId = checkOutItem(otherItemId, loansClient).getId();

    requestsClient.create(new RequestRequestBuilder()
      .recall()
      .withId(id)
      .withItemId(itemId)
      .withRequesterId(usersClient.create(new UserRequestBuilder()).getId()));

    JsonObject storageLoanForOtherItem = loansStorageClient.getById(loanForOtherItemId)
      .getJson();

    assertThat("action snapshot for open loan for other item should not change",
      storageLoanForOtherItem.getString("action"), is("checkedout"));

    assertThat("item status snapshot for open loan for other item should not change",
      storageLoanForOtherItem.getString("itemStatus"), is("Checked out"));
  }

  @Test
  public void creatingHoldRequestStillSucceedsWhenThereIsNoLoan()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID id = UUID.randomUUID();

    UUID itemId = itemsFixture.basedUponSmallAngryPlanet().getId();

    UUID loanId = checkOutItem(itemId, loansClient).getId();

    loansClient.delete(loanId);

    requestsClient.create(new RequestRequestBuilder()
      .hold()
      .withId(id)
      .withItemId(itemId)
      .withRequesterId(usersClient.create(new UserRequestBuilder()).getId()));
  }

  @Test
  public void creatingRecallRequestStillSucceedsWhenThereIsNoLoan()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException,
    UnsupportedEncodingException {

    UUID id = UUID.randomUUID();

    UUID itemId = itemsFixture.basedUponSmallAngryPlanet().getId();

    UUID loanId = checkOutItem(itemId, loansClient).getId();

    loansClient.delete(loanId);

    requestsClient.create(new RequestRequestBuilder()
      .recall()
      .withId(id)
      .withItemId(itemId)
      .withRequesterId(usersClient.create(new UserRequestBuilder()).getId()));
  }
}
