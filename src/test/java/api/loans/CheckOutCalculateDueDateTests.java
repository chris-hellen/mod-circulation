package api.loans;

import api.support.APITests;
import api.support.builders.CheckOutByBarcodeRequestBuilder;
import io.vertx.core.json.JsonObject;
import org.folio.circulation.domain.OpeningDay;
import org.folio.circulation.domain.OpeningDayPeriod;
import org.folio.circulation.domain.OpeningHour;
import org.folio.circulation.domain.policy.DueDateManagement;
import org.folio.circulation.domain.policy.LoansPolicyProfile;
import org.folio.circulation.support.http.client.IndividualResource;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.net.MalformedURLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static api.APITestSuite.END_OF_2019_DUE_DATE;
import static api.APITestSuite.exampleFixedDueDateSchedulesId;
import static api.support.fixtures.CalendarExamples.*;
import static api.support.fixtures.LibraryHoursExamples.CASE_CALENDAR_IS_UNAVAILABLE_SERVICE_POINT_ID;
import static api.support.matchers.TextDateTimeMatcher.isEquivalentTo;
import static org.folio.circulation.domain.policy.DueDateManagement.*;
import static org.folio.circulation.domain.policy.LoanPolicyPeriod.HOURS;
import static org.folio.circulation.resources.CheckOutByBarcodeResource.DATE_TIME_FORMATTER;
import static org.folio.circulation.support.PeriodUtil.isInPeriodOpeningDay;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CheckOutCalculateDueDateTests extends APITests {

  private static final String INTERVAL_MONTHS = "Months";
  private static final String INTERVAL_HOURS = "Hours";
  private static final String INTERVAL_MINUTES = "Minutes";

  private static final String DUE_DATE_KEY = "dueDate";
  private static final String LOAN_POLICY_ID_KEY = "loanPolicyId";

  private static final String ERROR_MESSAGE_DUE_DATE = "due date should be ";
  private static final String ERROR_MESSAGE_LOAN_POLICY = "last loan policy should be stored";

  /**
   * Scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Months|Weeks|Days
   * Closed Library Due Date Management = Keep the current due date
   * <p>
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should remain unchanged from system calculated due date timestamp
   */
  @Test
  public void testKeepCurrentDueDateLongTermLoans()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final DateTime loanDate = DateTime.now().toDateTime(DateTimeZone.UTC);
    final UUID checkoutServicePointId = UUID.randomUUID();
    int duration = new SplittableRandom().nextInt(1, 12);

    String loanPolicyName = "Keep the current due date: Rolling";
    JsonObject loanPolicyEntry = createLoanPolicyEntry(loanPolicyName, true,
      LoansPolicyProfile.ROLLING.name(), DueDateManagement.KEEP_THE_CURRENT_DUE_DATE.getValue(),
      duration, INTERVAL_MONTHS);
    String loanPolicyId = createLoanPolicy(loanPolicyEntry);

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .on(loanDate)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    assertThat(ERROR_MESSAGE_DUE_DATE + duration,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(loanDate.plusMonths(duration)));
  }

  /**
   * Scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = FIXED
   * Closed Library Due Date Management = Keep the current due date
   * Calendar allDay = false
   * Test period: WED=open, THU=open, FRI=open
   */
  @Test
  public void testKeepCurrentDueDateLongTermLoansFixed()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final UUID checkoutServicePointId = UUID.randomUUID();

    String fixedDueDateScheduleId = exampleFixedDueDateSchedulesId().toString();
    String loanPolicyId = createLoanPolicy(
      createLoanPolicyEntryFixed("Keep the current due date: FIXED",
        fixedDueDateScheduleId,
        LoansPolicyProfile.FIXED.name(),
        DueDateManagement.KEEP_THE_CURRENT_DUE_DATE.getValue()));

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    assertThat(ERROR_MESSAGE_DUE_DATE + END_OF_2019_DUE_DATE,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(END_OF_2019_DUE_DATE));
  }

  /**
   * Scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = FIXED
   * Closed Library Due Date Management = MOVE_TO_THE_END_OF_THE_PREVIOUS_OPEN_DAY
   * Calendar allDay = true
   * Test period: WED=open, THU=open, FRI=open
   */
  @Test
  public void testMoveToEndOfPreviousAllOpenDayFixed()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final UUID checkoutServicePointId = UUID.fromString(CASE_WED_THU_FRI_DAY_ALL_SERVICE_POINT_ID);

    String fixedDueDateScheduleId = exampleFixedDueDateSchedulesId().toString();
    String loanPolicyId = createLoanPolicy(
      createLoanPolicyEntryFixed("MOVE_TO_THE_END_OF_THE_PREVIOUS_OPEN_DAY: FIXED",
        fixedDueDateScheduleId,
        LoansPolicyProfile.FIXED.name(),
        MOVE_TO_THE_END_OF_THE_PREVIOUS_OPEN_DAY.getValue()));

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    DateTime expectedDate = new DateTime(LocalDate.parse(WEDNESDAY_DATE, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER))
      .atTime(LocalTime.MAX).toString()).withZoneRetainFields(DateTimeZone.UTC);

    assertThat(ERROR_MESSAGE_DUE_DATE + expectedDate,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(expectedDate));
  }

  /**
   * Scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = FIXED
   * Closed Library Due Date Management = MOVE_TO_THE_END_OF_THE_PREVIOUS_OPEN_DAY
   * Calendar allDay = false
   * Test period: WED=open, THU=open, FRI=open
   */
  @Test
  public void testMoveToEndOfPreviousOpenDayFixed()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final UUID checkoutServicePointId = UUID.fromString(CASE_WED_THU_FRI_SERVICE_POINT_ID);

    String fixedDueDateScheduleId = exampleFixedDueDateSchedulesId().toString();
    String loanPolicyId = createLoanPolicy(
      createLoanPolicyEntryFixed("MOVE_TO_THE_END_OF_THE_PREVIOUS_OPEN_DAY: FIXED",
        fixedDueDateScheduleId,
        LoansPolicyProfile.FIXED.name(),
        MOVE_TO_THE_END_OF_THE_PREVIOUS_OPEN_DAY.getValue()));

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    DateTime expectedDate = new DateTime(LocalDate.parse(WEDNESDAY_DATE, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER))
      .atTime(LocalTime.parse(END_TIME_SECOND_PERIOD)).toString()).withZoneRetainFields(DateTimeZone.UTC);

    assertThat(ERROR_MESSAGE_DUE_DATE + expectedDate,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(expectedDate));
  }

  /**
   * Scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = FIXED
   * Closed Library Due Date Management = MOVE_TO_THE_END_OF_THE_NEXT_OPEN_DAY
   * Calendar allDay = true
   * Test period: WED=open, THU=open, FRI=open
   */
  @Test
  public void testMoveToEndOfNextAllOpenDayFixed()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final UUID checkoutServicePointId = UUID.fromString(CASE_WED_THU_FRI_DAY_ALL_SERVICE_POINT_ID);

    String fixedDueDateScheduleId = exampleFixedDueDateSchedulesId().toString();
    String loanPolicyId = createLoanPolicy(
      createLoanPolicyEntryFixed("MOVE_TO_THE_END_OF_THE_NEXT_OPEN_DAY: FIXED",
        fixedDueDateScheduleId,
        LoansPolicyProfile.FIXED.name(),
        MOVE_TO_THE_END_OF_THE_NEXT_OPEN_DAY.getValue()));

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    DateTime expectedDate = new DateTime(LocalDate.parse(FRIDAY_DATE, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER))
      .atTime(LocalTime.MAX).toString()).withZoneRetainFields(DateTimeZone.UTC);

    assertThat(ERROR_MESSAGE_DUE_DATE + expectedDate,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(expectedDate));
  }

  /**
   * Scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = FIXED
   * Closed Library Due Date Management = MOVE_TO_THE_END_OF_THE_NEXT_OPEN_DAY
   * Calendar allDay = false
   * Test period: WED=open, THU=open, FRI=open
   */
  @Test
  public void testMoveToEndOfNextOpenDayFixed()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final UUID checkoutServicePointId = UUID.fromString(CASE_WED_THU_FRI_SERVICE_POINT_ID);

    String fixedDueDateScheduleId = exampleFixedDueDateSchedulesId().toString();
    String loanPolicyId = createLoanPolicy(
      createLoanPolicyEntryFixed("MOVE_TO_THE_END_OF_THE_NEXT_OPEN_DAY: FIXED",
        fixedDueDateScheduleId,
        LoansPolicyProfile.FIXED.name(),
        MOVE_TO_THE_END_OF_THE_NEXT_OPEN_DAY.getValue()));

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    DateTime expectedDate = new DateTime(LocalDate.parse(FRIDAY_DATE, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER))
      .atTime(LocalTime.parse(END_TIME_SECOND_PERIOD)).toString()).withZoneRetainFields(DateTimeZone.UTC);

    assertThat(ERROR_MESSAGE_DUE_DATE + expectedDate,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(expectedDate));
  }

  /**
   * Scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = FIXED
   * Closed Library Due Date Management = MOVE_TO_THE_END_OF_THE_CURRENT_DAY
   * Calendar allDay = true
   * Test period: WED=open, THU=open, FRI=open
   */
  @Test
  public void testMoveToEndOfCurrentAllDayFixed()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final UUID checkoutServicePointId = UUID.fromString(CASE_WED_THU_FRI_DAY_ALL_SERVICE_POINT_ID);

    String fixedDueDateScheduleId = exampleFixedDueDateSchedulesId().toString();
    String loanPolicyId = createLoanPolicy(
      createLoanPolicyEntryFixed("MOVE_TO_THE_END_OF_THE_CURRENT_DAY: FIXED",
        fixedDueDateScheduleId,
        LoansPolicyProfile.FIXED.name(),
        MOVE_TO_THE_END_OF_THE_CURRENT_DAY.getValue()));

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    DateTime expectedDate = new DateTime(LocalDate.parse(THURSDAY_DATE, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER))
      .atTime(LocalTime.MAX).toString()).withZoneRetainFields(DateTimeZone.UTC);

    assertThat(ERROR_MESSAGE_DUE_DATE + expectedDate,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(expectedDate));
  }

  /**
   * Scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = FIXED
   * Closed Library Due Date Management = MOVE_TO_THE_END_OF_THE_CURRENT_DAY
   * Calendar allDay = false
   * Test period: WED=open, THU=open, FRI=open
   */
  @Test
  public void testMoveToEndOfCurrentDayFixed()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final UUID checkoutServicePointId = UUID.fromString(CASE_WED_THU_FRI_SERVICE_POINT_ID);

    String fixedDueDateScheduleId = exampleFixedDueDateSchedulesId().toString();
    String loanPolicyId = createLoanPolicy(
      createLoanPolicyEntryFixed("MOVE_TO_THE_END_OF_THE_CURRENT_DAY: FIXED",
        fixedDueDateScheduleId,
        LoansPolicyProfile.FIXED.name(),
        MOVE_TO_THE_END_OF_THE_CURRENT_DAY.getValue()));

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    DateTime expectedDate = new DateTime(LocalDate.parse(THURSDAY_DATE, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER))
      .atTime(LocalTime.parse(END_TIME_SECOND_PERIOD)).toString()).withZoneRetainFields(DateTimeZone.UTC);

    assertThat(ERROR_MESSAGE_DUE_DATE + expectedDate,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(expectedDate));
  }

  /**
   * Test scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Months|Weeks|Days
   * Closed Library Due Date Management = Move to the end of the previous open day
   * <p>
   * Calendar allDay = true (exclude current day)
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the latest SPID-1 endTime for the closest previous Open=true day for SPID-1
   */
  @Test
  public void testMoveToEndOfPreviousAllOpenDay()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_DAY_ALL_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 3;

    // get datetime of endDay
    OpeningDayPeriod openingDay = getFirstFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_THE_END_OF_THE_PREVIOUS_OPEN_DAY,
      duration, INTERVAL_MONTHS, expectedDueDate, false);
  }

  /**
   * Test scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Months|Weeks|Days
   * Closed Library Due Date Management = Move to the end of the previous open day
   * <p>
   * Calendar allDay = false
   * Calendar openingHour = [range time]
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the latest SPID-1 endTime for the closest previous Open=true day for SPID-1
   */
  @Test
  public void testMoveToEndOfPreviousOpenDayTime()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 2;

    // get last datetime from hours period
    OpeningDayPeriod openingDay = getFirstFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_THE_END_OF_THE_PREVIOUS_OPEN_DAY,
      duration, INTERVAL_MONTHS, expectedDueDate, false);
  }

  /**
   * Test scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Months|Weeks|Days
   * Closed Library Due Date Management = Move to the end of the next open day
   * <p>
   * Calendar allDay = true (exclude current day)
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the latest SPID-1 endTime for the closest next Open=true day for SPID-1
   */
  @Test
  public void testMoveToEndOfNextAllOpenDay()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_DAY_ALL_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 5;

    // get datetime of endDay
    OpeningDayPeriod openingDay = getLastFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_THE_END_OF_THE_NEXT_OPEN_DAY,
      duration, INTERVAL_MONTHS, expectedDueDate, false);
  }

  /**
   * Test scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Months|Weeks|Days
   * Closed Library Due Date Management = Move to the end of the next open day
   * <p>
   * Calendar allDay = false
   * Calendar openingHour = [range time]
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the latest SPID-1 endTime for the closest next Open=true day for SPID-1
   */
  @Test
  public void testMoveToEndOfNextOpenDay()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 5;

    // get last datetime from hours period
    OpeningDayPeriod openingDay = getLastFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_THE_END_OF_THE_NEXT_OPEN_DAY,
      duration, INTERVAL_MONTHS, expectedDueDate, false);
  }

  /**
   * Test scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Months|Weeks|Days
   * Closed Library Due Date Management = Move to the end of the current day
   * <p>
   * Calendar allDay = true (exclude current day)
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * If SPID-1 is determined to be CLOSED for system-calculated due date (and timestamp as applicable for short-term loans)
   * Then the due date timestamp should be changed to the endTime current day for SPID-1 (i.e., truncating the loan length)
   */
  @Test
  public void testMoveToEndOfCurrentAllDay()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_DAY_ALL_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 5;

    OpeningDayPeriod openingDay = getCurrentFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_THE_END_OF_THE_CURRENT_DAY,
      duration, INTERVAL_MONTHS, expectedDueDate, false);
  }

  /**
   * Test scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Months|Weeks|Days
   * Closed Library Due Date Management = Move to the end of the current day
   * <p>
   * Calendar allDay = false
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * If SPID-1 is determined to be CLOSED for system-calculated due date (and timestamp as applicable for short-term loans)
   * Then the due date timestamp should be changed to the endTime current day for SPID-1 (i.e., truncating the loan length)
   */
  @Test
  public void testMoveToEndOfCurrentDay()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 5;

    OpeningDayPeriod openingDay = getCurrentFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_THE_END_OF_THE_CURRENT_DAY,
      duration, INTERVAL_MONTHS, expectedDueDate, false);
  }

  /**
   * Test scenario for Long-term loans:
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Months|Weeks|Days
   * Closed Library Due Date Management = Move to the end of the current day
   * <p>
   * Calendar allDay = false
   * Test period: WED=open, THU=open, FRI=open
   * <p>
   * Expected result:
   * If SPID-1 is determined to be CLOSED for system-calculated due date (and timestamp as applicable for short-term loans)
   * Then the due date timestamp should be changed to the endTime current day for SPID-1 (i.e., truncating the loan length)
   */
  @Test
  public void testMoveToEndOfCurrentDayCase2()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_WED_THU_FRI_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 2;

    OpeningDayPeriod openingDay = getCurrentFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_THE_END_OF_THE_CURRENT_DAY,
      duration, INTERVAL_MONTHS, expectedDueDate, false);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Hours
   * Closed Library Due Date Management = Move to the end of the current service point hours
   * <p>
   * Calendar allDay = true (exclude current day)
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the endTime of the current service point for SPID-1 (i.e., truncating the loan length)
   */
  @Test
  public void testMoveToEndOfCurrentServicePointHoursAllDay()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_DAY_ALL_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 2;

    OpeningDayPeriod openingDay = getCurrentFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_END_OF_CURRENT_SERVICE_POINT_HOURS,
      duration, INTERVAL_HOURS, expectedDueDate, false);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Hours
   * Closed Library Due Date Management = Move to the end of the current service point hours
   * <p>
   * Calendar allDay = true (exclude current day)
   * Test period: WED=open, THU=open, FRI=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the endTime of the current service point for SPID-1 (i.e., truncating the loan length)
   */
  @Test
  public void testMoveToEndOfCurrentServicePointHoursAllDayCase2()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_WED_THU_FRI_DAY_ALL_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 2;

    OpeningDayPeriod openingDay = getCurrentFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_END_OF_CURRENT_SERVICE_POINT_HOURS,
      duration, INTERVAL_HOURS, expectedDueDate, false);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Hours
   * Closed Library Due Date Management = Move to the end of the current service point hours
   * <p>
   * Calendar allDay = false
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the endTime of the current service point for SPID-1 (i.e., truncating the loan length)
   */
  @Test
  public void testMoveToEndOfCurrentServicePointHours()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 5;

    OpeningDayPeriod openingDay = getCurrentFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_END_OF_CURRENT_SERVICE_POINT_HOURS,
      duration, INTERVAL_HOURS, expectedDueDate, false);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Hours
   * Closed Library Due Date Management = Move to the end of the current service point hours
   * <p>
   * Calendar allDay = false
   * Test period: WED=open, THU=open, FRI=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the endTime of the current service point for SPID-1 (i.e., truncating the loan length)
   */
  @Test
  public void testMoveToEndOfCurrentServicePointHoursCase2()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_WED_THU_FRI_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 5;

    OpeningDayPeriod openingDay = getCurrentFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getEndDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_END_OF_CURRENT_SERVICE_POINT_HOURS,
      duration, INTERVAL_HOURS, expectedDueDate, false);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = Hours
   * Closed Library Due Date Management = Move to the beginning of the next open service point hours
   * <p>
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the earliest SPID-1 startTime for the closest next Open=true available hours for SPID-1
   * (Note that the system needs to logically consider 'rollover' scenarios where the service point remains open
   * for a continuity of hours that flow from one system date into the next - for example,
   * a service point that remains open until 2AM; then reopens at 8AM. In such a scenario,
   * the system should consider the '...beginning of the next open service point hours' to be 8AM. <NEED TO COME BACK TO THIS
   */
  @Test
  public void testMoveToBeginningOfNextOpenServicePointHours()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();

    int duration = 5;

    OpeningDayPeriod openingDay = getLastFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getStartDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_BEGINNING_OF_NEXT_OPEN_SERVICE_POINT_HOURS,
      duration, INTERVAL_HOURS, expectedDueDate, false);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = Hours
   * Closed Library Due Date Management = Move to the beginning of the next open service point hours
   * Calendar allDay = true
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the earliest SPID-1 startTime for the closest next Open=true available hours for SPID-1
   */
  @Test
  public void testMoveToBeginningOfNextOpenServicePointHoursAllDay()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_DAY_ALL_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 5;

    OpeningDayPeriod openingDay = getLastFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getStartDateTimeOpeningDay(openingDay.getOpeningDay());

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_BEGINNING_OF_NEXT_OPEN_SERVICE_POINT_HOURS,
      duration, INTERVAL_HOURS, expectedDueDate, false);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = Hours
   * Closed Library Due Date Management = Move to the beginning of the next open service point hours
   * Calendar allDay = true
   * Test period: WED=open, THU=open, FRI=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the earliest SPID-1 startTime for the closest next Open=true available hours for SPID-1
   */
  @Test
  public void testMoveToBeginningOfNextOpenServicePointHoursAllDayCase2()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_WED_THU_FRI_DAY_ALL_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 5;

    List<OpeningDayPeriod> openingDays = getCurrentAndNextFakeOpeningDayByServId(servicePointId);
    String currentDate = openingDays.get(0).getOpeningDay().getDate();
    LocalDate localDate = LocalDate.parse(currentDate, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));
    LocalDateTime localDateTime = localDate.atTime(LocalTime.now(ZoneOffset.UTC))
      .plusHours(duration);
    DateTime expectedDueDate = new DateTime(localDateTime.toString()).withZoneRetainFields(DateTimeZone.UTC);

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_BEGINNING_OF_NEXT_OPEN_SERVICE_POINT_HOURS,
      duration, INTERVAL_HOURS, expectedDueDate, true);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = Minutes
   * Closed Library Due Date Management = Move to the beginning of the next open service point hours
   * Calendar allDay = false
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the earliest SPID-1 startTime for the closest next Open=true available hours for SPID-1
   */
  @Test
  public void testMoveToBeginningOfNextOpenServicePointMinutesCase1()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 30;
    String interval = INTERVAL_MINUTES;

    List<OpeningDayPeriod> openingDays = getCurrentAndNextFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getStartDateTimeOpeningDayRollover(openingDays, interval, duration);

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_BEGINNING_OF_NEXT_OPEN_SERVICE_POINT_HOURS,
      duration, interval, expectedDueDate, true);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = Minutes
   * Closed Library Due Date Management = Move to the beginning of the next open service point hours
   * Calendar allDay = true
   * Test period: WED=open, THU=open, FRI=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the earliest SPID-1 startTime for the closest next Open=true available hours for SPID-1
   */
  @Test
  public void testMoveToBeginningOfNextOpenServicePointMinutesAllDay()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_WED_THU_FRI_DAY_ALL_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 30;
    String interval = INTERVAL_MINUTES;

    List<OpeningDayPeriod> openingDays = getCurrentAndNextFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getStartDateTimeOpeningDayRollover(openingDays, interval, duration);

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_BEGINNING_OF_NEXT_OPEN_SERVICE_POINT_HOURS,
      duration, interval, expectedDueDate, true);
  }

  /**
   * Test scenario for Short-term loans
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = Minutes
   * Closed Library Due Date Management = Move to the beginning of the next open service point hours
   * Calendar allDay = true
   * Test period: FRI=open, SAT=close, MON=open
   * <p>
   * Expected result:
   * Then the due date timestamp should be changed to the earliest SPID-1 startTime for the closest next Open=true available hours for SPID-1
   */
  @Test
  public void testMoveToBeginningOfNextOpenServicePointMinutesAllDayCase1()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    String servicePointId = CASE_FRI_SAT_MON_DAY_ALL_SERVICE_POINT_ID;
    String policyProfileName = LoansPolicyProfile.ROLLING.name();
    int duration = 30;
    String interval = INTERVAL_MINUTES;

    List<OpeningDayPeriod> openingDays = getCurrentAndNextFakeOpeningDayByServId(servicePointId);
    DateTime expectedDueDate = getStartDateTimeOpeningDayRollover(openingDays, interval, duration);

    checkFixedDayOrTime(servicePointId, policyProfileName, MOVE_TO_BEGINNING_OF_NEXT_OPEN_SERVICE_POINT_HOURS,
      duration, interval, expectedDueDate, true);
  }

  /**
   * Scenario for Short-term loans:
   * Loanable = Y
   * Loan profile = Rolling
   * Loan period = X Hours|Minutes
   * Closed Library Due Date Management = Keep the current due date/time
   * <p>
   * Expected result:
   * Then the due date timestamp should remain unchanged from system calculated due date timestamp
   */
  @Test
  public void testKeepCurrentDueDateShortTermLoans()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final DateTime loanDate = DateTime.now().withZoneRetainFields(DateTimeZone.UTC);
    final UUID checkoutServicePointId = UUID.randomUUID();
    int duration = new SplittableRandom().nextInt(1, 12);

    String loanPolicyName = "Keep the current due date/time";
    JsonObject loanPolicyEntry = createLoanPolicyEntry(loanPolicyName, true,
      LoansPolicyProfile.ROLLING.name(), DueDateManagement.KEEP_THE_CURRENT_DUE_DATE_TIME.getValue(),
      duration, INTERVAL_HOURS);
    String loanPolicyId = createLoanPolicy(loanPolicyEntry);

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .on(loanDate)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    assertThat(ERROR_MESSAGE_DUE_DATE + duration,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(loanDate.plusHours(duration)));
  }

  /**
   * Exception Scenario
   * When:
   * - Loanable = N
   */
  @Test
  public void testExceptionScenario()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final DateTime loanDate = DateTime.now().toDateTime(DateTimeZone.UTC);
    final UUID checkoutServicePointId = UUID.randomUUID();
    int duration = new SplittableRandom().nextInt(1, 60);

    String loanPolicyName = "Loan Policy Exception Scenario";
    JsonObject loanPolicyEntry = createLoanPolicyEntry(loanPolicyName, false,
      LoansPolicyProfile.ROLLING.name(), DueDateManagement.KEEP_THE_CURRENT_DUE_DATE.getValue(),
      duration, INTERVAL_MINUTES);
    String loanPolicyId = createLoanPolicy(loanPolicyEntry);

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .on(loanDate)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    assertThat(ERROR_MESSAGE_DUE_DATE + duration,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(loanDate.plusMinutes(duration)));
  }

  /**
   * Test scenario when Calendar API is unavailable
   */
  @Test
  public void testScenarioWhenCalendarApiIsUnavailable()
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    final DateTime loanDate = DateTime.now().toDateTime(DateTimeZone.UTC);
    int duration = new SplittableRandom().nextInt(1, 12);
    String loanPolicyName = "Calendar API is unavailable";
    JsonObject loanPolicyEntry = createLoanPolicyEntry(loanPolicyName, true,
      LoansPolicyProfile.ROLLING.name(), DueDateManagement.KEEP_THE_CURRENT_DUE_DATE_TIME.getValue(),
      duration, INTERVAL_HOURS);
    String loanPolicyId = createLoanPolicy(loanPolicyEntry);

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(itemsFixture.basedUponSmallAngryPlanet())
        .to(usersFixture.steve())
        .on(loanDate)
        .at(UUID.fromString(CASE_CALENDAR_IS_UNAVAILABLE_SERVICE_POINT_ID)));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    assertThat(ERROR_MESSAGE_DUE_DATE + duration,
      loan.getString(DUE_DATE_KEY), isEquivalentTo(loanDate.plusHours(duration)));
  }

  private void checkFixedDayOrTime(String servicePointId, String policyProfileName,
                                   DueDateManagement dueDateManagement, int duration, String interval,
                                   DateTime expectedDueDate, boolean isIncludeTime)
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    IndividualResource smallAngryPlanet = itemsFixture.basedUponSmallAngryPlanet();
    final IndividualResource steve = usersFixture.steve();
    final DateTime loanDate = DateTime.now().toDateTime(DateTimeZone.UTC);
    final UUID checkoutServicePointId = UUID.fromString(servicePointId);

    JsonObject loanPolicyEntry = createLoanPolicyEntry(dueDateManagement.getValue(), true,
      policyProfileName, dueDateManagement.getValue(), duration, interval);
    String loanPolicyId = createLoanPolicy(loanPolicyEntry);

    final IndividualResource response = loansFixture.checkOutByBarcode(
      new CheckOutByBarcodeRequestBuilder()
        .forItem(smallAngryPlanet)
        .to(steve)
        .on(loanDate)
        .at(checkoutServicePointId));

    final JsonObject loan = response.getJson();

    assertThat(ERROR_MESSAGE_LOAN_POLICY,
      loan.getString(LOAN_POLICY_ID_KEY), is(loanPolicyId));

    if (isIncludeTime) {
      checkDateTime(expectedDueDate, loan);
    } else {
      DateTime actualDueDate = DateTime.parse(loan.getString(DUE_DATE_KEY));
      assertThat(ERROR_MESSAGE_DUE_DATE + expectedDueDate + ", actual due date is " + actualDueDate,
        actualDueDate.compareTo(expectedDueDate) == 0);
    }
  }

  /**
   * Check the day and dateTime
   */
  private void checkDateTime(DateTime expectedDueDate, JsonObject loan) {
    DateTime actualDueDate = getThresholdDateTime(DateTime.parse(loan.getString(DUE_DATE_KEY)));

    assertThat("due date day should be " + expectedDueDate.getDayOfWeek() + " day of week",
      actualDueDate.getDayOfWeek() == expectedDueDate.getDayOfWeek());

    DateTime thresholdDateTime = getThresholdDateTime(expectedDueDate);
    assertThat(ERROR_MESSAGE_DUE_DATE + thresholdDateTime + ", actual due date is " + actualDueDate,
      actualDueDate.compareTo(thresholdDateTime) == 0);
  }

  private DateTime findDateTimeInPeriod(OpeningDayPeriod currentDayPeriod, LocalTime offsetTime, String currentDate) {
    List<OpeningHour> openingHoursList = currentDayPeriod.getOpeningDay().getOpeningHour();

    LocalDate localDate = LocalDate.parse(currentDate, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));
    boolean isInPeriod = false;
    LocalTime newOffsetTime = null;
    for (int i = 0; i < openingHoursList.size() - 1; i++) {
      LocalTime startTimeFirst = LocalTime.parse(openingHoursList.get(i).getStartTime());
      LocalTime startTimeSecond = LocalTime.parse(openingHoursList.get(i + 1).getStartTime());
      if (offsetTime.isAfter(startTimeFirst) && offsetTime.isBefore(startTimeSecond)) {
        isInPeriod = true;
        newOffsetTime = startTimeSecond;
        break;
      } else {
        newOffsetTime = startTimeSecond;
      }
    }

    LocalTime localTime = Objects.isNull(newOffsetTime) ? offsetTime.withMinute(0) : newOffsetTime;
    return new DateTime(LocalDateTime.of(localDate, isInPeriod ? localTime : offsetTime).toString()).withZoneRetainFields(DateTimeZone.UTC);
  }

  private DateTime getStartDateTimeOpeningDayRollover(List<OpeningDayPeriod> openingDays, String interval, int duration) {
    OpeningDayPeriod currentDayPeriod = openingDays.get(0);
    OpeningDayPeriod nextDayPeriod = openingDays.get(1);

    if (interval.equalsIgnoreCase(HOURS.name())) {
      if (currentDayPeriod.getOpeningDay().getAllDay()) {
        LocalDateTime localDateTime = LocalDateTime.now(ZoneOffset.UTC).plusHours(duration);
        return new DateTime(localDateTime.toString()).withZoneRetainFields(DateTimeZone.UTC);
      } else {
        LocalTime offsetTime = LocalTime.now(ZoneOffset.UTC).plusHours(duration);
        String currentDate = currentDayPeriod.getOpeningDay().getDate();

        if (isInPeriodOpeningDay(currentDayPeriod.getOpeningDay().getOpeningHour(), offsetTime)) {
          return findDateTimeInPeriod(currentDayPeriod, offsetTime, currentDate);
        } else {
          OpeningDay nextOpeningDay = nextDayPeriod.getOpeningDay();
          String nextDate = nextOpeningDay.getDate();
          LocalDate localDate = LocalDate.parse(nextDate, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));

          if (nextOpeningDay.getAllDay()) {
            return new DateTime(localDate.atTime(LocalTime.MIN).toString()).withZoneRetainFields(DateTimeZone.UTC);
          } else {
            OpeningHour openingHour = nextOpeningDay.getOpeningHour().get(0);
            LocalTime startTime = LocalTime.parse(openingHour.getStartTime());
            return new DateTime(LocalDateTime.of(localDate, startTime).toString()).withZoneRetainFields(DateTimeZone.UTC);
          }
        }
      }
    } else {
      OpeningDay currentOpeningDay = currentDayPeriod.getOpeningDay();
      String currentDate = currentOpeningDay.getDate();

      if (currentOpeningDay.getOpen()) {
        if (currentOpeningDay.getAllDay()) {
          LocalDate currentLocalDate = LocalDate.parse(currentDate, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));
          LocalDateTime currentEndLocalDateTime = LocalDateTime.of(currentLocalDate, LocalTime.MAX);
          LocalDateTime offsetLocalDateTime = LocalDateTime.of(currentLocalDate, LocalTime.now(ZoneOffset.UTC)).plusMinutes(duration);

          if (isInCurrentLocalDateTime(currentEndLocalDateTime, offsetLocalDateTime)) {
            return new DateTime(offsetLocalDateTime.toString()).withZoneRetainFields(DateTimeZone.UTC);
          } else {
            OpeningDay nextOpeningDay = nextDayPeriod.getOpeningDay();
            String nextDate = nextOpeningDay.getDate();
            LocalDate localDate = LocalDate.parse(nextDate, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));

            if (nextOpeningDay.getAllDay()) {
              return new DateTime(localDate.atTime(LocalTime.MIN).toString()).withZoneRetainFields(DateTimeZone.UTC);
            } else {
              OpeningHour openingHour = nextOpeningDay.getOpeningHour().get(0);
              LocalTime startTime = LocalTime.parse(openingHour.getStartTime());
              return new DateTime(LocalDateTime.of(localDate, startTime).toString()).withZoneRetainFields(DateTimeZone.UTC);
            }
          }
        } else {
          LocalTime offsetTime = LocalTime.now(ZoneOffset.UTC).plusMinutes(duration);
          if (isInPeriodOpeningDay(currentOpeningDay.getOpeningHour(), offsetTime)) {
            LocalDate localDate = LocalDate.parse(currentDate, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));
            return new DateTime(LocalDateTime.of(localDate, offsetTime).toString()).withZoneRetainFields(DateTimeZone.UTC);
          } else {
            OpeningDay nextOpeningDay = nextDayPeriod.getOpeningDay();
            String nextDate = nextOpeningDay.getDate();
            LocalDate localDate = LocalDate.parse(nextDate, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));

            if (nextOpeningDay.getAllDay()) {
              return new DateTime(localDate.atTime(LocalTime.MIN).toString()).withZoneRetainFields(DateTimeZone.UTC);
            } else {
              OpeningHour openingHour = nextOpeningDay.getOpeningHour().get(0);
              LocalTime startTime = LocalTime.parse(openingHour.getStartTime());
              return new DateTime(LocalDateTime.of(localDate, startTime).toString()).withZoneRetainFields(DateTimeZone.UTC);
            }
          }
        }
      } else {
        OpeningDay nextOpeningDay = nextDayPeriod.getOpeningDay();
        String nextDate = nextOpeningDay.getDate();
        LocalDate nextLocalDate = LocalDate.parse(nextDate, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));

        if (nextOpeningDay.getAllDay()) {
          return new DateTime(nextLocalDate.atTime(LocalTime.MIN).toString()).withZoneRetainFields(DateTimeZone.UTC);
        }
        OpeningHour openingHour = nextOpeningDay.getOpeningHour().get(0);
        LocalTime startTime = LocalTime.parse(openingHour.getStartTime());
        return new DateTime(LocalDateTime.of(nextLocalDate, startTime).toString()).withZoneRetainFields(DateTimeZone.UTC);
      }
    }
  }

  /**
   * Minor threshold when comparing minutes or milliseconds of dateTime
   */
  private DateTime getThresholdDateTime(DateTime dateTime) {
    return dateTime
      .withSecondOfMinute(0)
      .withMillisOfSecond(0);
  }

  private DateTime getEndDateTimeOpeningDay(OpeningDay openingDay) {
    boolean allDay = openingDay.getAllDay();
    String date = openingDay.getDate();
    LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));

    if (allDay) {
      return getDateTimeOfEndDay(localDate);
    } else {
      List<OpeningHour> openingHours = openingDay.getOpeningHour();

      if (openingHours.isEmpty()) {
        return getDateTimeOfEndDay(localDate);
      }
      OpeningHour openingHour = openingHours.get(openingHours.size() - 1);
      LocalTime localTime = LocalTime.parse(openingHour.getEndTime());
      return new DateTime(LocalDateTime.of(localDate, localTime).toString()).withZoneRetainFields(DateTimeZone.UTC);
    }
  }

  private DateTime getStartDateTimeOpeningDay(OpeningDay openingDay) {
    boolean allDay = openingDay.getAllDay();
    String date = openingDay.getDate();
    LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER));

    if (allDay) {
      return getDateTimeOfStartDay(localDate);
    } else {
      List<OpeningHour> openingHours = openingDay.getOpeningHour();

      if (openingHours.isEmpty()) {
        return getDateTimeOfStartDay(localDate);
      }
      OpeningHour openingHour = openingHours.get(0);
      LocalTime localTime = LocalTime.parse(openingHour.getStartTime());
      return new DateTime(LocalDateTime.of(localDate, localTime).toString()).withZoneRetainFields(DateTimeZone.UTC);
    }
  }

  /**
   * Determine whether the offset date is in the time period of the incoming current date
   *
   * @param currentLocalDateTime incoming LocalDateTime
   * @param offsetLocalDateTime  LocalDateTime with some offset days / hour / minutes
   * @return true if offsetLocalDateTime is contains offsetLocalDateTime in the time period
   */
  private boolean isInCurrentLocalDateTime(LocalDateTime currentLocalDateTime, LocalDateTime offsetLocalDateTime) {
    return offsetLocalDateTime.isBefore(currentLocalDateTime) || offsetLocalDateTime.isEqual(currentLocalDateTime);
  }

  /**
   * Get the date with the end of the day
   */
  private DateTime getDateTimeOfEndDay(LocalDate localDate) {
    return new DateTime(localDate.atTime(LocalTime.MAX).toString()).withZoneRetainFields(DateTimeZone.UTC);
  }

  /**
   * Get the date with the start of the day
   */
  private DateTime getDateTimeOfStartDay(LocalDate localDate) {
    return new DateTime(localDate.atTime(LocalTime.MIN).toString()).withZoneRetainFields(DateTimeZone.UTC);
  }

  /**
   * Create a fake json LoanPolicy
   */
  private JsonObject createLoanPolicyEntry(String name, boolean loanable,
                                           String profileId, String dueDateManagement,
                                           int duration, String intervalId) {
    return new JsonObject()
      .put("name", name)
      .put("description", "LoanPolicy")
      .put("loanable", loanable)
      .put("renewable", true)
      .put("loansPolicy", new JsonObject()
        .put("profileId", profileId)
        .put("period", new JsonObject().put("duration", duration).put("intervalId", intervalId))
        .put("closedLibraryDueDateManagementId", dueDateManagement))
      .put("renewalsPolicy", new JsonObject()
        .put("renewFromId", "CURRENT_DUE_DATE")
        .put("differentPeriod", false));
  }

  /**
   * Create a fake json LoanPolicy for fixed period
   */
  private JsonObject createLoanPolicyEntryFixed(String name, String fixedDueDateScheduleId,
                                                String profileId, String dueDateManagement) {
    return new JsonObject()
      .put("name", name)
      .put("description", "New LoanPolicy")
      .put("loanable", true)
      .put("renewable", true)
      .put("loansPolicy", new JsonObject()
        .put("profileId", profileId)
        .put("closedLibraryDueDateManagementId", dueDateManagement)
        .put("fixedDueDateScheduleId", fixedDueDateScheduleId)
      )
      .put("renewalsPolicy", new JsonObject()
        .put("renewFromId", "CURRENT_DUE_DATE")
        .put("differentPeriod", false));
  }

  private String createLoanPolicy(JsonObject loanPolicyEntry)
    throws InterruptedException, MalformedURLException, TimeoutException, ExecutionException {

    IndividualResource resource = loanPolicyClient.create(loanPolicyEntry);

    policiesToDelete.add(resource.getId());

    useLoanPolicyAsFallback(resource.getId());

    return resource.getId().toString();
  }
}
