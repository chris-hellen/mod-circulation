package org.folio.circulation.rules;

public class AppliedRuleConditionsEntity {

  boolean isItemTypePresent;
  boolean isLoanTypePresent;
  boolean isPatronGroupPresent;

  public AppliedRuleConditionsEntity(boolean isItemTypePresent,
    boolean isLoanTypePresent, boolean isPatronGroupPresent) {

    this.isItemTypePresent = isItemTypePresent;
    this.isLoanTypePresent = isLoanTypePresent;
    this.isPatronGroupPresent = isPatronGroupPresent;
  }

  public boolean isItemTypePresent() {
    return isItemTypePresent;
  }

  public boolean isLoanTypePresent() {
    return isLoanTypePresent;
  }

  public boolean isPatronGroupPresent() {
    return isPatronGroupPresent;
  }
}
