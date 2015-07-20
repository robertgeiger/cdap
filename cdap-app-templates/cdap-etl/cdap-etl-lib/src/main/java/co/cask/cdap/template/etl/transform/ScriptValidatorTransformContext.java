/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.template.etl.transform;

import org.apache.commons.validator.GenericValidator;
import org.apache.commons.validator.routines.CreditCardValidator;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.UrlValidator;

/**
 * Context for {@link ValidatorTransform}.
 */
public class ScriptValidatorTransformContext extends GenericValidator {
  private final static CreditCardValidator ccv = new CreditCardValidator();
  private final static UrlValidator urlv = new UrlValidator();
  private final static EmailValidator email = EmailValidator.getInstance();

  /**
   * Checks for a valid credit card number.
   * 
   * @param cardNumber 
   * @return true if valid, false otherwise.
   */
  public boolean isValidCreditCard(String cardNumber) {
    return ccv.isValid(cardNumber);
  }

  /**
   * Checks for a valid url.  
   * * 
   * @param url
   * @return true if valid, false otherwise
   */
  public boolean isValidURL(String url) {
    return urlv.isValid(url);
  }

  /**
   * Checks if a email id is valid. 
   * *
   * @param emailId
   * @return true if valid, false otherwise.
   */
  public boolean isValidEmail(String emailId) {
    return email.isValid(emailId);
  }

}
