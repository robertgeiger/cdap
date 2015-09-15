/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.exception;

import co.cask.cdap.proto.Id;

/**
 * Thrown when an element cannot be deleted.
 * @deprecated Use {@link co.cask.cdap.common.CannotBeDeletedException} instead
 */
@Deprecated
public class CannotBeDeletedException extends ConflictException {

  private final Id objectId;
  private final String reason;

  public CannotBeDeletedException(Id id) {
    super(String.format("'%s' could not be deleted", id));
    this.objectId = id;
    this.reason = null;
  }

  public CannotBeDeletedException(Id id, String reason) {
    super(String.format("'%s' could not be deleted. Reason: %s", id, reason));
    this.objectId = id;
    this.reason = reason;
  }

  public CannotBeDeletedException(Id id, Throwable cause) {
    super(String.format("'%s' could not be deleted. Reason: %s", id, cause.getMessage()), cause);
    this.objectId = id;
    this.reason = cause.getMessage();
  }

  public Id getObjectId() {
    return objectId;
  }

  public String getReason() {
    return reason;
  }
}
