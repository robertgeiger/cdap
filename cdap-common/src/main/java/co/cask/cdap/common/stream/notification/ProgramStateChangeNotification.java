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

package co.cask.cdap.common.stream.notification;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;

/**
 *
 */
public class ProgramStateChangeNotification {
  private final Id.Program program;
  private final ProgramRunStatus status;

  public ProgramStateChangeNotification(Id.Program program, ProgramRunStatus status) {
    this.program = program;
    this.status = status;
  }

  public Id.Program getProgram() {
    return program;
  }

  public ProgramRunStatus getStatus() {
    return status;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ProgramStateChangeNotification{");
    sb.append("program=").append(program);
    sb.append(", status=").append(status);
    sb.append('}');
    return sb.toString();
  }
}
