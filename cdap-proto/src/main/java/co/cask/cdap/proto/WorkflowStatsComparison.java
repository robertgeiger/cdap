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
package co.cask.cdap.proto;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Returns detailed statistics per run
 */
public class WorkflowStatsComparison {
  private final Map<String, Long> startTimes;
  private final Collection<ProgramNodes> programNodesList;

  public WorkflowStatsComparison(Map<String, Long> startTimes, Collection<ProgramNodes> programNodesList) {
    this.startTimes = startTimes;
    this.programNodesList = programNodesList;
  }

  public Map<String, Long> getStartTimes() {
    return startTimes;
  }

  public Collection<ProgramNodes> getProgramNodesList() {
    return programNodesList;
  }

  public static class ProgramNodes {
    private final String programName;
    private final List<WorkflowDetails> workflowDetailsList;
    private final ProgramType programType;

    public ProgramNodes(String programName, ProgramType programType, List<WorkflowDetails> workflowDetailsList) {
      this.programName = programName;
      this.programType = programType;
      this.workflowDetailsList = workflowDetailsList;
    }

    public void addWorkflowDetails(String workflowRunId, Map<String, Long> metrics) {
      workflowDetailsList.add(new WorkflowDetails(workflowRunId, metrics));
    }

    public String getProgramName() {
      return programName;
    }

    public List<WorkflowDetails> getWorkflowDetailsList() {
      return workflowDetailsList;
    }

    public ProgramType getProgramType() {
      return programType;
    }

    public static final class WorkflowDetails {
      private final String workflowRunId;
      private final Map<String, Long> metrics;

      public WorkflowDetails(String workflowRunId, Map<String, Long> metrics) {
        this.workflowRunId = workflowRunId;
        this.metrics = metrics;
      }

      public String getWorkflowRunId() {
        return workflowRunId;
      }

      public Map<String, Long> getMetrics() {
        return metrics;
      }
    }
  }
}

