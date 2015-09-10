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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * Tests lineage computation.
 */
public class LineageServiceTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Test
  public void testSimpleLineage() throws Exception {
    // Lineage for D3 -> P2 -> D2 -> P1 -> D1

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testSimpleLineage"));
    LineageService lineageService = new LineageService(lineageStore);

    // Define a run
    Id.DatasetInstance dataset1 = Id.DatasetInstance.from("default", "dataset1");
    Id.DatasetInstance dataset2 = Id.DatasetInstance.from("default", "dataset2");
    Id.Program program1 = Id.Program.from("default", "app1", ProgramType.FLOW, "flow1");
    Id.Flow.Flowlet flowlet1 = Id.Flow.Flowlet.from(program1.getApplication(), program1.getId(), "flowlet1");
    Id.Run run1 = new Id.Run(program1, RunIds.generate(10000).getId());

    // Define another run
    Id.DatasetInstance dataset3 = Id.DatasetInstance.from("default", "dataset3");
    Id.Program program2 = Id.Program.from("default", "app2", ProgramType.FLOW, "flow2");
    Id.Flow.Flowlet flowlet2 = Id.Flow.Flowlet.from(program2.getApplication(), program2.getId(), "flowlet2");
    Id.Run run2 = new Id.Run(program2, RunIds.generate(900).getId());

    // Add accesses for D3 -> P2 -> D2 -> P1 -> D1
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, "metadata11", flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.READ, "metadata12", flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.WRITE, "metadata21", flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.READ, "metadata22", flowlet2);

    // Lineage for D1 should be D3 -> P2 -> D2 -> P1 -> D1
    Lineage lineage = lineageService.computeLineage(dataset1, 500, 20000, 100);
    Assert.assertNotNull(lineage);

    Assert.assertEquals(ImmutableSet.of(program1, program2), lineage.getPrograms());
    Assert.assertEquals(ImmutableSet.of(dataset1, dataset2, dataset3), lineage.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2))
      ),
      lineage.getRelations());

    // Lineage for D2 should be D3 -> P2 -> D2
    Lineage lineage2 = lineageService.computeLineage(dataset2, 500, 20000, 100);
    Assert.assertNotNull(lineage2);

    Assert.assertEquals(ImmutableSet.of(program2), lineage2.getPrograms());
    Assert.assertEquals(ImmutableSet.of(dataset2, dataset3), lineage2.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset2, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2))
      ),
      lineage2.getRelations());

    // Lineage for D1 for one level should be D2 -> P1 -> D1
    Lineage lineage3 = lineageService.computeLineage(dataset1, 500, 20000, 1);
    Assert.assertNotNull(lineage3);

    Assert.assertEquals(ImmutableSet.of(program1), lineage3.getPrograms());
    Assert.assertEquals(ImmutableSet.of(dataset1, dataset2), lineage3.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1))
      ),
      lineage3.getRelations());
  }

  @Test
  public void testSimpleLoopLineage() throws Exception {
    // Lineage for D1 -> P2 -> D2 -> P1 -> D1

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testSimpleLoopLineage"));
    LineageService lineageService = new LineageService(lineageStore);

    // Define a run
    Id.DatasetInstance dataset1 = Id.DatasetInstance.from("default", "dataset1");
    Id.DatasetInstance dataset2 = Id.DatasetInstance.from("default", "dataset2");
    Id.Program program1 = Id.Program.from("default", "app1", ProgramType.FLOW, "flow1");
    Id.Flow.Flowlet flowlet1 = Id.Flow.Flowlet.from(program1.getApplication(), program1.getId(), "flowlet1");
    Id.Run run1 = new Id.Run(program1, RunIds.generate(10000).getId());

    // Define another run
    Id.Program program2 = Id.Program.from("default", "app2", ProgramType.FLOW, "flow2");
    Id.Flow.Flowlet flowlet2 = Id.Flow.Flowlet.from(program2.getApplication(), program2.getId(), "flowlet2");
    Id.Run run2 = new Id.Run(program2, RunIds.generate(900).getId());

    // Add accesses for D1 -> P2 -> D2 -> P1 -> D1
    lineageStore.addAccess(run1, dataset1, AccessType.WRITE, "metadata11", flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.READ, "metadata12", flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.WRITE, "metadata21", flowlet2);
    lineageStore.addAccess(run2, dataset1, AccessType.READ, "metadata22", flowlet2);

    // Lineage for D1 should be D1 -> P2 -> D2 -> P1 -> D1
    Lineage lineage = lineageService.computeLineage(dataset1, 500, 20000, 100);
    Assert.assertNotNull(lineage);

    Assert.assertEquals(ImmutableSet.of(program1, program2), lineage.getPrograms());
    Assert.assertEquals(ImmutableSet.of(dataset1, dataset2), lineage.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset1, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2))
      ),
      lineage.getRelations());

    // Lineage for D2 should be P1 -> D1 -> P2 -> D2
    Lineage lineage2 = lineageService.computeLineage(dataset2, 500, 20000, 100);
    Assert.assertNotNull(lineage2);

    Assert.assertEquals(ImmutableSet.of(program1, program2), lineage2.getPrograms());
    Assert.assertEquals(ImmutableSet.of(dataset1, dataset2), lineage2.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset1, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2))
      ),
      lineage2.getRelations());

    // Lineage for D1 for one level should be D2 -> P1 -> D1
    Lineage lineage3 = lineageService.computeLineage(dataset1, 500, 20000, 1);
    Assert.assertNotNull(lineage3);

    Assert.assertEquals(ImmutableSet.of(program1), lineage3.getPrograms());
    Assert.assertEquals(ImmutableSet.of(dataset1, dataset2), lineage3.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset1, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1))
      ),
      lineage3.getRelations());
  }

  @Test
  public void testBranchLineage() throws Exception {
    // Lineage for:
    //
    //       ->D4        -> D5 -> P3 -> D6
    //       |           |
    //       |           |
    // D1 -> P1 -> D2 -> P2 -> D3
    //       |     |           |
    //       |     |           |
    // D0 -->|     ---------------> P4 -> D7

    LineageStore lineageStore = new LineageStore(getTxExecFactory(), dsFrameworkUtil.getFramework(),
                                                 Id.DatasetInstance.from("default", "testBranchLineage"));
    LineageService lineageService = new LineageService(lineageStore);

    // Define datasets
    Id.DatasetInstance dataset0 = Id.DatasetInstance.from("default", "dataset0");
    Id.DatasetInstance dataset1 = Id.DatasetInstance.from("default", "dataset1");
    Id.DatasetInstance dataset2 = Id.DatasetInstance.from("default", "dataset2");
    Id.DatasetInstance dataset3 = Id.DatasetInstance.from("default", "dataset3");
    Id.DatasetInstance dataset4 = Id.DatasetInstance.from("default", "dataset4");
    Id.DatasetInstance dataset5 = Id.DatasetInstance.from("default", "dataset5");
    Id.DatasetInstance dataset6 = Id.DatasetInstance.from("default", "dataset6");
    Id.DatasetInstance dataset7 = Id.DatasetInstance.from("default", "dataset7");

    // Define programs and runs
    Id.Program program1 = Id.Program.from("default", "app1", ProgramType.FLOW, "flow1");
    Id.Flow.Flowlet flowlet1 = Id.Flow.Flowlet.from(program1.getApplication(), program1.getId(), "flowlet1");
    Id.Run run1 = new Id.Run(program1, RunIds.generate(10000).getId());

    Id.Program program2 = Id.Program.from("default", "app2", ProgramType.FLOW, "flow2");
    Id.Flow.Flowlet flowlet2 = Id.Flow.Flowlet.from(program2.getApplication(), program2.getId(), "flowlet2");
    Id.Run run2 = new Id.Run(program2, RunIds.generate(900).getId());

    Id.Program program3 = Id.Program.from("default", "app3", ProgramType.WORKER, "worker3");
    Id.Run run3 = new Id.Run(program3, RunIds.generate(800).getId());

    Id.Program program4 = Id.Program.from("default", "app4", ProgramType.SERVICE, "service4");
    Id.Run run4 = new Id.Run(program4, RunIds.generate(800).getId());

    // Add accesses
    lineageStore.addAccess(run1, dataset0, AccessType.READ, "metadata10", flowlet1);
    lineageStore.addAccess(run1, dataset1, AccessType.READ, "metadata11", flowlet1);
    lineageStore.addAccess(run1, dataset2, AccessType.WRITE, "metadata12", flowlet1);
    lineageStore.addAccess(run1, dataset4, AccessType.WRITE, "metadata14", flowlet1);

    lineageStore.addAccess(run2, dataset2, AccessType.READ, "metadata22", flowlet2);
    lineageStore.addAccess(run2, dataset3, AccessType.WRITE, "metadata23", flowlet2);
    lineageStore.addAccess(run2, dataset5, AccessType.WRITE, "metadata25", flowlet2);

    lineageStore.addAccess(run3, dataset5, AccessType.READ, "metadata35");
    lineageStore.addAccess(run3, dataset6, AccessType.WRITE, "metadata36");

    lineageStore.addAccess(run4, dataset2, AccessType.READ, "metadata42");
    lineageStore.addAccess(run4, dataset3, AccessType.READ, "metadata43");
    lineageStore.addAccess(run4, dataset7, AccessType.WRITE, "metadata47");

    // Lineage for D7 should be D1 -> P1 -> D2 -> P2 -> D3
    //                                |     |           |
    //                           D0 ->|      -------------> P4 -> D7
    //
    Lineage lineage = lineageService.computeLineage(dataset7, 500, 20000, 100);
    Assert.assertNotNull(lineage);

    Assert.assertEquals(ImmutableSet.of(program1, program2, program4), lineage.getPrograms());
    Assert.assertEquals(ImmutableSet.of(dataset0, dataset1, dataset2, dataset3, dataset7), lineage.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset0, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset3, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset2, program4, AccessType.READ, toSet(twillRunId(run4)), emptySet()),
        new Relation(dataset3, program4, AccessType.READ, toSet(twillRunId(run4)), emptySet()),
        new Relation(dataset7, program4, AccessType.WRITE, toSet(twillRunId(run4)), emptySet())
      ),
      lineage.getRelations());

    // Lineage for D6 should be D1 -> P1 -> D2 -> P2 -> D5 -> P3 -> D6
    //                                |
    //                                |
    //                           D0 ->|
    //
    lineage = lineageService.computeLineage(dataset6, 500, 20000, 100);
    Assert.assertNotNull(lineage);

    Assert.assertEquals(ImmutableSet.of(program1, program2, program3), lineage.getPrograms());
    Assert.assertEquals(ImmutableSet.of(dataset0, dataset1, dataset2, dataset5, dataset6), lineage.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset0, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program2, AccessType.READ, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset5, program2, AccessType.WRITE, toSet(twillRunId(run2)), toSet(flowlet2)),
        new Relation(dataset5, program3, AccessType.READ, toSet(twillRunId(run3)), emptySet()),
        new Relation(dataset6, program3, AccessType.WRITE, toSet(twillRunId(run3)), emptySet())
      ),
      lineage.getRelations());

    // Lineage for D2 should be D1 -> P1 -> D2
    //                                |
    //                                |
    //                           D0 ->|
    //
    lineage = lineageService.computeLineage(dataset2, 500, 20000, 100);
    Assert.assertNotNull(lineage);

    Assert.assertEquals(ImmutableSet.of(program1), lineage.getPrograms());
    Assert.assertEquals(ImmutableSet.of(dataset0, dataset1, dataset2), lineage.getData());

    Assert.assertEquals(
      ImmutableSet.of(
        new Relation(dataset0, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset1, program1, AccessType.READ, toSet(twillRunId(run1)), toSet(flowlet1)),
        new Relation(dataset2, program1, AccessType.WRITE, toSet(twillRunId(run1)), toSet(flowlet1))
      ),
      lineage.getRelations());
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.copyOf(elements);
  }

  private static Set<Id.NamespacedId> emptySet() {
    return Collections.emptySet();
  }

  private RunId twillRunId(Id.Run run) {
    return RunIds.fromString(run.getId());
  }

  private TransactionExecutorFactory getTxExecFactory() {
    return dsFrameworkUtil.getInjector().getInstance(TransactionExecutorFactory.class);
  }
}
