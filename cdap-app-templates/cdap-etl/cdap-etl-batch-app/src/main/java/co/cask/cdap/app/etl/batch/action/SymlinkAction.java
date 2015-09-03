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

package co.cask.cdap.app.etl.batch.action;

import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.template.etl.common.ETLStage;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;

/**
 * This class will only work in distributed, as it only works with HDFS.
 */
public class SymlinkAction extends AbstractWorkflowAction {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SymlinkAction.class);

  private static final String NAME = "Symlink";

  public static final String SYMLINK_PATH = "symbolic.link.path";
  public static final String DEFAULT_URI = "default.uri";

  private Map<String, String> properties;

  public SymlinkAction(ETLStage action) {
    super(action.getName());
    properties = action.getProperties();
  }

  @Override
  public WorkflowActionSpecification configure() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(SYMLINK_PATH)),
                                String.format("You must set the \'%s\' property to create a symbolic link.",
                                              SYMLINK_PATH));
    return WorkflowActionSpecification.Builder.with().setName(NAME).setDescription("")
      .withOptions(properties).build();
  }

  @Override
  public void run() {
    try {
      getContext().getRuntimeArguments();
      FileContext fileContext;
      if (Strings.isNullOrEmpty(properties.get(DEFAULT_URI))) {
        fileContext = FileContext.getFileContext();
      } else {
        fileContext = FileContext.getFileContext(new URI(properties.get(DEFAULT_URI)));
      }
      Map<String, String> arguments = getContext().getRuntimeArguments();
      fileContext.createSymlink(new Path(properties.get(SYMLINK_PATH)),
                                new Path(FileSetArguments.getOutputPath(arguments)), true);
    } catch (Exception e) {
      throw new RuntimeException("Error creating symbolic link: " + e);
    }
    throw new RuntimeException("THIS WORKED!!!!");
  }
}
