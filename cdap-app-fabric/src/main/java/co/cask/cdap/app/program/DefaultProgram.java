/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.app.program;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.twill.filesystem.Location;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Default implementation of program.
 */
public final class DefaultProgram implements Program {

  private final CConfiguration cConf;
  private final String mainClassName;
  private final ProgramType processorType;

  private final Id.Program id;

  private final Location programJarLocation;
  private final File expandFolder;
  private final ClassLoader parentClassLoader;
  private final File specFile;
  private boolean expanded;
  private ClassLoader classLoader;
  private ApplicationSpecification specification;

  /**
   * Creates a program instance.
   *
   * @param programJarLocation Location of the program jar file.
   * @param expandFolder Local directory for expanding the jar file into. If it is {@code null},
   *                     the {@link #getClassLoader()} methods would throw exception.
   * @param parentClassLoader Parent classloader for the program class.
   */
  DefaultProgram(Location programJarLocation, @Nullable CConfiguration cConf,
                 @Nullable File expandFolder, @Nullable ClassLoader parentClassLoader) throws IOException {
    this.programJarLocation = programJarLocation;
    this.cConf = cConf;
    this.expandFolder = expandFolder;
    this.parentClassLoader = parentClassLoader;

    Manifest manifest = BundleJarUtil.getManifest(programJarLocation);
    if (manifest == null) {
      throw new IOException("Failed to load manifest in program jar from " + programJarLocation.toURI());
    }

    mainClassName = getAttribute(manifest, ManifestFields.MAIN_CLASS);
    id = Id.Program.from(getAttribute(manifest, ManifestFields.ACCOUNT_ID),
                         getAttribute(manifest, ManifestFields.APPLICATION_ID),
                         ProgramType.valueOf(getAttribute(manifest, ManifestFields.PROGRAM_TYPE)),
                         getAttribute(manifest, ManifestFields.PROGRAM_NAME));

    this.processorType = ProgramType.valueOfPrettyName(getAttribute(manifest, ManifestFields.PROGRAM_TYPE));

    // Load the app spec from the jar file if no expand folder is provided. Otherwise do lazy loading after the jar
    // is expanded.
    String specPath = getAttribute(manifest, ManifestFields.SPEC_FILE);
    if (expandFolder == null) {
      specification = ApplicationSpecificationAdapter.create().fromJson(
        CharStreams.newReaderSupplier(BundleJarUtil.getEntry(programJarLocation, specPath), Charsets.UTF_8));
      specFile = null;
    } else {
      specFile = new File(expandFolder, specPath);
    }
  }

  DefaultProgram(Location programJarLocation, ClassLoader classLoader) throws IOException {
    this(programJarLocation, null, null, null);
    this.classLoader = classLoader;
  }

  @Override
  public String getMainClassName() {
    return mainClassName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Class<T> getMainClass() throws ClassNotFoundException {
    return (Class<T>) getClassLoader().loadClass(mainClassName);
  }

  @Override
  public ProgramType getType() {
    return processorType;
  }

  @Override
  public Id.Program getId() {
    return id;
  }

  @Override
  public String getName() {
    return id.getId();
  }

  @Override
  public String getNamespaceId() {
    return id.getNamespaceId();
  }

  @Override
  public String getApplicationId() {
    return id.getApplicationId();
  }

  @Override
  public synchronized ApplicationSpecification getApplicationSpecification() {
    if (specification == null) {
      expandIfNeeded();
      try {
        specification = ApplicationSpecificationAdapter.create().fromJson(
          CharStreams.newReaderSupplier(Files.newInputStreamSupplier(specFile), Charsets.UTF_8));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return specification;
  }

  @Override
  public Location getJarLocation() {
    return programJarLocation;
  }

  @Override
  public synchronized ClassLoader getClassLoader() {
    if (classLoader == null) {
      // The following precondition should always pass.
      // cConf can only be null if a program class loader is already provided through the constructor.
      Preconditions.checkNotNull(cConf, "CConfiguration cannot be null.");
      expandIfNeeded();
      try {
        classLoader = ProgramClassLoader.create(cConf, expandFolder, parentClassLoader, processorType);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return classLoader;
  }

  private String getAttribute(Manifest manifest, Attributes.Name name) throws IOException {
    String value = manifest.getMainAttributes().getValue(name);
    check(value != null, "Fail to get %s attribute from jar", name);
    return value;
  }

  private void check(boolean condition, String fmt, Object... objs) throws IOException {
    if (!condition) {
      throw new IOException(String.format(fmt, objs));
    }
  }

  private synchronized void expandIfNeeded() {
    if (expanded) {
      return;
    }

    Preconditions.checkState(expandFolder != null, "Directory for jar expansion is not defined.");

    try {
      BundleJarUtil.unpackProgramJar(programJarLocation, expandFolder);
      expanded = true;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (expanded && classLoader instanceof Closeable) {
      Closeables.closeQuietly((Closeable) classLoader);
    }
  }
}
