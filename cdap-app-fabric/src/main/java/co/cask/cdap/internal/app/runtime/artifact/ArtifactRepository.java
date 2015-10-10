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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.artifact.ApplicationClass;
import co.cask.cdap.api.artifact.ArtifactClasses;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.common.ArtifactAlreadyExistsException;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.ArtifactRangeNotFoundException;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.common.conf.ArtifactConfig;
import co.cask.cdap.common.conf.ArtifactConfigReader;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ApplicationClassInfo;
import co.cask.cdap.proto.artifact.ApplicationClassSummary;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * This class manages artifact and artifact metadata. It is mainly responsible for inspecting artifacts to determine
 * metadata for the artifact.
 */
public class ArtifactRepository {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactRepository.class);
  private final ArtifactStore artifactStore;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final ArtifactInspector artifactInspector;
  private final File systemArtifactDir;
  private final ArtifactConfigReader configReader;

  @Inject
  ArtifactRepository(CConfiguration cConf, ArtifactStore artifactStore) {
    this.artifactStore = artifactStore;
    File baseUnpackDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.artifactClassLoaderFactory = new ArtifactClassLoaderFactory(cConf, baseUnpackDir);
    this.artifactInspector = new ArtifactInspector(cConf, artifactClassLoaderFactory, baseUnpackDir);
    this.systemArtifactDir = new File(cConf.get(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR));
    this.configReader = new ArtifactConfigReader();
  }

  /**
   * Clear all artifacts in the given namespace. This method is only intended to be called by unit tests, and
   * when a namespace is being deleted.
   *
   * @param namespace the namespace to delete artifacts in.
   * @throws IOException if there was an error making changes in the meta store
   */
  public void clear(Id.Namespace namespace) throws IOException {
    artifactStore.clear(namespace);
  }

  /**
   * Get all artifacts in the given namespace, optionally including system artifacts as well. Will never return
   * null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get artifacts from
   * @param includeSystem whether system artifacts should be included in the results
   * @return an unmodifiable list of artifacts that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  public List<ArtifactSummary> getArtifacts(Id.Namespace namespace, boolean includeSystem) throws IOException {
    List<ArtifactSummary> summaries = Lists.newArrayList();
    if (includeSystem) {
      convertAndAdd(summaries, artifactStore.getArtifacts(Id.Namespace.SYSTEM));
    }
    return Collections.unmodifiableList(convertAndAdd(summaries, artifactStore.getArtifacts(namespace)));
  }

  /**
   * Get all artifacts in the given namespace of the given name. Will never return null.
   * If no artifacts exist, an exception is thrown. Namespace existence is not checked.
   *
   * @param namespace the namespace to get artifacts from
   * @param name the name of artifacts to get
   * @return an unmodifiable list of artifacts in the given namespace of the given name
   * @throws IOException if there as an exception reading from the meta store
   * @throws ArtifactNotFoundException if no artifacts of the given name in the given namespace exist
   */
  public List<ArtifactSummary> getArtifacts(Id.Namespace namespace, String name)
    throws IOException, ArtifactNotFoundException {
    List<ArtifactSummary> summaries = Lists.newArrayList();
    return Collections.unmodifiableList(convertAndAdd(summaries, artifactStore.getArtifacts(namespace, name)));
  }

  /**
   * Get details about the given artifact. Will never return null.
   * If no such artifact exist, an exception is thrown. Namespace existence is not checked.
   *
   * @param artifactId the id of the artifact to get
   * @return details about the given artifact
   * @throws IOException if there as an exception reading from the meta store
   * @throws ArtifactNotFoundException if the given artifact does not exist
   */
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws IOException, ArtifactNotFoundException {
    return artifactStore.getArtifact(artifactId);
  }

  /**
   * Get all application classes in the given namespace, optionally including classes from system artifacts as well.
   * Will never return null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get application classes from
   * @param includeSystem whether classes from system artifacts should be included in the results
   * @return an unmodifiable list of application classes that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  public List<ApplicationClassSummary> getApplicationClasses(Id.Namespace namespace,
                                                             boolean includeSystem) throws IOException {
    List<ApplicationClassSummary> summaries = Lists.newArrayList();
    if (includeSystem) {
      addAppSummaries(summaries, Id.Namespace.SYSTEM);
    }
    addAppSummaries(summaries, namespace);

    return Collections.unmodifiableList(summaries);
  }

  /**
   * Get all application classes in the given namespace of the given class name.
   * Will never return null. If no artifacts exist, an empty list is returned. Namespace existence is not checked.
   *
   * @param namespace the namespace to get application classes from
   * @param className the application class to get
   * @return an unmodifiable list of application classes that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  public List<ApplicationClassInfo> getApplicationClasses(Id.Namespace namespace,
                                                          String className) throws IOException {
    List<ApplicationClassInfo> infos = Lists.newArrayList();
    for (Map.Entry<ArtifactDescriptor, ApplicationClass> entry :
      artifactStore.getApplicationClasses(namespace, className).entrySet()) {
      ArtifactSummary artifactSummary = ArtifactSummary.from(entry.getKey().getArtifactId());
      ApplicationClass appClass = entry.getValue();
      infos.add(new ApplicationClassInfo(artifactSummary, appClass.getClassName(), appClass.getConfigSchema()));
    }
    return Collections.unmodifiableList(infos);
  }

  /**
   * Returns a {@link SortedMap} of plugin artifact to all plugins available for the given artifact. The keys
   * are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins available to the given
   * artifact.
   *
   * @param artifactId the id of the artifact to get plugins for
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   */
  public SortedMap<ArtifactDescriptor, List<PluginClass>> getPlugins(Id.Artifact artifactId)
    throws IOException, ArtifactNotFoundException {
    return artifactStore.getPluginClasses(artifactId);
  }

  /**
   * Returns a {@link SortedMap} of plugin artifact to all plugins of the given type available for the given artifact.
   * The keys are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins available to the given
   * artifact.
   *
   * @param artifactId the id of the artifact to get plugins for
   * @param pluginType the type of plugins to get
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   */
  public SortedMap<ArtifactDescriptor, List<PluginClass>> getPlugins(Id.Artifact artifactId, String pluginType)
    throws IOException, ArtifactNotFoundException {
    return artifactStore.getPluginClasses(artifactId, pluginType);
  }

  /**
   * Returns a {@link SortedMap} of plugin artifact to plugin available for the given artifact. The keys
   * are sorted by the {@link ArtifactDescriptor} for the artifact that contains plugins available to the given
   * artifact.
   *
   * @param artifactId the id of the artifact to get plugins for
   * @param pluginType the type of plugins to get
   * @param pluginName the name of plugins to get
   * @return an unmodifiable sorted map from plugin artifact to plugins in that artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   */
  public SortedMap<ArtifactDescriptor, PluginClass> getPlugins(Id.Artifact artifactId, String pluginType,
                                                               String pluginName)
    throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    return artifactStore.getPluginClasses(artifactId, pluginType, pluginName);
  }

  /**
   * Returns a {@link Map.Entry} representing the plugin information for the plugin being requested.
   *
   * @param artifactId the id of the artifact to get plugins for
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param selector for selecting which plugin to use
   * @return the entry found
   * @throws IOException if there was an exception reading plugin metadata from the artifact store
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws PluginNotExistsException if no plugins of the given type and name are available to the given artifact
   */
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(Id.Artifact artifactId, String pluginType,
                                                               String pluginName, PluginSelector selector)
    throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    SortedMap<ArtifactDescriptor, PluginClass> pluginClasses = artifactStore.getPluginClasses(
      artifactId, pluginType, pluginName);
    SortedMap<ArtifactId, PluginClass> artifactIds = Maps.newTreeMap();
    for (Map.Entry<ArtifactDescriptor, PluginClass> pluginClassEntry : pluginClasses.entrySet()) {
      artifactIds.put(pluginClassEntry.getKey().getArtifactId(), pluginClassEntry.getValue());
    }
    Map.Entry<ArtifactId, PluginClass> chosenArtifact = selector.select(artifactIds);
    if (chosenArtifact == null) {
      throw new PluginNotExistsException(artifactId, pluginType, pluginName);
    }

    for (Map.Entry<ArtifactDescriptor, PluginClass> pluginClassEntry : pluginClasses.entrySet()) {
      if (pluginClassEntry.getKey().getArtifactId().compareTo(chosenArtifact.getKey()) == 0) {
        return pluginClassEntry;
      }
    }
    throw new PluginNotExistsException(artifactId, pluginType, pluginName);
  }

  /**
   * Inspects and builds plugin and application information for the given artifact.
   *
   * @param artifactId the id of the artifact to inspect and store
   * @param artifactFile the artifact to inspect and store
   * @return detail about the newly added artifact
   * @throws IOException if there was an exception reading from the artifact store
   * @throws WriteConflictException if there was a write conflict writing to the ArtifactStore
   * @throws ArtifactAlreadyExistsException if the artifact already exists
   * @throws InvalidArtifactException if the artifact is invalid. For example, if it is not a zip file,
   *                                  or the application class given is not an Application.
   */
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile)
    throws IOException, WriteConflictException, ArtifactAlreadyExistsException, InvalidArtifactException {

    Location artifactLocation = Locations.toLocation(artifactFile);
    try (CloseableClassLoader parentClassLoader = artifactClassLoaderFactory.createClassLoader(artifactLocation)) {
      ArtifactClasses artifactClasses = inspectArtifact(artifactId, artifactFile, null, parentClassLoader);
      validatePluginSet(artifactClasses.getPlugins());
      ArtifactMeta meta = new ArtifactMeta(artifactClasses, ImmutableSet.<ArtifactRange>of());
      return artifactStore.write(artifactId, meta, Files.newInputStreamSupplier(artifactFile));
    }
  }

  /**
   * Inspects and builds plugin and application information for the given artifact.
   *
   * @param artifactId the id of the artifact to inspect and store
   * @param artifactFile the artifact to inspect and store
   * @param parentArtifacts artifacts the given artifact extends.
   *                        If null, the given artifact does not extend another artifact
   * @throws IOException if there was an exception reading from the artifact store
   * @throws ArtifactRangeNotFoundException if none of the parent artifacts could be found
   * @throws WriteConflictException if there was a write conflict writing to the metatable. Should not happen often,
   *                                and it should be possible to retry the operation if it occurs.
   * @throws ArtifactAlreadyExistsException if the artifact already exists and is not a snapshot version
   * @throws InvalidArtifactException if the artifact is invalid. Can happen if it is not a zip file,
   *                                  if the application class given is not an Application,
   *                                  or if it has parents that also have parents.
   */
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts)
    throws IOException, ArtifactRangeNotFoundException, WriteConflictException,
    ArtifactAlreadyExistsException, InvalidArtifactException {

    return addArtifact(artifactId, artifactFile, parentArtifacts, null);
  }

  /**
   * Inspects and builds plugin and application information for the given artifact, adding an additional set of
   * plugin classes to the plugins found through inspection. This method is used when all plugin classes
   * cannot be derived by inspecting the artifact but need to be explicitly set. This is true for 3rd party plugins
   * like jdbc drivers.
   *
   * @param artifactId the id of the artifact to inspect and store
   * @param artifactFile the artifact to inspect and store
   * @param parentArtifacts artifacts the given artifact extends.
   *                        If null, the given artifact does not extend another artifact
   * @param additionalPlugins the set of additional plugin classes to add to the plugins found through inspection.
   *                          If null, no additional plugin classes will be added
   * @throws IOException if there was an exception reading from the artifact store
   * @throws ArtifactRangeNotFoundException if none of the parent artifacts could be found
   */
  public ArtifactDetail addArtifact(Id.Artifact artifactId, File artifactFile,
                                    @Nullable Set<ArtifactRange> parentArtifacts,
                                    @Nullable Set<PluginClass> additionalPlugins)
    throws IOException, ArtifactAlreadyExistsException, WriteConflictException,
    InvalidArtifactException, ArtifactRangeNotFoundException {

    if (additionalPlugins != null) {
      validatePluginSet(additionalPlugins);
    }

    parentArtifacts = parentArtifacts == null ? Collections.<ArtifactRange>emptySet() : parentArtifacts;
    CloseableClassLoader parentClassLoader;
    if (parentArtifacts.isEmpty()) {
      parentClassLoader =
        artifactClassLoaderFactory.createClassLoader(Locations.toLocation(artifactFile));
    } else {
      validateParentSet(artifactId, parentArtifacts);
      parentClassLoader = createParentClassLoader(artifactId, parentArtifacts);
    }

    try {
      ArtifactClasses artifactClasses = inspectArtifact(artifactId, artifactFile, additionalPlugins, parentClassLoader);
      ArtifactMeta meta = new ArtifactMeta(artifactClasses, parentArtifacts);
      return artifactStore.write(artifactId, meta, Files.newInputStreamSupplier(artifactFile));
    } finally {
      parentClassLoader.close();
    }
  }

  private ArtifactClasses inspectArtifact(Id.Artifact artifactId, File artifactFile,
                                          @Nullable Set<PluginClass> additionalPlugins,
                                          ClassLoader parentClassLoader) throws IOException, InvalidArtifactException {
    ArtifactClasses artifactClasses = artifactInspector.inspectArtifact(artifactId, artifactFile, parentClassLoader);
    validatePluginSet(artifactClasses.getPlugins());
    if (additionalPlugins == null || additionalPlugins.isEmpty()) {
      return artifactClasses;
    } else {
      return ArtifactClasses.builder()
        .addApps(artifactClasses.getApps())
        .addPlugins(artifactClasses.getPlugins())
        .addPlugins(additionalPlugins)
        .build();
    }
  }

  /**
   * Scan all files in the local system artifact directory, looking for jar files and adding them as system artifacts.
   * If the artifact already exists it will not be added again unless it is a snapshot version.
   *
   * @throws IOException if there was some IO error adding the system artifacts
   * @throws WriteConflictException if there was a write conflicting adding the system artifact. This shouldn't happen,
   *                                but if it does, it should be ok to retry the operation.
   */
  public void addSystemArtifacts() throws IOException, WriteConflictException {

    // scan the directory for artifact .jar files and config files for those artifacts
    List<SystemArtifactInfo> systemArtifacts = new ArrayList<>();
    for (File jarFile : DirUtils.listFiles(systemArtifactDir, "jar")) {
      // parse id from filename
      Id.Artifact artifactId;
      try {
        artifactId = Id.Artifact.parse(Id.Namespace.SYSTEM, jarFile.getName());
      } catch (IllegalArgumentException e) {
        LOG.warn(String.format("Skipping system artifact '%s' because the name is invalid: ", e.getMessage()));
        continue;
      }

      // check for a corresponding .json config file
      String artifactFileName = jarFile.getName();
      String configFileName = artifactFileName.substring(0, artifactFileName.length() - ".jar".length()) + ".json";
      File configFile = new File(systemArtifactDir, configFileName);

      try {
        // read and parse the config file if it exists. Otherwise use an empty config with the artifact filename
        ArtifactConfig artifactConfig = configFile.isFile() ?
          configReader.read(artifactId.getNamespace(), configFile) : new ArtifactConfig();

        validateParentSet(artifactId, artifactConfig.getParents());
        validatePluginSet(artifactConfig.getPlugins());
        systemArtifacts.add(new SystemArtifactInfo(artifactId, jarFile, artifactConfig));
      } catch (InvalidArtifactException e) {
        LOG.warn(String.format("Could not add system artifact '%s' because it is invalid.", artifactFileName), e);
      }
    }

    // taking advantage of the fact that we only have 1 level of dependencies
    // so we can add all the parents first, then we know its safe to add everything else
    // add all parents
    Set<Id.Artifact> parents = new HashSet<>();
    for (SystemArtifactInfo child : systemArtifacts) {
      Id.Artifact childId = child.getArtifactId();

      for (SystemArtifactInfo potentialParent : systemArtifacts) {
        Id.Artifact potentialParentId = potentialParent.getArtifactId();
        // skip if we're looking at ourselves
        if (childId.equals(potentialParentId)) {
          continue;
        }

        if (child.getConfig().hasParent(potentialParentId)) {
          parents.add(potentialParentId);
        }
      }
    }

    // add all parents first
    for (SystemArtifactInfo systemArtifact : systemArtifacts) {
      if (parents.contains(systemArtifact.getArtifactId())) {
        addSystemArtifact(systemArtifact);
      }
    }

    // add children next
    for (SystemArtifactInfo systemArtifact : systemArtifacts) {
      if (!parents.contains(systemArtifact.getArtifactId())) {
        addSystemArtifact(systemArtifact);
      }
    }
  }

  private void addSystemArtifact(SystemArtifactInfo systemArtifactInfo) throws IOException, WriteConflictException {
    String fileName = systemArtifactInfo.getArtifactFile().getName();
    try {
      Id.Artifact artifactId = systemArtifactInfo.getArtifactId();

      // if it's not a snapshot and it already exists, don't bother trying to add it since artifacts are immutable
      if (!artifactId.getVersion().isSnapshot()) {
        try {
          artifactStore.getArtifact(artifactId);
          return;
        } catch (ArtifactNotFoundException e) {
          // this is fine, means it doesn't exist yet and we should add it
        }
      }

      addArtifact(artifactId,
        systemArtifactInfo.getArtifactFile(),
        systemArtifactInfo.getConfig().getParents(),
        systemArtifactInfo.getConfig().getPlugins());
    } catch (ArtifactAlreadyExistsException e) {
      // shouldn't happen... but if it does for some reason it's fine, it means it was added some other way already.
    } catch (ArtifactRangeNotFoundException e) {
      LOG.warn(String.format("Could not add system artifact '%s' because it extends artifacts that do not exist.",
        fileName), e);
    } catch (InvalidArtifactException e) {
      LOG.warn(String.format("Could not add system artifact '%s' because it is invalid.", fileName), e);
    }
  }

  /**
   * Delete the specified artifact. Programs that use the artifact will not be able to start.
   *
   * @param artifactId the artifact to delete
   * @throws IOException if there was some IO error deleting the artifact
   */
  public void deleteArtifact(Id.Artifact artifactId) throws IOException {
    artifactStore.delete(artifactId);
  }

  // convert details to summaries (to hide location and other unnecessary information)
  private List<ArtifactSummary> convertAndAdd(List<ArtifactSummary> summaries, Iterable<ArtifactDetail> details) {
    for (ArtifactDetail detail : details) {
      summaries.add(ArtifactSummary.from(detail.getDescriptor().getArtifactId()));
    }
    return summaries;
  }

  /**
   * Create a parent classloader using an artifact from one of the artifacts in the specified parents.
   *
   * @param artifactId the id of the artifact to create the parent classloader for
   * @param parentArtifacts the ranges of parents to create the classloader from
   * @return a classloader based off a parent artifact
   * @throws ArtifactRangeNotFoundException if none of the parents could be found
   * @throws InvalidArtifactException if one of the parents also has parents
   * @throws IOException if there was some error reading from the store
   */
  private CloseableClassLoader createParentClassLoader(Id.Artifact artifactId, Set<ArtifactRange> parentArtifacts)
    throws ArtifactRangeNotFoundException, IOException, InvalidArtifactException {

    List<ArtifactDetail> parents = new ArrayList<>();
    for (ArtifactRange parentRange : parentArtifacts) {
      parents.addAll(artifactStore.getArtifacts(parentRange));
    }

    if (parents.isEmpty()) {
      throw new ArtifactRangeNotFoundException(String.format("Artifact %s extends artifacts '%s' that do not exist",
        artifactId, Joiner.on('/').join(parentArtifacts)));
    }

    // check if any of the parents also have parents, which is not allowed. This is to simplify things
    // so that we don't have to chain a bunch of classloaders, and also to keep it simple for users to avoid
    // complicated dependency trees that are hard to manage.
    boolean isInvalid = false;
    StringBuilder errMsg = new StringBuilder("Invalid artifact '")
      .append(artifactId)
      .append("'.")
      .append(" Artifact parents cannot have parents.");
    for (ArtifactDetail parent : parents) {
      Set<ArtifactRange> grandparents = parent.getMeta().getUsableBy();
      if (!grandparents.isEmpty()) {
        isInvalid = true;
        errMsg
          .append(" Parent '")
          .append(parent.getDescriptor().getArtifactId().getName())
          .append("-")
          .append(parent.getDescriptor().getArtifactId().getVersion().getVersion())
          .append("' has parents.");
      }
    }
    if (isInvalid) {
      throw new InvalidArtifactException(errMsg.toString());
    }

    // assumes any of the parents will do
    Location parentLocation = parents.get(0).getDescriptor().getLocation();

    return artifactClassLoaderFactory.createClassLoader(parentLocation);
  }

  private void addAppSummaries(List<ApplicationClassSummary> summaries, Id.Namespace namespace) {
    for (Map.Entry<ArtifactDescriptor, List<ApplicationClass>> classInfo :
      artifactStore.getApplicationClasses(namespace).entrySet()) {
      ArtifactSummary artifactSummary = ArtifactSummary.from(classInfo.getKey().getArtifactId());

      for (ApplicationClass appClass : classInfo.getValue()) {
        summaries.add(new ApplicationClassSummary(artifactSummary, appClass.getClassName()));
      }
    }
  }

  /**
   * Validates the parents of an artifact. Checks that each artifact only appears with a single version range.
   *
   * @param parents the set of parent ranges to validate
   * @throws InvalidArtifactException if there is more than one version range for an artifact
   */
  @VisibleForTesting
  static void validateParentSet(Id.Artifact artifactId, Set<ArtifactRange> parents) throws InvalidArtifactException {
    boolean isInvalid = false;
    StringBuilder errMsg = new StringBuilder("Invalid parents field.");

    // check for multiple version ranges for the same artifact.
    // ex: "parents": [ "etlbatch[1.0.0,2.0.0)", "etlbatch[3.0.0,4.0.0)" ]
    Set<String> parentNames = new HashSet<>();
    // keep track of dupes so that we don't have repeat error messages if there are more than 2 ranges for a name
    Set<String> dupes = new HashSet<>();
    for (ArtifactRange parent : parents) {
      String parentName = parent.getName();
      if (!parentNames.add(parentName) && !dupes.contains(parentName)) {
        errMsg.append(" Only one version range for parent '");
        errMsg.append(parentName);
        errMsg.append("' can be present.");
        dupes.add(parentName);
        isInvalid = true;
      }
      if (artifactId.getName().equals(parentName) && artifactId.getNamespace().equals(parent.getNamespace())) {
        throw new InvalidArtifactException(String.format(
          "Invalid parent '%s' for artifact '%s'. An artifact cannot extend itself.", parent, artifactId));
      }
    }

    // final err message should look something like:
    // "Invalid parents. Only one version range for parent 'etlbatch' can be present."
    if (isInvalid) {
      throw new InvalidArtifactException(errMsg.toString());
    }
  }

  /**
   * Validates the set of plugins for an artifact. Checks that the pair of plugin type and name are unique among
   * all plugins in an artifact.
   *
   * @param plugins the set of plugins to validate
   * @throws InvalidArtifactException if there is more than one class with the same type and name
   */
  @VisibleForTesting
  static void validatePluginSet(Set<PluginClass> plugins) throws InvalidArtifactException {
    boolean isInvalid = false;
    StringBuilder errMsg = new StringBuilder("Invalid plugins field.");
    Set<ImmutablePair<String, String>> existingPlugins = new HashSet<>();
    Set<ImmutablePair<String, String>> dupes = new HashSet<>();
    for (PluginClass plugin : plugins) {
      ImmutablePair<String, String> typeAndName = ImmutablePair.of(plugin.getType(), plugin.getName());
      if (!existingPlugins.add(typeAndName) && !dupes.contains(typeAndName)) {
        errMsg.append(" Only one plugin with type '");
        errMsg.append(typeAndName.getFirst());
        errMsg.append("' and name '");
        errMsg.append(typeAndName.getSecond());
        errMsg.append("' can be present.");
        dupes.add(typeAndName);
        isInvalid = true;
      }
    }

    // final err message should look something like:
    // "Invalid plugins. Only one plugin with type 'source' and name 'table' can be present."
    if (isInvalid) {
      throw new InvalidArtifactException(errMsg.toString());
    }
  }
}
