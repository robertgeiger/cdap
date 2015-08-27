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

package co.cask.cdap.explore.service;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.explore.service.hive.Hive12CDH5ExploreService;
import co.cask.cdap.explore.service.hive.Hive12ExploreService;
import co.cask.cdap.explore.service.hive.Hive13ExploreService;
import co.cask.cdap.explore.service.hive.Hive14ExploreService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.internal.utils.Dependencies;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;

/**
 * Utility class for the explore service.
 */
public class ExploreServiceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreServiceUtils.class);

  private static final String HIVEAUTHFACTORY_CLASS = "org.apache.hive.service.auth.HiveAuthFactory";

  /**
   * Hive support enum.
   */
  public enum HiveSupport {
    // The order of the enum values below is very important
    // CDH 5.0 to 5.1 uses Hive 0.12
    HIVE_CDH5_0(Pattern.compile("^.*cdh5.0\\..*$"), Hive12CDH5ExploreService.class),
    HIVE_CDH5_1(Pattern.compile("^.*cdh5.1\\..*$"), Hive12CDH5ExploreService.class),
    // CDH 5.2.x and 5.3.x use Hive 0.13
    HIVE_CDH5_2(Pattern.compile("^.*cdh5.2\\..*$"), Hive13ExploreService.class),
    HIVE_CDH5_3(Pattern.compile("^.*cdh5.3\\..*$"), Hive13ExploreService.class),
    // CDH > 5.3 uses Hive >= 1.1 (which Hive14ExploreService supports)
    HIVE_CDH5(Pattern.compile("^.*cdh5\\..*$"), Hive14ExploreService.class),

    HIVE_12(null, Hive12ExploreService.class),
    HIVE_13(null, Hive13ExploreService.class),
    HIVE_14(null, Hive14ExploreService.class),
    HIVE_1_0(null, Hive14ExploreService.class),
    HIVE_1_1(null, Hive14ExploreService.class);

    private final Pattern hadoopVersionPattern;
    private final Class<? extends ExploreService> hiveExploreServiceClass;

    HiveSupport(Pattern hadoopVersionPattern, Class<? extends ExploreService> hiveExploreServiceClass) {
      this.hadoopVersionPattern = hadoopVersionPattern;
      this.hiveExploreServiceClass = hiveExploreServiceClass;
    }

    public Pattern getHadoopVersionPattern() {
      return hadoopVersionPattern;
    }

    public Class<? extends ExploreService> getHiveExploreServiceClass() {
      return hiveExploreServiceClass;
    }
  }

  // Caching the dependencies so that we don't trace them twice
  private static Set<File> exploreDependencies = null;
  // Caching explore class loader
  private static ClassLoader exploreClassLoader = null;

  private static final Pattern HIVE_SITE_FILE_PATTERN = Pattern.compile("^.*/hive-site\\.xml$");
  private static final Pattern YARN_SITE_FILE_PATTERN = Pattern.compile("^.*/yarn-site\\.xml$");
  private static final Pattern MAPRED_SITE_FILE_PATTERN = Pattern.compile("^.*/mapred-site\\.xml$");

  /**
   * Get all the files contained in a class path.
   */
  public static Iterable<File> getClassPathJarsFiles(String hiveClassPath) {
    if (hiveClassPath == null) {
      return null;
    }
    return Iterables.transform(Splitter.on(':').split(hiveClassPath), STRING_FILE_FUNCTION);
  }

  private static final Function<String, File> STRING_FILE_FUNCTION =
    new Function<String, File>() {
      @Override
      public File apply(String input) {
        return new File(input).getAbsoluteFile();
      }
    };

  /**
   * Builds a class loader with the class path provided.
   */
  public static ClassLoader getExploreClassLoader() {
    if (exploreClassLoader != null) {
      return exploreClassLoader;
    }

    // EXPLORE_CLASSPATH and EXPLORE_CONF_FILES will be defined in startup scripts if Hive is installed.
    String exploreClassPathStr = System.getProperty(Constants.Explore.EXPLORE_CLASSPATH);
    LOG.debug("Explore classpath = {}", exploreClassPathStr);
    if (exploreClassPathStr == null) {
      throw new RuntimeException("System property " + Constants.Explore.EXPLORE_CLASSPATH + " is not set.");
    }

    String exploreConfPathStr = System.getProperty(Constants.Explore.EXPLORE_CONF_FILES);
    LOG.debug("Explore confPath = {}", exploreConfPathStr);
    if (exploreConfPathStr == null) {
      throw new RuntimeException("System property " + Constants.Explore.EXPLORE_CONF_FILES + " is not set.");
    }

    Iterable<File> hiveClassPath = getClassPathJarsFiles(exploreClassPathStr);
    Iterable<File> hiveConfFiles = getClassPathJarsFiles(exploreConfPathStr);
    ImmutableList.Builder<URL> builder = ImmutableList.builder();
    for (File file : Iterables.concat(hiveClassPath, hiveConfFiles)) {
      try {
        if (file.getName().matches(".*\\.xml")) {
          builder.add(file.getParentFile().toURI().toURL());
        } else {
          builder.add(file.toURI().toURL());
        }
      } catch (MalformedURLException e) {
        LOG.error("Jar URL is malformed", e);
        throw Throwables.propagate(e);
      }
    }
    exploreClassLoader = new URLClassLoader(Iterables.toArray(builder.build(), URL.class),
                                            ClassLoader.getSystemClassLoader());
    return exploreClassLoader;
  }

  public static Class<? extends ExploreService> getHiveService() {
    HiveSupport hiveVersion = checkHiveSupport(null);
    return hiveVersion.getHiveExploreServiceClass();
  }

  public static HiveSupport checkHiveSupport() {
    return checkHiveSupport(getExploreClassLoader());
  }

  /**
   * Check that Hive is in the class path - with a right version.
   */
  public static HiveSupport checkHiveSupport(ClassLoader hiveClassLoader) {
    // First try to figure which hive support is relevant based on Hadoop distribution name
    String hadoopVersion = VersionInfo.getVersion();
    LOG.info("Hadoop version is: {}", hadoopVersion);
    for (HiveSupport hiveSupport : HiveSupport.values()) {
      if (hiveSupport.getHadoopVersionPattern() != null &&
        hiveSupport.getHadoopVersionPattern().matcher(hadoopVersion).matches()) {
        return hiveSupport;
      }
    }

    ClassLoader usingCL = hiveClassLoader;
    if (usingCL == null) {
      usingCL = ExploreServiceUtils.class.getClassLoader();
    }

    try {
      Class<?> hiveVersionInfoClass = usingCL.loadClass("org.apache.hive.common.util.HiveVersionInfo");
      String hiveVersion = (String) hiveVersionInfoClass.getDeclaredMethod("getVersion").invoke(null);
      if (hiveVersion.startsWith("0.12.")) {
        return HiveSupport.HIVE_12;
      } else if (hiveVersion.startsWith("0.13.")) {
        return HiveSupport.HIVE_13;
      } else if (hiveVersion.startsWith("0.14.") || hiveVersion.startsWith("1.0.")) {
        return HiveSupport.HIVE_14;
      } else if (hiveVersion.startsWith("1.1.")) {
        return HiveSupport.HIVE_1_1;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    throw new RuntimeException("Hive distribution not supported. Set the configuration '" +
                                 Constants.Explore.EXPLORE_ENABLED +
                                 "' to false to start up without Explore.");
  }

  /**
   * Return the list of absolute paths of the bootstrap classes.
   */
  public static Set<String> getBoostrapClasses() {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String classpath : Splitter.on(File.pathSeparatorChar).split(System.getProperty("sun.boot.class.path"))) {
      File file = new File(classpath);
      builder.add(file.getAbsolutePath());
      try {
        builder.add(file.getCanonicalPath());
      } catch (IOException e) {
        LOG.warn("Could not add canonical path to aux class path for file {}", file.toString(), e);
      }
    }
    return builder.build();
  }

  /**
   * Trace the jar dependencies needed by the Explore container. Uses a separate class loader to load Hive classes,
   * built using the explore classpath passed as a system property to master.
   *
   * @return an ordered set of jar files.
   */
  public static Set<File> traceExploreDependencies(File tmpDir) throws IOException {
    if (exploreDependencies != null) {
      return exploreDependencies;
    }

    ClassLoader classLoader = getExploreClassLoader();
    return traceExploreDependencies(classLoader, tmpDir);
  }

  /**
   * Trace the jar dependencies needed by the Explore container.
   *
   * @param classLoader class loader to use to trace the dependencies.
   *                    If it is null, use the class loader of this class.
   * @param tmpDir temporary directory for storing rewritten jar files.
   * @return an ordered set of jar files.
   */
  public static Set<File> traceExploreDependencies(ClassLoader classLoader, File tmpDir)
    throws IOException {
    if (exploreDependencies != null) {
      return exploreDependencies;
    }

    ClassLoader usingCL = classLoader;
    if (classLoader == null) {
      usingCL = ExploreRuntimeModule.class.getClassLoader();
    }

    final Set<String> bootstrapClassPaths = getBoostrapClasses();

    ClassAcceptor classAcceptor = new ClassAcceptor() {
      /* Excluding any class contained in the bootstrapClassPaths and Kryo classes.
        * We need to remove Kryo dependency in the Explore container. Spark introduced version 2.21 version of Kryo,
        * which would be normally shipped to the Explore container. Yet, Hive requires Kryo 2.22,
        * and gets it from the Hive jars - hive-exec.jar to be precise.
        * */
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        if (bootstrapClassPaths.contains(classPathUrl.getFile()) ||
          className.startsWith("com.esotericsoftware.kryo")) {
          return false;
        }
        return true;
      }
    };

    Set<File> hBaseTableDeps = traceDependencies(usingCL, classAcceptor, tmpDir,
                                                 HBaseTableUtilFactory.getHBaseTableUtilClass().getName());

    // Note the order of dependency jars is important so that HBase jars come first in the classpath order
    // LinkedHashSet maintains insertion order while removing duplicate entries.
    Set<File> orderedDependencies = new LinkedHashSet<>();
    orderedDependencies.addAll(hBaseTableDeps);
    orderedDependencies.addAll(traceDependencies(usingCL, classAcceptor, tmpDir,
                                                 DatasetService.class.getName(),
                                                 "co.cask.cdap.hive.datasets.DatasetStorageHandler",
                                                 "co.cask.cdap.hive.datasets.StreamStorageHandler",
                                                 "org.apache.hadoop.hive.ql.exec.mr.ExecDriver",
                                                 "org.apache.hive.service.cli.CLIService",
                                                 "org.apache.hadoop.mapred.YarnClientProtocolProvider",
                                                 // Needed for - at least - CDH 4.4 integration
                                                 "org.apache.hive.builtins.BuiltinUtils",
                                                 // Needed for - at least - CDH 5 integration
                                                 "org.apache.hadoop.hive.shims.Hadoop23Shims"));

    exploreDependencies = orderedDependencies;
    return orderedDependencies;
  }

  /**
   * Trace the dependencies files of the given className, using the classLoader,
   * and including the classes that's accepted by the classAcceptor
   *
   * Nothing is returned if the classLoader does not contain the className.
   */
  public static Set<File> traceDependencies(ClassLoader classLoader, final ClassAcceptor classAcceptor,
                                            File tmpDir, String... classNames) throws IOException {
    LOG.debug("Tracing dependencies for classes: {}", Arrays.toString(classNames));

    ClassLoader usingCL = classLoader;
    if (usingCL == null) {
      usingCL = ExploreRuntimeModule.class.getClassLoader();
    }

    final String rewritingClassName = HIVEAUTHFACTORY_CLASS;
    final List<File> rewritingFiles = new ArrayList<>();

    final Set<File> jarFiles = Sets.newHashSet();

    for (String className : classNames) {
      Dependencies.findClassDependencies(
        usingCL,
        new ClassAcceptor() {
          @Override
          public boolean accept(String className, URL classUrl, URL classPathUrl) {
            if (!classAcceptor.accept(className, classUrl, classPathUrl)) {
              return false;
            }

            if (rewritingClassName.equals(className)) {
              rewritingFiles.add(new File(classPathUrl.getFile()));
            }

            jarFiles.add(new File(classPathUrl.getFile()));
            return true;
          }
        },
        className
      );
    }

    // Rewrite HiveAuthFactory.loginFromKeytab to be a no-op method.
    // This is needed because we don't want to use Hive's CLIService since
    // we're already using delegation tokens
    for (File rewritingFile : rewritingFiles) {
      // TODO: this may cause lots of rewrites since we may rewrite the same jar multiple times
      File rewrittenJar = rewriteHiveAuthFactory(
        rewritingFile, new File(tmpDir, rewritingFile.getName() + "-" + System.currentTimeMillis() + ".jar"));
      jarFiles.add(rewrittenJar);
      LOG.debug("Rewrote {} to {}", rewritingFile.getAbsolutePath(), rewrittenJar.getAbsolutePath());
    }
    jarFiles.removeAll(rewritingFiles);

    if (LOG.isDebugEnabled()) {
      for (File jarFile : jarFiles) {
        LOG.debug("Added jar {}", jarFile.getAbsolutePath());
      }
    }

    return jarFiles;
  }

  @VisibleForTesting
  static File rewriteHiveAuthFactory(File sourceJar, File targetJar) throws IOException {
    try (
      JarFile input = new JarFile(sourceJar);
      JarOutputStream output = new JarOutputStream(new FileOutputStream(targetJar))
    ) {
      String hiveAuthFactoryPath = HIVEAUTHFACTORY_CLASS.replace('.', '/') + ".class";

      Enumeration<JarEntry> sourceEntries = input.entries();
      while (sourceEntries.hasMoreElements()) {
        JarEntry entry = sourceEntries.nextElement();
        output.putNextEntry(new JarEntry(entry.getName()));

        try (InputStream entryInputStream = input.getInputStream(entry)) {
          if (!hiveAuthFactoryPath.equals(entry.getName())) {
            ByteStreams.copy(entryInputStream, output);
            continue;
          }


          try {
            // Rewrite the bytecode of HiveAuthFactory.loginFromKeytab method to a no-op method
            ClassReader cr = new ClassReader(entryInputStream);
            ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
            cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
              @Override
              public MethodVisitor visitMethod(final int access, final String name, final String desc,
                                               String signature, String[] exceptions) {
                MethodVisitor methodVisitor = super.visitMethod(access, name, desc, signature, exceptions);
                if (!"loginFromKeytab".equals(name)) {
                  return methodVisitor;
                }
                GeneratorAdapter adapter = new GeneratorAdapter(methodVisitor, access, name, desc);
                adapter.returnValue();

                // VisitMaxs with 0 so that COMPUTE_MAXS from ClassWriter will compute the right values.
                adapter.visitMaxs(0, 0);
                return new MethodVisitor(Opcodes.ASM5) { };
              }
            }, 0);
            output.write(cw.toByteArray());
          } catch (Exception e) {
            throw new IOException("Unable to generate HiveAuthFactory class", e);
          }
        }
      }

      return targetJar;
    }
  }

  /**
   * Updates environment variables in hive-site.xml, mapred-site.xml and yarn-site.xml for explore.
   * All other conf files are returned without any update.
   * @param confFile conf file to update
   * @param tempDir temp dir to create files if necessary
   * @return the new conf file to use in place of confFile
   */
  public static File updateConfFileForExplore(File confFile, File tempDir) {
    if (HIVE_SITE_FILE_PATTERN.matcher(confFile.getAbsolutePath()).matches()) {
      return updateHiveConfFile(confFile, tempDir);
    } else if (YARN_SITE_FILE_PATTERN.matcher(confFile.getAbsolutePath()).matches()) {
      return updateYarnConfFile(confFile, tempDir);
    } else if (MAPRED_SITE_FILE_PATTERN.matcher(confFile.getAbsolutePath()).matches()) {
      return updateMapredConfFile(confFile, tempDir);
    } else {
      return confFile;
    }
  }

  /**
   * Change yarn-site.xml file, and return a temp copy of it to which are added
   * necessary options.
   */
  private static File updateYarnConfFile(File confFile, File tempDir) {
    Configuration conf = new Configuration(false);
    try {
      conf.addResource(confFile.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("File {} is malformed.", confFile, e);
      throw Throwables.propagate(e);
    }

    String yarnAppClassPath = conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                       Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));

    // add the pwd/* at the beginning of classpath. so user's jar will take precedence and without this change,
    // job.jar will be at the beginning of the classpath, since job.jar has old guava version classes,
    // we want to add pwd/* before
    yarnAppClassPath = "$PWD/*," + yarnAppClassPath;

    conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, yarnAppClassPath);

    File newYarnConfFile = new File(tempDir, "yarn-site.xml");
    try (FileOutputStream os = new FileOutputStream(newYarnConfFile)) {
      conf.writeXml(os);
    } catch (IOException e) {
      LOG.error("Problem creating and writing to temporary yarn-conf.xml conf file at {}", newYarnConfFile, e);
      throw Throwables.propagate(e);
    }

    return newYarnConfFile;
  }

  /**
   * Change mapred-site.xml file, and return a temp copy of it to which are added
   * necessary options.
   */
  private static File updateMapredConfFile(File confFile, File tempDir) {
    Configuration conf = new Configuration(false);
    try {
      conf.addResource(confFile.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("File {} is malformed.", confFile, e);
      throw Throwables.propagate(e);
    }

    String mrAppClassPath = conf.get(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
                                       MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH);

    // Add the pwd/* at the beginning of classpath. Without this change, old jars from mr framework classpath
    // get into classpath.
    mrAppClassPath = "$PWD/*," + mrAppClassPath;

    conf.set(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH, mrAppClassPath);

    File newMapredConfFile = new File(tempDir, "mapred-site.xml");
    try (FileOutputStream os = new FileOutputStream(newMapredConfFile)) {
      conf.writeXml(os);
    } catch (IOException e) {
      LOG.error("Problem creating and writing to temporary mapred-site.xml conf file at {}", newMapredConfFile, e);
      throw Throwables.propagate(e);
    }

    return newMapredConfFile;
  }

  /**
   * Change hive-site.xml file, and return a temp copy of it to which are added
   * necessary options.
   */
  private static File updateHiveConfFile(File confFile, File tempDir) {
    Configuration conf = new Configuration(false);
    try {
      conf.addResource(confFile.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("File {} is malformed.", confFile, e);
      throw Throwables.propagate(e);
    }

    // we prefer jars at container's root directory before job.jar,
    // we edit the YARN_APPLICATION_CLASSPATH in yarn-site.xml using
    // co.cask.cdap.explore.service.ExploreServiceUtils.updateYarnConfFile and
    // setting the MAPREDUCE_JOB_CLASSLOADER and MAPREDUCE_JOB_USER_CLASSPATH_FIRST to false will put
    // YARN_APPLICATION_CLASSPATH before job.jar for container's classpath.
    conf.setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false);

    File newHiveConfFile = new File(tempDir, "hive-site.xml");

    try (FileOutputStream os = new FileOutputStream(newHiveConfFile)) {
      conf.writeXml(os);
    } catch (IOException e) {
      LOG.error("Problem creating temporary hive-site.xml conf file at {}", newHiveConfFile, e);
      throw Throwables.propagate(e);
    }
    return newHiveConfFile;
  }
}
