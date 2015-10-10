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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.DataFabricFacadeModule;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramResourceReporter;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.stream.DefaultStreamWriter;
import co.cask.cdap.app.stream.StreamWriterFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.queue.QueueReaderFactory;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.Command;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Services;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.Permission;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * A {@link TwillRunnable} for running a program through a {@link ProgramRunner}.
 *
 * @param <T> The {@link ProgramRunner} type.
 */
public abstract class AbstractProgramTwillRunnable<T extends ProgramRunner> implements TwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramTwillRunnable.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();

  private String name;
  private String hConfName;
  private String cConfName;

  private Injector injector;
  private Program program;
  private ProgramOptions programOpts;
  private ProgramController controller;
  private Configuration hConf;
  private CConfiguration cConf;
  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;
  private StreamCoordinatorClient streamCoordinatorClient;
  private ProgramResourceReporter resourceReporter;
  private LogAppenderInitializer logAppenderInitializer;
  private CountDownLatch runlatch;

  /**
   * Constructor.
   *
   * @param name Name of the TwillRunnable
   * @param hConfName Name of the hConf file as in the container directory
   * @param cConfName Name of the cConf file as in the container directory
   */
  protected AbstractProgramTwillRunnable(String name, String hConfName, String cConfName) {
    this.name = name;
    this.hConfName = hConfName;
    this.cConfName = cConfName;
  }

  protected abstract Class<T> getProgramClass();

  /**
   * Provides sets of configurations to put into the specification. Children classes can override
   * this method to provides custom configurations.
   */
  protected Map<String, String> getConfigs() {
    return ImmutableMap.of();
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.<String, String>builder()
                     .put("hConf", hConfName)
                     .put("cConf", cConfName)
                     .putAll(getConfigs())
                     .build())
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    System.setSecurityManager(new RunnableSecurityManager(System.getSecurityManager()));

    runlatch = new CountDownLatch(1);
    name = context.getSpecification().getName();
    Map<String, String> configs = context.getSpecification().getConfigs();

    LOG.info("Initialize runnable: " + name);
    try {
      CommandLine cmdLine = parseArgs(context.getApplicationArguments());

      // Loads configurations
      hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      UserGroupInformation.setConfiguration(hConf);

      cConf = CConfiguration.create(new File(configs.get("cConf")));

      injector = Guice.createInjector(createModule(context));

      zkClientService = injector.getInstance(ZKClientService.class);
      kafkaClientService = injector.getInstance(KafkaClientService.class);
      metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
      streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);

      // Initialize log appender
      logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();

      try {
        program = injector.getInstance(ProgramFactory.class)
          .create(cmdLine.getOptionValue(RunnableOptions.JAR));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      programOpts = createProgramOptions(cmdLine, context, configs);
      resourceReporter = new ProgramRunnableResourceReporter(program, metricsCollectionService, context);

      LOG.info("Runnable initialized: " + name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    // need to make sure controller exists before handling the command
    runlatch.await();
    if (ProgramCommands.SUSPEND.equals(command)) {
      controller.suspend().get();
      return;
    }
    if (ProgramCommands.RESUME.equals(command)) {
      controller.resume().get();
      return;
    }
    if (ProgramOptionConstants.INSTANCES.equals(command.getCommand())) {
      int instances = Integer.parseInt(command.getOptions().get("count"));
      controller.command(ProgramOptionConstants.INSTANCES, instances).get();
      return;
    }
    LOG.warn("Ignore unsupported command: " + command);
  }

  @Override
  public void stop() {
    try {
      LOG.info("Stopping runnable: {}.", name);
      controller.stop().get();
      logAppenderInitializer.close();
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while stopping runnable: {}.", name, e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.error("Failed to stop runnable: {}.", name, e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns {@code true} if service error is propagated as error for this controller state.
   * If this method returns {@code false}, service error is just getting logged and state will just change
   * to {@link ProgramController.State#KILLED}. This method returns {@code true} by default.
   */
  protected boolean propagateServiceError() {
    return true;
  }

  @Override
  public void run() {
    LOG.info("Starting metrics service");
    Futures.getUnchecked(
      Services.chainStart(zkClientService, kafkaClientService,
                          metricsCollectionService, streamCoordinatorClient, resourceReporter));

    LOG.info("Starting runnable: {}", name);
    controller = injector.getInstance(getProgramClass()).run(program, programOpts);
    final SettableFuture<ProgramController.State> state = SettableFuture.create();
    controller.addListener(new AbstractListener() {

      @Override
      public void alive() {
        runlatch.countDown();
      }

      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        if (currentState == ProgramController.State.ALIVE) {
          alive();
        } else {
          super.init(currentState, cause);
        }
      }

      @Override
      public void completed() {
        state.set(ProgramController.State.COMPLETED);
      }

      @Override
      public void killed() {
        state.set(ProgramController.State.KILLED);
      }

      @Override
      public void error(Throwable cause) {
        LOG.error("Program runner error out.", cause);
        state.setException(cause);
      }
    }, MoreExecutors.sameThreadExecutor());

    try {
      state.get();
      LOG.info("Program {} stopped.", name);
    } catch (InterruptedException e) {
      LOG.warn("Program {} interrupted.", name, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOG.error("Program {} execution failed.", name, e);
      if (propagateServiceError()) {
        throw Throwables.propagate(Throwables.getRootCause(e));
      }
    } finally {
      // Always unblock the handleCommand method if it is not unblocked before (e.g if program failed to start).
      // The controller state will make sure the corresponding command will be handled correctly in the correct state.
      runlatch.countDown();
    }
  }

  @Override
  public void destroy() {
    LOG.info("Releasing resources: {}", name);
    if (program != null) {
      Closeables.closeQuietly(program);
    }
    Futures.getUnchecked(
      Services.chainStop(resourceReporter, streamCoordinatorClient,
                         metricsCollectionService, kafkaClientService, zkClientService));
    LOG.info("Runnable stopped: {}", name);
  }

  private CommandLine parseArgs(String[] args) {
    Options opts = new Options()
      .addOption(createOption(RunnableOptions.JAR, "Program jar location"))
      .addOption(createOption(RunnableOptions.PROGRAM_OPTIONS, "Program options"));

    try {
      return new PosixParser().parse(opts, args);
    } catch (ParseException e) {
      throw Throwables.propagate(e);
    }
  }

  private Option createOption(String opt, String desc) {
    Option option = new Option(opt, true, desc);
    option.setRequired(true);
    return option;
  }

  /**
   * Creates program options. It contains program and user arguments as passed form the distributed program runner.
   * Extra program arguments are inserted based on the environment information (e.g. host, instance id). Also all
   * configs available through the TwillRunnable configs are also available through program arguments.
   */
  private ProgramOptions createProgramOptions(CommandLine cmdLine, TwillContext context, Map<String, String> configs) {
    ProgramOptions original = GSON.fromJson(cmdLine.getOptionValue(RunnableOptions.PROGRAM_OPTIONS),
                                            ProgramOptions.class);

    // Overwrite them with environmental information
    Map<String, String> arguments = Maps.newHashMap(original.getArguments().asMap());
    arguments.put(ProgramOptionConstants.INSTANCE_ID, Integer.toString(context.getInstanceId()));
    arguments.put(ProgramOptionConstants.INSTANCES, Integer.toString(context.getInstanceCount()));
    arguments.put(ProgramOptionConstants.RUN_ID, original.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    arguments.put(ProgramOptionConstants.TWILL_RUN_ID, context.getApplicationRunId().getId());
    arguments.put(ProgramOptionConstants.HOST, context.getHost().getCanonicalHostName());
    arguments.putAll(Maps.filterKeys(configs, Predicates.not(Predicates.in(ImmutableSet.of("hConf", "cConf")))));

    return new SimpleProgramOptions(context.getSpecification().getName(), new BasicArguments(arguments),
                                    original.getUserArguments(), original.isDebug());
  }

  // TODO(terence) make this works for different mode
  protected Module createModule(final TwillContext context) {
    return Modules.combine(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new ExploreClientModule(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(InetAddress.class).annotatedWith(Names.named(Constants.AppFabric.SERVER_ADDRESS))
            .toInstance(context.getHost());

          // For Binding queue stuff
          bind(QueueReaderFactory.class).in(Scopes.SINGLETON);

          // For program loading
          install(createProgramFactoryModule());

          // For binding DataSet transaction stuff
          install(new DataFabricFacadeModule());

          bind(ServiceAnnouncer.class).toInstance(new ServiceAnnouncer() {
            @Override
            public Cancellable announce(String serviceName, int port) {
              return context.announce(serviceName, port);
            }
          });

          bind(Store.class).to(DefaultStore.class);

          // For binding StreamWriter
          install(createStreamFactoryModule());
        }
      }
    );
  }

  private Module createStreamFactoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder().implement(StreamWriter.class, DefaultStreamWriter.class)
                  .build(StreamWriterFactory.class));
        expose(StreamWriterFactory.class);
      }
    };
  }

  private Module createProgramFactoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(LocationFactory.class)
          .annotatedWith(Names.named("program.location.factory"))
          .toInstance(new LocalLocationFactory(new File(System.getProperty("user.dir"))));
        bind(ProgramFactory.class).in(Scopes.SINGLETON);
        expose(ProgramFactory.class);
      }
    };
  }

  /**
   * A private factory for creating instance of Program.
   * It's needed so that we can inject different LocationFactory just for loading program.
   */
  private static final class ProgramFactory {

    private final CConfiguration cConf;
    private final LocationFactory locationFactory;

    @Inject
    ProgramFactory(CConfiguration cConf, @Named("program.location.factory") LocationFactory locationFactory) {
      this.cConf = cConf;
      this.locationFactory = locationFactory;
    }

    public Program create(String path) throws IOException {
      Location location = locationFactory.create(path);
      return Programs.createWithUnpack(cConf, location, Files.createTempDir());
    }
  }

  /**
   * A {@link SecurityManager} used by the runnable container. Currently it specifically disabling
   * Spark classes to call {@link System#exit(int)}, {@link Runtime#halt(int)}
   * and {@link System#setSecurityManager(SecurityManager)}.
   * This is to workaround modification done in HDP Spark (CDAP-3014).
   *
   * In general, we can use the security manager to restrict what system actions can be performed by program as well.
   */
  private static final class RunnableSecurityManager extends SecurityManager {

    private final SecurityManager delegate;

    private RunnableSecurityManager(@Nullable SecurityManager delegate) {
      this.delegate = delegate;
    }

    @Override
    public void checkPermission(Permission perm) {
      if ("setSecurityManager".equals(perm.getName()) && isFromSpark()) {
        throw new SecurityException("Set SecurityManager not allowed from Spark class: "
                                      + Arrays.toString(getClassContext()));
      }
      if (delegate != null) {
        delegate.checkPermission(perm);
      }
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      if ("setSecurityManager".equals(perm.getName()) && isFromSpark()) {
        throw new SecurityException("Set SecurityManager not allowed from Spark class: "
                                      + Arrays.toString(getClassContext()));
      }
      if (delegate != null) {
        delegate.checkPermission(perm, context);
      }
    }

    @Override
    public void checkExit(int status) {
      if (isFromSpark()) {
        throw new SecurityException("Exit not allowed from Spark class: " + Arrays.toString(getClassContext()));
      }
      if (delegate != null) {
        delegate.checkExit(status);
      }
    }

    /**
     * Returns true if the current class context has spark class.
     */
    private boolean isFromSpark() {
      for (Class c : getClassContext()) {
        if (c.getName().startsWith("org.apache.spark.")) {
          return true;
        }
      }
      return false;
    }
  }
}

