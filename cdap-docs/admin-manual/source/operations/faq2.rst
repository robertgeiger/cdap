.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

=====
FAQ 2
=====

Here are some selected examples of potential problems and possible resolutions.

Installation and Startup
========================

Building CDAP from Source
-------------------------
- We are trying to build CDAP from the source code, following the instructions given in
  BUILD.rst, but are unable find the results. Our build commands are::

    mvn package -e -X -DskipTests -Dcheckstyle.skip
    mvn package -e -X -DskipTests -Dcheckstyle.skip -P examples,templates,dist,release,tgz,unit-tests

  With these commands we are able to build (the maven build ends with SUCCESS status) but there is
  no cdap-<version>.tar.gz file found at cdap-distribution/target/ as mentioned in the README.rst.


The first of these commands builds all modules, skipping tests and checkstyle, but does not build a distribution.
The second command is the correct command, and it creates multiple output files (`` tar.gz ``), located in
the ``target`` directory inside each of ``cdap-master``, ``cdap-kafka``, ``cdap-gateway``, and ``cdap-ui``.


Memory and CPU Requirements
---------------------------
How much memory and how many CPU cores are required for CDAP services running on HDP
(Horton Data Platform)? We're seeing eight containers being used for the CDAP master.service.

The settings are governed by two sources: CDAP and YARN.

The default setting for CDAP are found in the cdap-defaults.xml, and are over-ridden in
particular instances by the cdap-site.xml file. These vary with each service and range
from 512 to 1024 MB and from one to two cores.

The YARN settings will over-ride these; for instance, the minimum YARN container size is
determined by yarn.scheduler.minimum-allocation-mb. The YARN default in HDP/Hadoop is 1024
MB, so containers will be allocated with 1024 MB, even if the CDAP settings are for 512
MB.


Upgrading CDAP
--------------

Can a current CDAP installation be upgraded more than one version?

This table lists the upgrade paths available for different CDAP versions:

Version  Upgrade Directly To:

2.6.3    2.8.1

If you are doing a new installation, we recommend using the current version of CDAP.


Where is the CDAP CLI (Command Line Interface)?
-----------------------------------------------
We've installed CDAP on a cluster using RPM, and wanted to use the CDAP CLI, but couldn't find it.

If you've installed the ``cdap-cli`` RPM, it's located under ``/opt/cdap/cli/bin``.

You can add this location to your PATH to prevent the need for specifying the entire script every time.

**Note:** ``rpm -ql cdap-cli`` will list the contents of the package ``cdap-cli``, once it
has been installed.


Error with CDAP SDK on start-up
-------------------------------
I've downloaded an SDK package (cdap-sdk-3.1.0.zip) from the cask.co website, and have installed it
on a CDH 5 data node with CentOS 6.5, JDK 1.7, node.js and maven 3.3.3. I'm seeing this error on startup::

  ERROR [main:c.c.c.StandaloneMain@268] - Failed to start Standalone CDAP
  java.lang.NoSuchMethodError: co.cask.cdap.UserInterfaceService.getServiceName()Ljava/lang/String;
    at co.cask.cdap.UserInterfaceService.access$000(UserInterfaceService.java:44) ~[co.cask.cdap.cdap-standalone-3.1.0.jar:na]
    ...
  	at co.cask.cdap.StandaloneMain.main(StandaloneMain.java:265) ~[co.cask.cdap.cdap-standalone-3.1.0.jar:na]  

You've downloaded the standalone version of CDAP. **It's not intended to be run on Hadoop clusters.**

Instead, you might want to download the CDAP CSD for Cloudera Manager, either from 
http://cloudera.com/downloads or http://cask.co/downloads. Using the CSD, you will be able to install CDAP on CDH.

In addition, the stack trace suggests that the JAVA_HOME is pointing to 1.6, rather than
1.7. The minimum version of Java supported by CDAP is 1.7. Echo ``$JAVA_HOME`` and adjust
it as required.


Installing CDAP on Cloudera
---------------------------
I am experiencing problems installing CDAP on Cloudera Live on AWS. Following `the tutorial 
<http://docs.cask.co/cdap/current/en/integrations/partners/cloudera/step-by-step-cloudera.html#step-by-step-cloudera-add-service>__,
when trying to start services, I received the following error in stderr::

  "Error found before invoking supervisord: No parcel provided required tags: set([u'cdap'])"


Start by clicking on the parcel icon (near the top-left corner of Cloudera Manager; looks
like a gift-wrapped box), and ensuring that the CDAP parcel is listed as *Active*.

There are 4 steps to installing a parcel:
* Adding the repository to the list of repositories searched by Cloudera Manager
* "Downloading" the parcel to the Cloudera Manager server
* "Distributing" the parcel to all the servers in the cluster
* "Activating" the parcel

The error message suggests that you have not completed the last step, *Activation*.


Starting Standalone CDAP…it failed to start
-------------------------------------------
When I start the CDAP Standalone, it fails to start. In the CDAP log, I'm seeing this error message::

  2015-05-15 12:15:53,028 - ERROR [heartbeats-scheduler:c.c.c.d.s.s.MDSStreamMetaStore$1@71] - Failed to access app.meta table
  co.cask.cdap.data2.dataset2.DatasetManagementException: Cannot retrieve dataset instance app.meta info,
  details: Response code: 407, message:'Proxy Authentication Required', body: '<HTML><HEAD>
  <TITLE>Access Denied</TITLE>
  </HEAD>

  Your credentials could not be authenticated: "Credentials are missing.". 
  You will not be permitted access until your credentials can be verified.

  This is typically caused by an incorrect username and/or password, 
  but could also be caused by network problems.
  
  For assistance, contact your network support team.
  
  at co.cask.cdap.data2.datafabric.dataset.DatasetServiceClient.getInstance(DatasetServiceClient.java:104)
  ...
  
I am running from behind a corporate poxy host, in case that's an issue.

According to that log, this is indeed caused by the proxy setting. 

CDAP services internally makes HTTP requests to each other; one example is the dataset
service. Depending on your proxy and its settings, these requests can end up being sent to
the proxy instead.

One item to check is that your system's network setting is configured to exclude both
localhost and 127.0.0.1 from the proxy routing. If they aren't, the services will not be
able to communicate with each other, and you'll see error messages such as these.


Questions About Installing in Distributed Mode
----------------------------------------------
I've installed CDAP and following the installation instructions, each component is
installed onto two machines.  I'm not using the CDAP Authentication Server at this point
to minimize the moving parts.  Is it really necessary to install all components on both
machines?  Could I instead install just the web app on a third Node and the other components on
the second Node?  Could I install each component on a separate machine if I chose to? The HA [High
Availability] Environment diagram seems to indicate this.

The CDAP components are independently scalable, so you can install from 1 to N of each component on any
combination of nodes.  The primary reasons to do so are for HA, and for cdap-router's data
ingest capacity.

Port 10000 was being used by another service so I changed router.server.port to 10023.

I'm assuming this was Hive Server2?  We are considering changing the router default port. 
You can follow this here: https://issues.cask.co/browse/CDAP-1696 

Several properties specify an IP where a service is running, such as: router.server.address,
metrics.query.bind.address, data.tx.bind.address, app.bind.address, router.bind.address.
What do I set these to if the components are running on multiple machines?

Our convention is that '*.bind.*' properties are what services use during startup to
listen on a particular interface/port.  '*.server.*' properties are used by clients to
connect to another (potentially remote) service.  

For '*.bind.address' properties, it is
often easiest just to set these to '0.0.0.0' to listen on all interfaces.   

'*.server.*'
properties are used by clients to connect to another remote service. 

The only one you
should need to configure initially is router.server.address, which is used by the UI to
connect to the router.  As an example, ideally routers running in production would have a
load balancer in front, which is what you would set router.server.address to. 
Alternatively, you could configure each UI instance to point to a particular router, and
if you have both UI and router running on each node, you could use '127.0.0.1'.

Applications
============

Analytics Pipeline with Hive, Spark, and CDAP
---------------------------------------------
We are planning to build an anlytics pipeline with these stages:

- Data Ingestion
- Data Transformation
- Data Analytics

In the Data Transformation stage, we are planning to use the CDAP Explore Service to transform the
data using JOIN, GROUP BY and ORDER BY queries.

We have an analytics pipeline in place, which uses Hive and Spark for processing. Now, we
want to port our pipeline using CDAP.

In Hive, we have queries which perform JOINs on multiple tables, GROUP BY and ORDER BY
queries. We understand that an application can't connect to the Explore Service and make
requests to it. Instead, we are planning to run our queries from outside the application
and once all queries are completed trigger the Spark program.

Are there better ways to use Hive and Spark together with CDAP?


The problem you have is to get the output of the Hive stage into CDAP.
For this, there are a couple of approaches that you can try. The ETL module in CDAP has a
database source which is described with an example:
https://github.com/cdap-guides/cdap-etl-adapter-guide/tree/develop/DBTableToHBaseTable.
Currently, this has not been tested with Hive, but it would be good to try it out with
Hive JDBC.

You can use this to get data into a CDAP Table. From then on, it becomes a dataset like
any other in CDAP, and can be accessed using Spark.

You can also set up an ETL pipeline (using the CDAP-UI) with Hive as a source and a CDAP Table as a
sink. Please see the `DBTableToHBaseTable example 
<https://github.com/cdap-guides/cdap-etl-adapter-guide/tree/develop/DBTableToHBaseTable>`__
for an example of database-to-CDAP Table. You will need to change the JDBC properties in the example
provided on that page. Alternatively, you can provide all of this information in the
CDAP-UI. 


Spark Running in Distributed Mode
---------------------------------
We're trying to execute a data analysis pipeline (MapReduce and Spark) on an HDP cluster using CDAP.
We are able to execute MapReduce jobs successfully.
However, Spark jobs are getting submitted but failing with following exception:

   Container exited with a non-zero exit code 1
   ...
   
There are a number of ways to solve problems such as these:

1. First, can you run a simple Spark job on YARN directly? Specifically, submit a Spark
   job using ``spark-submit —master yarn``. The shell script used to launch Spark containers on
   YARN is controlled by ``spark-submit``, not by CDAP. If that is unsuccessful, solving that is
   a pre-requisite to having CDAP working with Spark.

#. If you are seeing an exception such as::

      Exception message:
      /hadoop/yarn/local/usercache/yarn/appcache/application_1438676756737_0070/
      container_e03_1438676756737_0070_02_000005/launch_container.sh: line 26:
      $PWD/cdap-spark.jar/lib/*:...:$PWD/mr-framework/hadoop/share/hadoop/hdfs/lib/*:/
      usr/hdp/${hdp.version}/hadoop/lib/hadoop-lzo-0.6.0.${hdp.version}.jar:/etc/hadoop/conf/
      secure:$PWD/__app__.jar:$PWD/*: bad substitution

   The error message can be caused by the ``hdp.version`` property not being set for the
   YARN containers. This can be resolved by adding theses configurations to the
   ``cdap-site.xml`` file (usually located inside ``/etc/cdap/conf/``)::

      <property>
          <name>app.program.jvm.opts</name>
          <value>-Dhdp.version=${hdp.version} -XX:MaxPermSize=128M ${twill.jvm.gc.opts}</value>
          <description>Java options for all program containers</description>
      </property>
    
#. To further debug: when launching a Spark program, CDAP will first launch a YARN
   application to act as the client for submitting the actual Spark job. You can look at the 
   log files from that client container; usually the container-id ends with ``00002``,
   since ``00001`` is the YARN application.


User Interface
==============

Does CDAP support CORS?
-----------------------
CORS (`Cross-Origin Resource Sharing <http://www.w3.org/TR/cors/>`__) is 
currently not supported in CDAP. 

If you were interested in using CORS to create a webapp that showed information about CDAP
gathered through the RESTful APIs, a workaround would be the method used for the CDAP-UI.
Make backend requests through a NodeJS server and route the response to the client
browser. Here, the NodeJS server acts as a proxy and from it you can call the CDAP RESTful
end points without any issues of cross-domain.


Databases and Transactions
==========================

Understand the BufferingTable Undo API 
--------------------------------------
I don't understand the expected behavior of the undo API from BufferingTable.
If the input map contains a null value for a column, does this mean we should 
be deleting that entry for the associated column from the persistent store?

::

  /**
   * Undos previously persisted changes. After this method returns we assume that 
   * data can be visible to other table clients (of course other clients may choose 
   * still not to see it based on transaction isolation logic).
   *
   * @param persisted previously persisted changes. Map is described as row->(column->value).
   *                  Map can contain null values which means that the corresponded column was deleted
   * @throws Exception
   */
  protected abstract void undo(NavigableMap<byte[], NavigableMap<byte[], Update>> persisted)
    throws Exception;


Let's say you have these key-value pairs in your BufferingTable::

  a: 1
  b: 2

Then you deleted ``a`` |---| you would then have::

  b: 2

If you called ``undo()`` with ``{a: null}``, then the expected behavior would be to
"undelete" the ``"a"`` key-value pair which was previously deleted, resulting in the original
state::

  a: 1
  b: 2


Transaction...is not in progress during HelloWorld
--------------------------------------------------
I've modified the HelloWorld example, and now I am seeing transaction related errors::

[warn 2015/08/10 22:50:53.299 IST <DatasetTypeManager STARTING> tid=0x24] Transaction 1231000000 is not in progress.
co.cask.tephra.TransactionNotInProgressException: canCommit() is called for transaction 1231000000 
that is not in progress (it is known to be invalid)
	at co.cask.tephra.TransactionManager.commit(TransactionManager.java:842)
	at co.cask.tephra.inmemory.InMemoryTxSystemClient.commit(InMemoryTxSystemClient.java:73)
	at co.cask.tephra.TransactionContext.commit(TransactionContext.java:265)
	...
	
The message ``(it is known to be invalid)`` indicates that the transaction has timed out.
Transactions normally time out after 30 seconds and then are moved to the "invalid" set.
You can either start a long-running transaction [link] or increase the transaction timeout property [link].


Is the @RoundRobin annotation appropriate for stream events? 
-------------------------------------------------------------
The documentation only talks about partitioning when consuming from queues, not streams.
Do the same partitioning strategies |---| FIFO, round-robin, and hash-based |---| also
apply to streams?

The @RoundRobin annotation is a property of the flowlet and it is applicable irrespective of
who (either a stream or another flowlet) is emitting the data to the flowlet. When a
stream is connected to a flowlet, the stream acts as a source which is basically a file-backed
queue. Based on the partitioning strategy specified for the flowlet, an appropriate instance
of the flowlet consumes the event from the queue.


Other Resources
===============

Check our issues database for known issues
------------------------------------------
When trying to solve an issue, one source of information is the CDAP Issues database. 
The `unresolved issues can be browsed
<https://issues.cask.co/issues/?jql=project%3DCDAP%20AND%20resolution%3DUnresolved%20ORDER%20BY%20priority%20DESC>`__; 
and using the search box in the upper-right, you can look for issues that contain a particular problem or keyword:

.. image:: ../_images/faq-quick-search.png



