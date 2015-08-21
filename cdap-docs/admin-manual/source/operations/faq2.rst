.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

=====
FAQ 2
=====

Here are some selected examples of potential problems and possible resolutions.

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



Transactions
============

Transaction...is not in progress during HelloWorld
--------------------------------------------------
I've modified the HelloWorld example, and now I am seeing transaction related errors::

[warn 2015/08/10 22:50:53.299 IST <DatasetTypeManager STARTING> tid=0x24] Transaction 1231000000 is not in progress.
co.cask.tephra.TransactionNotInProgressException: canCommit() is called for transaction 1231000000 that is not in progress (it is known to be invalid)
	at co.cask.tephra.TransactionManager.commit(TransactionManager.java:842)
	at co.cask.tephra.inmemory.InMemoryTxSystemClient.commit(InMemoryTxSystemClient.java:73)
	at co.cask.tephra.TransactionContext.commit(TransactionContext.java:265)
	...
	
Actually it looks like your transaction timed out. 
The message ``(it is known to be invalid)`` indicates that the transaction has timed out.

Transactions normally time out after 30 seconds and then are moved to the "invalid" set.
You can either start a long-running transaction [link] or increase the transaction timeout property [link].


Is the @RoundRobin annotation appropriate for stream events? 
-------------------------------------------------------------
The documentation only talks about partitioning when consuming from queues, not streams.
Do the same partitioning strategies  |---| FIFO, round-robin, and hash-based |---| also
apply to streams?

The @RoundRobin annotation is a property of the flowlet and it is applicable irrespective of
who (either a stream or another flowlet) is emitting the data to the flowlet. When a
stream is connected to a flowlet, the stream acts as a source which is basically a file-backed
queue. Based on the partitioning strategy specified for the flowlet, an appropriate instance
of the flowlet consumes the event from the queue.


Where is the CDAP CLI (Command Line Interface)?
-----------------------------------------------
We've installed CDAP on a cluster using RPM, and wanted to use the CDAP CLI, but couldn't find it.

If you've installed the ``cdap-cli`` RPM, it's located under ``/opt/cdap/cli/bin``.

You can add this location to your PATH to prevent the need for specifying the entire script every time.

**Note:** ``rpm -ql cdap-cli`` will list the contents of the package ``cdap-cli``, once it
has been installed.


Upgrading CDAP
==============

Can a current CDAP installation be upgraded more than one version?

This table lists the upgrade paths available for different CDAP versions:

Version  Direct Upgrade to:

2.6.3    2.8.1

If you are doing a new installation, we recommend using the current version of CDAP.

Installation
============

Memory and CPU Requirements
---------------------------
How much memory and how many CPU cores are required for CDAP services running on HDP
(Horton Data Platform)? We're seeing eight containers for the CDAP master.service.

The settings are governed by two sources: CDAP and YARN.

The default setting for CDAP are found in the cdap-defaults.xml, and are over-ridden in
particular instances by the cdap-site.xml file. These vary with each service and range
from 512 to 1024 MB and from one to two cores.

The YARN settings will over-ride these; for instance, the minimum YARN container size is
determined by yarn.scheduler.minimum-allocation-mb. The YARN default in HDP/Hadoop is 1024
MB, so containers will be allocated with 1024 MB, even if the CDAP settings are for 512
MB.

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


Check our issues database for known issues
------------------------------------------
When trying to solve an issue, one source of information is the CDAP Issues database. 
The `unresolved issues can be browsed
<https://issues.cask.co/issues/?jql=project%3DCDAP%20AND%20resolution%3DUnresolved%20ORDER%20BY%20priority%20DESC>`__; 
and using the search box in the upper-right, you can look for issues that contain a particular problem or keyword:

.. image:: ../_images/faq-quick-search.png

