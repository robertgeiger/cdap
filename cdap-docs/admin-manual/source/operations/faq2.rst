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
