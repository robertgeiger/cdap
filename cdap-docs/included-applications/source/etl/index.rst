.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-index:

============
ETL Overview 
============


A Quick Intro to ETL
====================

Most data infrastructures are front ended with an ETL system (Extract-Transform-Load). The
purpose of the system is to:

- Extract data from one or more sources;
- Transform the data to fit the operational needs; and
- Load the data into a desired destination.

ETL usually comprises a source, zero or more transformations and one or more sinks, in what is called
an *ETL pipeline:*

.. image:: ../_images/etl-pipeline.png
   :width: 6in
   :align: center


.. rubric:: System Artifacts, ETL Applications, and ETL Plugins 

An *Application Template* is a blueprint that is used to create an *Adapter*, an instantiation of
a template in CDAP.

CDAP provides system artifacts |---| ``cdap-etl-batch`` and ``cdap-etl-realtime`` |---|
which are used to create applications that perform ETL in either batch or real time, and
consist of a variety of sources, transformations, and sinks that are packaged together.

A third system artifact, ``cdap-etl-lib``, provides common resources for the other two system artifacts.

The batch sources can write to any batch sinks that are available and real-time sources can
write to any real-time sinks. Transformations work with either sinks or sources. Transformations
can use *validators* to test data and check that it follows user-specified rules.

This lists the available sources, sinks, and transformations (transforms):

.. list-table::
   :widths: 30 40 30
   :header-rows: 1

   * - Sources
     - Transformations
     - Sinks
   * - - **Batch Sources**

         - :ref:`Database <included-apps-etl-plugins-batch-sources-database>`
         - :ref:`Elasticsearch <included-apps-etl-plugins-batch-sources-elasticsearch>`
         - :ref:`File <included-apps-etl-plugins-batch-sources-file>`
         - :ref:`KVTable <included-apps-etl-plugins-batch-sources-kvtable>`
         - :ref:`Amazon S3 <included-apps-etl-plugins-batch-sources-s3>`
         - :ref:`Stream <included-apps-etl-plugins-batch-sources-stream>`
         - :ref:`Table <included-apps-etl-plugins-batch-sources-table>`
         - :ref:`TPFSAvro <included-apps-etl-plugins-batch-sources-tpfsavro>`

     - - **Transforms**

         - :ref:`LogParser <included-apps-etl-plugins-transformations-logparser>`
         - :ref:`Projection <included-apps-etl-plugins-transformations-projection>`
         - :ref:`Script <included-apps-etl-plugins-transformations-script>`
         - :ref:`ScriptFilter <included-apps-etl-plugins-transformations-scriptfilter>`
         - :ref:`StructuredRecordToGenericRecord <included-apps-etl-plugins-transformations-structuredrecordtogenericrecord>`
         - :ref:`Validator <included-apps-etl-plugins-transformations-validator>`

     - - **Batch Sinks**

         - :ref:`Cube <included-apps-etl-plugins-batch-sinks-cube>`
         - :ref:`Database <included-apps-etl-plugins-batch-sinks-database>`
         - :ref:`Elasticsearch <included-apps-etl-plugins-batch-sinks-elasticsearch>`   
         - :ref:`KVTable <included-apps-etl-plugins-batch-sinks-kvtable>`
         - :ref:`SnapshotAvro <included-apps-etl-plugins-batch-sinks-snapshotavro>`
         - :ref:`SnapshotParquet <included-apps-etl-plugins-batch-sinks-snapshotparquet>`
         - :ref:`Table <included-apps-etl-plugins-batch-sinks-table>`
         - :ref:`TPFSAvro <included-apps-etl-plugins-batch-sinks-tpfsavro>`
         - :ref:`TPFSParquet <included-apps-etl-plugins-batch-sinks-tpfsparquet>`

   * - - **Real-time Sources**

         - :ref:`AmazonSQS <included-apps-etl-plugins-real-time-sources-amazonsqs>`
         - :ref:`DataGenerator <included-apps-etl-plugins-real-time-sources-datagenerator>`
         - :ref:`JMS <included-apps-etl-plugins-real-time-sources-jms>`
         - :ref:`Kafka <included-apps-etl-plugins-real-time-sources-kafka>`
         - :ref:`Twitter <included-apps-etl-plugins-real-time-sources-twitter>`

     - - **Transforms**

         - :ref:`LogParser <included-apps-etl-plugins-transformations-logparser>`
         - :ref:`Projection <included-apps-etl-plugins-transformations-projection>`
         - :ref:`Script <included-apps-etl-plugins-transformations-script>`
         - :ref:`ScriptFilter <included-apps-etl-plugins-transformations-scriptfilter>`
         - :ref:`StructuredRecordToGenericRecord <included-apps-etl-plugins-transformations-structuredrecordtogenericrecord>`
         - :ref:`Validator <included-apps-etl-plugins-transformations-validator>`

     - - **Real-time Sinks**

         - :ref:`Cube <included-apps-etl-plugins-real-time-sinks-cube>`
         - :ref:`Elasticsearch <included-apps-etl-plugins-real-time-sinks-elasticsearch>`
         - :ref:`Stream <included-apps-etl-plugins-real-time-sinks-stream>`
         - :ref:`Table <included-apps-etl-plugins-real-time-sinks-table>`



.. rubric:: ETL Applications

An *ETL Adapter* is an instantiation of an ETL template that has been given a specific
configuration on creation.

**Batch adapters** can be scheduled to run periodically using a cron expression and can read
data from batch sources using a MapReduce job. The batch adapter then performs any
optional transformations before writing to a batch sink.

**Real time adapters** are designed to poll sources periodically to fetch the data, perform any
optional transformations, and then write to a real-time sink.

ETL adapters are created by preparing a configuration that specifies the ETL template and
which source, transformations (transforms), and sinks are used to create the adapter. The
configuration can either be written as a JSON file or, in the case of the CDAP UI,
specified in-memory.

.. rubric:: ETL Plugins

The sources, transformations, and sinks are generically called plugins. Plugins provide a
way to extend the functionality of existing artifacts. An application can be created with
existing plugins or, if the user wishes, they can write their own source, transform, and
sink plugins to add their own. You can write your own validator using functions supplied in
either the :ref:`CoreValidator <included-apps-etl-plugins-shared-core-validator>` or implement your own.


Application and Plugin Details
==============================

.. |etl-creating| replace:: **Creating an ETL Application:**
.. _etl-creating: creating.html

- |etl-creating|_ Covers using the system artifacts and ETL plugins included with CDAP to create an ETL application.


.. |etl-operations| replace:: **Application Lifecycle Management:**
.. _etl-operations: ../../reference-manual/http-restful-api/lifecycle.html

- |etl-operations|_ Manage ETL Applications using CDAP's :ref:`Lifecycle HTTP RESTful API <http-restful-api-lifecycle>`.


.. |etl-custom| replace:: **Creating Custom ETL Plugins:**
.. _etl-custom: custom.html

- |etl-custom|_ Intended for developers writing custom ETL plugins.

.. |etl-plugins| replace:: **ETL Plugins:**
.. _etl-plugins: plugins/index.html

- |etl-plugins|_ Details on ETL plugins and exploring available plugins using RESTful APIs.

