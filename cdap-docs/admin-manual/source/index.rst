.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _admin-index:

==================================================
CDAP Administration Manual
==================================================


.. rubric:: Installation


.. |installation| replace:: **Installation:**
.. _installation: installation/index.html

|installation|_ Covers **putting CDAP into production, with installation, configuration, and security setup.**
Appendices cover the XML files used to configure the CDAP installation and security configurations.

.. |quickstart| replace:: **Quick Start:**
.. _quickstart: installation/quick-start.html

- |quickstart|_ A quick start guide that covers the **most-common case of installing and 
  configuring CDAP.** Many people may find this sufficient; if your case isn't covered, the
  :ref:`install` guide has additional details.

.. |installation-install| replace:: **Installation:**
.. _installation-install: installation/installation.html

- |installation-install|_ Covers **installing CDAP:** the system, network, and software
  requirements; packaging options; and the instructions for installation of the
  CDAP components so they will work with your existing Hadoop cluster.

.. |configuration| replace:: **Configuration:**
.. _configuration: installation/configuration.html

- |configuration|_ Covers **configuring CDAP:** once CDAP :ref:`is installed <install>`,
  covers the configuration and verification of the CDAP components.

.. |security| replace:: **Security:**
.. _security: installation/security.html

- |security|_ CDAP supports **securing clusters using perimeter security.** This section
  describes enabling security, configuring authentication and testing security.

.. |appendices| replace:: **Appendices:**

- |appendices| Two appendices cover the XML files used to configure the 
  :ref:`CDAP installation <appendix-cdap-site.xml>` and the :ref:`security configuration.
  <appendix-cdap-security.xml>`


.. rubric:: Operations

.. |operations| replace:: **Operations:**
.. _operations: installation/index.html

|operations|_ Covers **logging, metrics, monitoring, preferences, scaling instances, resource guarantees, 
transaction service maintenance, troubleshooting, and introduces the CDAP UI.** 

.. |logging| replace:: **Logging:**
.. _logging: operations/logging.html

- |logging|_ Covers **CDAP support for logging** through standard SLF4J (Simple Logging Facade for Java) APIs.

.. |metrics| replace:: **Metrics:**
.. _metrics: operations/metrics.html

- |metrics|_ CDAP collects **metrics about the application’s behavior and performance**.
  
.. |monitoring| replace:: **Monitoring:**
.. _monitoring: operations/monitoring.html

- |monitoring|_ CDAP collects **logs and metrics** for all of its internal services. 
  This section provides links to the relevant APIs for accessing these logs and metrics.

.. |preferences| replace:: **Preferences and Runtime Arguments:**
.. _preferences: operations/preferences.html

- |preferences|_ Flows, MapReduce programs, services, workflows, and workers can receive **runtime arguments.**

.. |scaling-instances| replace:: **Scaling Instances:**
.. _scaling-instances: operations/scaling-instances.html

- |scaling-instances|_ Covers **querying and setting the number of instances of Flowlets.** 

.. |resource-guarantees| replace:: **Resource Guarantees:**
.. _resource-guarantees: operations/resource-guarantees.html

- |resource-guarantees|_ Providing resource guarantees **for CDAP programs in YARN.**

.. |tx-maintenance| replace:: **Transaction Service Maintenance:**
.. _tx-maintenance: operations/tx-maintenance.html

- |tx-maintenance|_ Periodic maintenance of the **Transaction Service.**

.. |troubleshooting| replace:: **Troubleshooting:**
.. _troubleshooting: operations/troubleshooting.html

- |troubleshooting|_ Selected examples of potential **problems and possible resolutions.**

.. |cdap-ui| replace:: **CDAP UI:**
.. _cdap-ui: operations/cdap-ui.html

- |cdap-ui|_ The CDAP UI is available for **deploying, querying and managing CDAP.** 
