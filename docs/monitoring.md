---
layout: global
title: Monitoring and Instrumentation
---

There are several ways to monitor Spark applications.

# Web Interfaces

Every SparkContext launches a web UI, by default on port 4040, that 
displays useful information about the application. This includes:

* A list of scheduler stages and tasks
* A summary of RDD sizes and memory usage
* Environmental information.
* Information about the running executors

You can access this interface by simply opening `http://<driver-node>:4040` in a web browser.
If multiple SparkContexts are running on the same host, they will bind to successive ports
beginning with 4040 (4041, 4042, etc).

Note that this information is only available for the duration of the application by default.
To view the web UI after the fact, set `spark.eventLog.enabled` to true before starting the
application. This configures Spark to log Spark events that encode the information displayed
in the UI to persisted storage.

## Viewing After the Fact

Spark's Standalone Mode cluster manager also has its own
[web UI](spark-standalone.html#monitoring-and-logging). If an application has logged events over
the course of its lifetime, then the Standalone master's web UI will automatically re-render the
application's UI after the application has finished.

If Spark is run on Mesos or YARN, it is still possible to reconstruct the UI of a finished
application through Spark's history server, provided that the application's event logs exist.
You can start a the history server by executing:

    ./sbin/start-history-server.sh <base-logging-directory>

The base logging directory must be supplied, and should contain sub-directories that each
represents an application's event logs. This creates a web interface at
`http://<server-url>:18080` by default. The history server depends on the following variables:

<table class="table">
  <tr><th style="width:21%">Environment Variable</th><th>Meaning</th></tr>
  <tr>
    <td><code>SPARK_DAEMON_MEMORY</code></td>
    <td>Memory to allocate to the history server. (default: 512m).</td>
  </tr>
  <tr>
    <td><code>SPARK_DAEMON_JAVA_OPTS</code></td>
    <td>JVM options for the history server (default: none).</td>
  </tr>
</table>

Further, the history server can be configured as follows:

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td>spark.history.updateInterval</td>
    <td>10</td>
    <td>
      The period, in seconds, at which information displayed by this history server is updated.
      Each update checks for any changes made to the event logs in persisted storage.
    </td>
  </tr>
  <tr>
    <td>spark.history.retainedApplications</td>
    <td>250</td>
    <td>
      The number of application UIs to retain. If this cap is exceeded, then the oldest
      applications will be removed.
    </td>
  </tr>
  <tr>
    <td>spark.history.ui.port</td>
    <td>18080</td>
    <td>
      The port to which the web interface of the history server binds.
    </td>
  </tr>
</table>

Note that in all of these UIs, the tables are sortable by clicking their headers,
making it easy to identify slow tasks, data skew, etc.

# Metrics

Spark has a configurable metrics system based on the 
[Coda Hale Metrics Library](http://metrics.codahale.com/). 
This allows users to report Spark metrics to a variety of sinks including HTTP, JMX, and CSV 
files. The metrics system is configured via a configuration file that Spark expects to be present 
at `$SPARK_HOME/conf/metrics.properties`. A custom file location can be specified via the 
`spark.metrics.conf` [configuration property](configuration.html#spark-properties).
Spark's metrics are decoupled into different 
_instances_ corresponding to Spark components. Within each instance, you can configure a 
set of sinks to which metrics are reported. The following instances are currently supported:

* `master`: The Spark standalone master process.
* `applications`: A component within the master which reports on various applications.
* `worker`: A Spark standalone worker process.
* `executor`: A Spark executor.
* `driver`: The Spark driver process (the process in which your SparkContext is created).

Each instance can report to zero or more _sinks_. Sinks are contained in the
`org.apache.spark.metrics.sink` package:

* `ConsoleSink`: Logs metrics information to the console.
* `CSVSink`: Exports metrics data to CSV files at regular intervals.
* `JmxSink`: Registers metrics for viewing in a JXM console.
* `MetricsServlet`: Adds a servlet within the existing Spark UI to serve metrics data as JSON data.
* `GraphiteSink`: Sends metrics to a Graphite node.

Spark also supports a Ganglia sink which is not included in the default build due to
licensing restrictions:

* `GangliaSink`: Sends metrics to a Ganglia node or multicast group.

To install the `GangliaSink` you'll need to perform a custom build of Spark. _**Note that
by embedding this library you will include [LGPL](http://www.gnu.org/copyleft/lesser.html)-licensed 
code in your Spark package**_. For sbt users, set the 
`SPARK_GANGLIA_LGPL` environment variable before building. For Maven users, enable 
the `-Pspark-ganglia-lgpl` profile. In addition to modifying the cluster's Spark build
user applications will need to link to the `spark-ganglia-lgpl` artifact.

The syntax of the metrics configuration file is defined in an example configuration file, 
`$SPARK_HOME/conf/metrics.properties.template`.

# Advanced Instrumentation

Several external tools can be used to help profile the performance of Spark jobs:

* Cluster-wide monitoring tools, such as [Ganglia](http://ganglia.sourceforge.net/), can provide 
insight into overall cluster utilization and resource bottlenecks. For instance, a Ganglia 
dashboard can quickly reveal whether a particular workload is disk bound, network bound, or 
CPU bound.
* OS profiling tools such as [dstat](http://dag.wieers.com/home-made/dstat/), 
[iostat](http://linux.die.net/man/1/iostat), and [iotop](http://linux.die.net/man/1/iotop) 
can provide fine-grained profiling on individual nodes.
* JVM utilities such as `jstack` for providing stack traces, `jmap` for creating heap-dumps, 
`jstat` for reporting time-series statistics and `jconsole` for visually exploring various JVM 
properties are useful for those comfortable with JVM internals.
