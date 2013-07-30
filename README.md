cassandra-data-access
=====================

Support for reading and writing structured objects in Cassandra.
Structured objects are accessed by a hierarchical path delimited by forward slashes.

Some special characters are used internally in paths, so paths should not contain any of the special characters
'@#', and of course a path element cannot contain a forward slash '/'.

Internally the object is decomposed into key value pairs using the
[Jackson JSON Processor](http://wiki.fasterxml.com/JacksonHome). How objects are broken
down can be customized by the caller using annotations supported by Jackson.

api
---
The interfaces and helper classes for StructuredDataAccessSupport.

cql
---
Implementation of StructuredDataAccessSupport for CQL3. Uses the Datastax Java Driver for transport and low level
operations.

Note: The tests currently are all system tests and require a local Cassandra 1.2+ database to be running.

To use structured data in a CQL3 table, the following data modeling rules apply:

* There must be a designated path column and it must be the first clustering key,
i.e. the next element of the primary key after the partition key.
* There must be a designated value column.
* There can only be one designated path and one designated value column per table.
* The designated path and value columns must be typed as a textual type.


thrift
------
Implementations of StructuredDataAccessSupport for column and super column family access, using the Hector client
library (and therefore Thrift) for transport and low level operations.

core
----

Shared code used by both thrift and cql modules for implementation.
