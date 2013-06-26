cassandra-data-access
=====================
Two modules for Cassandra Data Access.

cql
---
Template object, will be either a working JDBCTemplate configured for CQL3, or a CqlTemplate in the style of JDBCTemplate. 
Uses the Datastax Java Driver for transport and low level operations. 
Also planned for this module is a layer of support for accessing structured data using hierarchical paths and JSON.

Note: The tests currently are all system tests and require a local Cassandra 1.2+ database to be running.

thrift
------
Templates for column and super column family access, using the Hector library and Thrift for transport and low level operations. 



