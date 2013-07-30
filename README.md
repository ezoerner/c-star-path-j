cassandra-structured-data-access
================================

Support for reading and writing structured objects in Cassandra.
Structured objects are accessed by a hierarchical path delimited by forward slashes.

Some special characters are used internally in paths, so paths should not contain any of the special characters
'@#', and of course a path element cannot contain a forward slash '/'.

On `writeToPath`, the object is first converted into maps, lists, and simple objects with the help of
[Jackson JSON Processor](http://wiki.fasterxml.com/JacksonHome). How objects are converted can be
customized by using annotations supported by Jackson. These structures are then decomposed into key-value pairs
where the keys are paths.

On `readFromPath`, the reverse process is used to recompose the key-value pairs back into a structured object or POJO.

Paths can be used to access structured data at different levels within the structure. Lists have a specidal encoding
in a path using '@' followed by a integer index encoded into the path.  
**Note:** Special support for Sets of simple values is also planned but not yet implemented.

###Example:

    Class1 {
        Class2 a = new Class2();
    }

    Class2 {
        List<Class3> b = Arrays.asList(new Class3(42), new Class3(43));
    }

    Class3 {
        int c;

        Class3(int c) { this.c = c }
    }

`new Class1()` would be decomposed into the following path-value pairs:

`a/b/@0/c/ -> 42`  
`a/b/@1/c/ -> 43`


api
---
The main interface `StructuredDataAccessSupport` and helper classes.

cql
---
Implementation of `StructuredDataAccessSupport` for CQL3. Uses the
[Datastax Java Driver](https://github.com/datastax/java-driver) for transport and low level operations.

Note: The tests currently are all system tests and require a local Cassandra 1.2+ database to be running.

To use structured data in a CQL3 table, the following data modeling rules apply:

* There must be a designated path column and it must be the first clustering key,
i.e. the next element of the primary key after the partition key.
* There must be a designated value column.
* There can only be one designated path and one designated value column per table.
* The designated path and value columns must be typed as a textual type.


thrift
------
Implementations of `StructuredDataAccessSupport` for standard column family access, using the
[Hector client](https://github.com/hector-client/hector) library (and therefore Thrift) for transport and
low level operations.  
**Note:** Support for super column families is planned but not yet implemented.

core
----

Shared code used by both thrift and cql modules.
