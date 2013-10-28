C* Path
=======

Support for reading and writing structured objects in Cassandra.
Structured objects can be accessed whole or in part by hierarchical paths.

There is a presentation about C* Path from the Cassandra Summit Europe 2013 [here](http://www.slideshare.net/techblog/c-path).

On `writeToPath`, the object is first converted into maps, lists, and simple objects with the help of
[Jackson JSON Processor](http://wiki.fasterxml.com/JacksonHome) (the fasterxml.com version).
How objects are converted can be customized by using annotations supported by Jackson. These structures are then
decomposed into key-value pairs where the keys are paths.

On `readFromPath`, the reverse process is used to recompose the key-value pairs back into a structured object or POJO.

Paths can be used to access structured data at different levels within the structure. A Path can also contain a special
element that refers to an index within a list (or array or collection).

**Note:** Special support for Sets of simple values is also planned but not yet implemented.
In the meantime sets can be modeled as maps.

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

`new Class1()` would be decomposed into the following key-value pairs:

`a/b/@0/c/ -> 42`  
`a/b/@1/c/ -> 43`


Maven Dependency
----------------

The artifacts are published at Maven Central.
To include C* Path in your project, use one of the following dependencies. Generally either the CQL or thrift dependency
is used, but not both.

To include CQL module:

    <dependency>
        <groupId>com.ebuddy.cassandra</groupId>
        <artifactId>cql-data-access</artifactId>
        <version>2.2.5</version>
    </dependency>


Or include the Thrift module:

    <dependency>
        <groupId>com.ebuddy.cassandra</groupId>
        <artifactId>thrift-data-access</artifactId>
        <version>2.2.5</version>
    </dependency>

api module
----------
The main interface `StructuredDataAccessSupport` and helper classes.

cql module
----------
Implementation of `StructuredDataAccessSupport` for CQL3. Uses the
[Datastax Java Driver](https://github.com/datastax/java-driver) for transport and low level operations.

To use structured data in a CQL3 table, the following data modeling rules apply:

* The table should have one path column that is the first clustering key, i.e. the second column in the primary
  key after the partition key.
* There should be one other column for the values.
* The path and value columns should be typed as a textual type.

Note: The tests include system tests that require a local Cassandra 1.2+ database to be running.
These tests are in the "system" TestNG test group.

thrift module
-------------
Implementations of `StructuredDataAccessSupport` for standard column family access, using the
[Hector client](https://github.com/hector-client/hector) library (and therefore Thrift) for transport and
low level operations.  
**Note:** Support for super column families is planned but not yet implemented.

core module
-----------
Shared code used by both thrift and cql modules.
