# Business-to-Safe Core API

The Business to Safe Core is a java based API that provides basic framework for replication services. The default implementation provided with the package is based on IRODs Jargon v 4.0.2.


### Installation

* Clone the code in the repository.
* Run
`mvn install -Dmaven.test.skip=true`


### Testing

* Edit the testing.properties file and provide the IRODs connection information.
* Run `mvn test`
This will test the all basic functionalities replicate, retrieve, list, search, delete.


### How to use

To use this package you need to add the following two dependencies in your project.
> ```
<dependency>
  <groupId>org.irods</groupId>
  <artifactId>jargon</artifactId>
  <version>4.0.2-UFAL</version>
</dependency>
<dependency>
  <groupId>cz.cuni.mff.ufal</groupId>
  <artifactId>b2safe</artifactId>
  <version>1.0</version>
</dependency>
```

then you can use the default IRODs implemention or implements your own ReplicationService.
