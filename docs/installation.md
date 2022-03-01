---
title: Install doric in your project
permalink: docs/installation/
---
Not yet sorry
# Installing doric
Doric is compatible with spark version 3.1.3. Just add the dependency in your build tool.

The latest stable version of doric is 0.0.2.

The latest experimental version of doric is 0.0.2+23-16ee7656-SNAPSHOT.

## Sbt
```scala
libraryDependencies += "org.hablapps" % "doric_2.12" % "0.0.2"
```
## Maven
```xml
<dependency>
  <groupId>org.hablapps</groupId>
  <artifactId>doric_2.12</artifactId>
  <version>0.0.2</version>
</dependency>
```

Doric requires to activate the following flag when creating the spark context:
`spark.sql.datetime.java8API.enabled` equal to true.

Doric is committed to use the most modern API's first.
