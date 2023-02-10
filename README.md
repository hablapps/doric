# [doric](https://en.wikipedia.org/wiki/Doric_order)

Type-safe columns for spark DataFrames!

[![GitHub release (latest by date)](https://img.shields.io/github/v/release/hablapps/doric?label=latest%20release&logo=github)](https://github.com/hablapps/doric/releases/latest)
[![GitHub Release Date](https://img.shields.io/github/release-date/hablapps/doric?logo=github)](https://github.com/hablapps/doric/releases/latest)

[![CI](https://github.com/hablapps/doric/actions/workflows/ci.yml/badge.svg)](https://github.com/hablapps/doric/actions/workflows/ci.yml)
[![pages-build-deployment](https://github.com/hablapps/doric/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/hablapps/doric/actions/workflows/pages/pages-build-deployment)
[![Release](https://github.com/hablapps/doric/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/hablapps/doric/actions/workflows/release.yml)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/hablapps/doric/main?filepath=notebooks)

| Spark |                                                                        Maven Central                                                                         | Codecov                                                                                                                                                                 |
|:-----:|:------------------------------------------------------------------------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2.4.x | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_2-4_2.11)](https://mvnrepository.com/artifact/org.hablapps/doric_2-4_2.11/0.0.6) | [![Codecov](https://img.shields.io/codecov/c/github/hablapps/doric?flag=spark-2.4.x&label=codecov&logo=codecov&token=N7ZXUXZX1I)](https://codecov.io/gh/hablapps/doric) |
| 3.0.x | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-0_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-0_2.12/0.0.6) | [![Codecov](https://img.shields.io/codecov/c/github/hablapps/doric?flag=spark-3.0.x&label=codecov&logo=codecov&token=N7ZXUXZX1I)](https://codecov.io/gh/hablapps/doric) |
| 3.1.x | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-1_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-1_2.12/0.0.6) | [![Codecov](https://img.shields.io/codecov/c/github/hablapps/doric?flag=spark-3.1.x&label=codecov&logo=codecov&token=N7ZXUXZX1I)](https://codecov.io/gh/hablapps/doric) |
| 3.2.x | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-2_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-2_2.12/0.0.6) | [![Codecov](https://img.shields.io/codecov/c/github/hablapps/doric?flag=spark-3.2.x&label=codecov&logo=codecov&token=N7ZXUXZX1I)](https://codecov.io/gh/hablapps/doric) |
| 3.3.x | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-3_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-3_2.12/0.0.6) | [![Codecov](https://img.shields.io/codecov/c/github/hablapps/doric?flag=spark-3.3.x&label=codecov&logo=codecov&token=N7ZXUXZX1I)](https://codecov.io/gh/hablapps/doric) |
----

Doric offers type-safety in DataFrame column expressions at a minimum
cost, without compromising performance. In particular, doric allows you
to:

* Get rid of malformed column expressions at compile time
* Avoid implicit type castings
* Run DataFrames only when it is safe to do so
* Get all errors at once
* Modularize your business logic

You'll get all these goodies: 

* Without resorting to Datasets and sacrificing performance, i.e. sticking to DataFrames
* With minimal learning curve: almost no change in your code with respect to conventional column expressions
* Without fully committing to a strong static typing discipline throughout all your code

## User guide

Please, check out this [notebook](notebooks/README.ipynb) for examples
of use and rationale (also available through the
[binder](https://mybinder.org/v2/gh/hablapps/doric/HEAD?filepath=notebooks/README.ipynb)
link).

You can also check our [documentation page](https://www.hablapps.com/doric/)

## Installation

Fetch the JAR from Maven:

_Sbt_
```scala
libraryDependencies += "org.hablapps" %% "doric_3-2" % "0.0.6"
```
_Maven_
```xml
<dependency>
    <groupId>org.hablapps</groupId>
    <artifactId>doric_3-2_2.12</artifactId>
    <version>0.0.6</version>
</dependency>
```

`Doric` depends on Spark internals, and it's been tested against the
following spark versions.

| Spark | Scala | Tested |                                                                            doric                                                                             |
|:-----:|:-----:|:------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| 2.4.1 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.2 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.3 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.4 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.5 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.6 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.7 | 2.11  |   ✅   |                                                                              -                                                                               |
| 2.4.8 | 2.11  |   ✅   | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_2-4_2.11)](https://mvnrepository.com/artifact/org.hablapps/doric_2-4_2.11/0.0.6) |
| 3.0.0 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.0.1 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.0.2 | 2.12  |   ✅   | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-0_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-0_2.12/0.0.6) |
| 3.1.0 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.1.1 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.1.2 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.1.3 | 2.12  |   ✅   | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-1_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-1_2.12/0.0.6) |
| 3.2.0 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.2.1 | 2.12  |   ✅   |                                                                              -                                                                               |
| 3.2.2 | 2.12  |   ✅   | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-2_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-2_2.12/0.0.6) |
| 3.3.0 | 2.12  |   ✅   | [![Maven Central](https://img.shields.io/maven-central/v/org.hablapps/doric_3-3_2.12)](https://mvnrepository.com/artifact/org.hablapps/doric_3-3_2.12/0.0.6) |


## Contributing 

Doric is intended to offer a type-safe version of the whole Spark
Column API. Please, check the list of [open
issues](https://github.com/hablapps/doric/issues) and help us to
achieve that goal!

Please read the [contribution guide](CONTRIBUTING.md) 📋
