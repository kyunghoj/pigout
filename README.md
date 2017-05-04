README
======

PigOut: Make Multiple Hadoop Clusters Work Together

TODOs
---
* Add unit test code
* Make PigOut deployable on public clouds such as Amazon Web Services and Google Cloud
* Add flexibility:
  * Reasonable configuration parameters: Data sources (e.g., HDFS cluster) and matching data analysis platform (e.g., Hadoop MapReduce)
* Cleaner and more reasonable implementation

Changelog
---
* May 4, 2017: Add TODO items 
* September 29, 2016: Start code clean up

Build
---

```
$> mvn install
```

Develop
---

To import the project into Eclipse:

* Generate eclipse configuration by:
```
$> mvn eclipse:eclipse -DdownloadSources -DdownloadJavadocs
```

* Then, import the project by clicking File->Import->Existing project into...

* If you want to import PigOut as a Maven project in Eclipse, you shouldn't generate eclipse configuration files as above. (TODO: Then how?)

Run
---

```
$> bin/pigout -x local -f simple.pig
```

Code Contributors
---

* Kyungho Jeon
* Feng Shen
* Sharath Chandrashekhara

Advisors
---

* Prof. Oliver Kennedy
* Prof. Steven Y. Ko
