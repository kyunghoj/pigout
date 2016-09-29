README
======

PigOut: Make Multiple Hadoop Clusters Work Together

TODO
---
* Add unit test code

Changelog
---
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
