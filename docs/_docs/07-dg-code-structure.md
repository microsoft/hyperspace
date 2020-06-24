---
title: "Code Structure"
permalink: /docs/dg-code-structure/
excerpt: "How to configure Hyperspace for your needs."
last_modified_at: 2020-06-23
toc: true
---

Hyperspace code is written in Scala and it is using 'sbt' as the build tool. Below is the list of folders in the code base hierarchy:

### src
`src` folder contains Hyperspace’s source and test code.

The source code files are available under `src/main/scala` and include implementation of core functionality and features of Hyperspace.

**src/main/scala/com/microsoft/hyperspace**
- **actions** : Implementation of Hyperspace actions to create, maintain and remove indexes in a concurrent manner.
- **index**: Implementation of Hyeprspace indexes, optimizations rules and other core features. 
- **util**: Various utility functions that are used by different features and components.
  
**src/main/scala/org/apache/spark**: Contains classes/methods to provide access to Spark's private classes/methods. 

If you are working on adding changes to Hyperspace, you would need to add or modify code in above packages.

The test code is available under:
**`src/test/scala/com/microsoft/hyperspace`**

The test code has a similar structure as the source code and each package in the test code includes unit test cases for the corresponding feature or functionality from the source code. Any code change or additional feature implementation needs to add relevant cases which fully cover the modified code.

### docs
`docs` folder contains useful documentation on different aspects of Hyperspace including coding, contribution and formatting guidelines, and information on how to build the project in various environments. You can access the documentation using the [Hyperspace Documentation](https://microsoft.github.io/hyperspace/docs/ug-quick-start-guide/) site. 

### dev
`dev` folder contains required resources for contributing code to the Hyperspace project as a developer. Currently, it includes the Scala formatting configuration file that is needed to make sure any new code change complies with the Hyperspace’s [coding guidelines](https://github.com/microsoft/hyperspace/blob/master/CONTRIBUTING.md).

### build
`build` folder contains sbt build scripts and configurations which are used to build the Hyperspace code base.

### project
`project` folder contains additional sbt build properties and plugin configurations.
