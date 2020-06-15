Scala Coding Style
===============

* For Scala code, we follow these coding and style guidelines:
    * [Databricks Scala Guide](https://github.com/databricks/scala-style-guide)
    * [Scala style guide](https://docs.scala-lang.org/style/)
> Note: Databricks Style Guide will be preferred for conflict resolutions.

* For formatting, [scalafmt](https://scalameta.org/scalafmt) is used with the custom configuration (found in [/dev/.scalafmt.conf](/dev/.scalafmt.conf))
  * Installation of `scalafmt` can be found [here](https://scalameta.org/scalafmt/docs/installation.html)

#### Organizing Imports
Scalafmt does not organize imports automatically as suggested in the databricks style-guide
To do this automatically using intellij
Intellij -> Settings -> Code Style -> Scala -> Imports -> Import Layout
and set the layout to:
```
java
javax
_______ blank line _______
scala
_______ blank line _______
all other imports
_______ blank line _______
com.microsoft.hyperspace
```
To do the import re-arrangement, use the keyboard shortcut for Optimize Imports.
The shortcut is at<br>
Settings -> Keymap -> Main menu -> Code -> Optimize Imports
