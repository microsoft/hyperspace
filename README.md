![Icon](https://github.com/rapoth/hyperspace/blob/master/docs/assets/images/hyperspace-small-banner.png?raw=true)

# Hyperspace

An open source indexing subsystem that brings index-based query acceleration to Apache Spark™ and big data workloads.

[aka.ms/hyperspace](https://aka.ms/hyperspace)

[![Build Status](https://dev.azure.com/ossspark/public/_apis/build/status/microsoft.hyperspace?branchName=master)](https://dev.azure.com/ossspark/public/_build/latest?definitionId=3&branchName=master)
[![javadoc](https://javadoc.io/badge2/com.microsoft.hyperspace/hyperspace-core_2.12/javadoc.svg)](https://javadoc.io/doc/com.microsoft.hyperspace/hyperspace-core_2.12/latest/com/microsoft/hyperspace/index.html)

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

Please review our [contribution guide](CONTRIBUTING.md).

## Inspiration and Special Thanks

This project would not have been possible without the outstanding work from the following communities:

- [Apache Spark](https://spark.apache.org/): Unified Analytics Engine for Big Data, the engine that 
  Hyperspace builds on top of.
- [Delta Lake](https://delta.io): Delta Lake is an open-source storage layer that brings ACID 
  transactions to Apache Spark™ and big data workloads. Hyperspace derives quite a bit of inspiration 
  from the way the Delta Lake community operates and pioneering of some surrounding ideas in the 
  context of data lakes (e.g., their novel use of optimistic concurrency). 
- [Databricks](https://databricks.com/): Unified analytics platform. Many thanks to all the inspiration 
  they have provided us.
- [.NET for Apache Spark™](https://github.com/dotnet/spark): Hyperspace offers .NET bindings for 
  developers, thanks to the efforts of this team in collaborating and releasing the bindings just-in-time.
- [Minimal Mistakes](https://github.com/mmistakes/minimal-mistakes): The awesome theme behind 
  Hyperspace documentation. 

## Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## License

Apache License 2.0, see [LICENSE](https://github.com/microsoft/hyperspace/blob/master/LICENSE).
