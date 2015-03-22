# Toolsverse ETL Framework #

ETL Framework is a standalone Extract Transform Load engine written in Java. It includes executables for all major platforms and can be easily integrated into other applications.

## New in 3.2 (01/18/2013) ##
  * Improved auto-update functionality
  * Bug fixes

## Key Features ##
  * embeddable, open source and free
  * fast and scalable
  * uses target database features to do transformations and loads
  * manual and automatic data mapping
  * data streaming
  * bulk data loads
  * data quality features using SQL, JavaScript and regex
  * data transformations

## We created ETL Framework with these primary goals in mind ##
  * **Performance.** High speeds are achieved by utilizing power of the specific target database (e.g. temporary tables, bulk load, cursors); multithreading is supported at all levels: from extract and load to executing multiple ETL scenarios.
  * **Data streaming.** When streaming is enabled, it is possible to move practically unlimited sets of data from the source to destination even in the memory constrained environment.
  * **Easy to program.** High-level XML-based programming language is included in the ETL framework;  it is possible to embed code in the native SQL dialects (such as PL/SQL) and JavaScript.
  * **Feature-rich.** All expected features are included—from column level mapping to support for a wide range of data formats. Data qualiy features and validation using JavaScript, SQL and regex as well as transformations such as de-duplication, de-normalization, pivot, set operations, filtering, ordering, and validation are built in. Most popular databases are natively supported.
  * **Embeddable and expandable.** All core components—drivers, connectors, transformations, functions, code generators etc.—are dynamically loaded plug-in modules. It is easy to add new or modify existing functionality. ETL Framework can also be easily integrated into your application by embedding the ETL engine or running it in the client-server mode.

## Requirements ##
  * Java 1.6 and up
  * At least 4 MB of RAM

## Quick Resources ##
  * [Features](http://www.toolsverse.com/products/etl-framework/#features)
  * [User Guide](http://www.toolsverse.com/products/etl-framework/docs/etl-framework-user-guide.pdf)
  * [ETL Scenario Examples](http://www.toolsverse.com/products/etl-framework/examples/)
  * [How ETL Framework works](http://www.toolsverse.com/products/etl-framework/etlworks.shtml)
  * [Support Forum](http://toolsverse.com/forum/viewforum.php?f=4&sid=f31dfe44ea30df9a119451f5a4690cd1)
  * [Java Doc](http://www.toolsverse.com/javadoc/)