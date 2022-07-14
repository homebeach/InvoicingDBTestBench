Invoicing Database Test Bench


This program is used to generate test data into invoicing database and executing query tests.

In order to repeat the tests used in the study MySQL 5.1.41, MySQL 8.0.29, MariaDB 10.8.3 and Neo4J 4.4.8 need to be installed first. Configure different ports for different MySQL versions and MariaDB if they are installed simultaneously.

After the databases have been installed, just run the main program.

The main program includes the same test settings used in the study.

The content of the data files city_of_houston.csv, firstnames.csv, surnames.csv have been shortened to fit into GitHub.

Full file containing firstnames can be downloaded from https://data.world/alexandra/baby-names. Rename the file to firstnames.csv and move it into the "data" folder.

Full file containing surnames can be downloaded from https://data.world/uscensusbureau/frequently-occurring-surnames-from-the-census-2010. Rename the file to surnames.csv and copy it to data folder.

Full list of addresses in the city of Houston can be downloaded from http://results.openaddresses.io/. Download the file openaddr-collected-us_south.zip. Unzip the file. From the unzipped folder copy us/tx/city_of_houston.csv to data folder.
