## Pre-Req

* Python 3.x should be installed.

* Following modules in python:
    * pyyaml
    * pyscopg2
    * pystache
    * requests
* Following modules in OS:
    * connectivity to tableau postgres and tableau API interface.
    * username and password for tableau server API interface.
    * postgres username and password for tableau server repository.
    
## Configurations

* ~/confs/base_conf.yaml - Configurations for the archiver to run. Ensure all configurations are provided in the file.
    * whitelist - filters that will stop a particular worbook, datasource from being archived.
* ~/confs/datasource_archive.sql - SQL that provides the input the archiver to archive the datasources.
* ~/confs/workbook_archive.sql - SQL that provides the input the archiver to archive the workbook.
* ~/log - Log directory where tableau archiver logs are rotated.
* ~/output - Output directory where workbooks are stored.
* ~/export - Directory where contains a csv of workbook or datasource to be archived.
* ~/templates - HTML template which is used in email that are being sent to users.
* ~/src - source code directory.
* ~/src/run_datasource.sh - scripts that triggers the datasource archival.
* ~/src/run_workbook.sh - scripts that triggers the workbook archival.

## Functionality

* Archives workbook and datasources.
* Emails users once the archival is complete.
* Download only the .xml of a workbook or datasource.

## Steps

1. Install Python3.5.
2. Install external libraries / packages mentioned in pre-requisuties. 
3. Execute `python archiver_main.py`.