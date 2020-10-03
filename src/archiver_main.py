import logging
import os
import os.path
import yaml
import sys
import archiver as archiver
import tab_select as tab_select
import tab_interface_rest as tab_interface_rest

from logging import handlers


def parse_configurations():
    """
    Read yaml files and store it in object.
    :returns Configuration Object of conf.yaml , List of items we need to access.
    """
    if sys.argv[1] == "mulsoftdev":
        with open('./confs/base_conf.yaml') as cnf:
            confs = yaml.load(cnf)
    c_list = ['workbook','datasource']
    return confs,c_list # str(args.content_type).lower()

def configure_logging(log_confs):
    logger = logging.getLogger('tab-archiver-logger')
    logger.setLevel(log_confs.get('level', logging.INFO))

    formatter = logging.Formatter('%(asctime)s - %(pathname)s - %(levelname)s: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_confs.get('level', logging.INFO))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    log_path = log_confs.get('path', '/tmp/tab-archiver/log/tab-archiver.log')
    log_dir = os.path.dirname(log_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # file_handler = logging.FileHandler(log_path, mode='a')

    # Fix to rotate the log file every day
    file_handler = handlers.TimedRotatingFileHandler(log_path, when='h', interval=1, backupCount=5)
    file_handler.setLevel(log_confs.get('level', logging.INFO))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)



def run(archiver_confs, selector_confs, interface_confs, logger, content_type_l):
    """
    :param content_type:
    :param logger:
    :param interface_confs:
    :param selector_confs:
    :param archiver_confs:
    """
    logger.info("Starting archiver run")
    logger.info("Archiver confs: %s\nSelector confs: %s\n", archiver_confs, selector_confs)
    interface = tab_interface_rest.TabInterface(interface_confs) #Create Tableau API interface object.
    arch = archiver.ContentArchiver(interface, archiver_confs) #Create archiver object.

    for content_type in content_type_l: #Iterate for different content (wb , datasource)
        selector = tab_select.Selector(selector_confs, content_type) #Object to run query.
        content = selector.get_content() #Executing query present in sql file respective to content type.
        arch.exportcsv(content) #Export query output to csv.
        if content_type == 'workbook':
            arch.process_content(content, content_type)
        elif content_type == 'datasource':
            arch.process_content(content, content_type)

    logger.info("Finished archiver run")

#Starting Script here.
if __name__ == '__main__':
    confs, content_type = parse_configurations() #Load Configuration and content_type.
    configure_logging(confs['log']) #Configure logs
    logger = logging.getLogger('tab-archiver-logger') #Creating logging object.
    run(confs['archiver'], confs['tab_select'], confs['tab_interface'], logger, content_type) #Trigger Script - main method.
