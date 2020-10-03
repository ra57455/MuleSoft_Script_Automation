################################################################################
# Queries Tableau backend to return workbooks for archiving 
################################################################################

import psycopg2
import logging


class Selector(object):
    """
    Running SQL Query
    """
    query_cols = ["site_id", #Columns of SQL result to obtain.
                  "site_name",
                  "site_luid",
                  "project_name",
                  "project_luid",
                  "document_name",
                  "document_luid",
                  "size",
                  "type",
                  "repo_url",
                  "user_name",
                  "user_email",
                  "inactive_days",
                  ]

    def __init__(self, confs, content_type):
        """
        Initilize DB details.
        """
        self.conn_dict = {
            'dbname': confs['pg_dbname'],
            # 'database': 'workgroup',
            'user': confs['pg_username'],
            'password': confs['pg_password'],
            'host': confs['pg_host'],
            'port': confs['pg_port']
        }

        # self.wb_whitelist = confs['wb_whitelist']
        # self.ds_whitelist = confs['ds_whitelist']
        # self.proj_whitelist = confs['proj_whitelist']
        # self.site_whitelist = confs['site_whitelist']
        self.content_type = str(content_type).lower()

        # Added to handle type of content archived
        if self.content_type == 'workbook':
            self.query = self._get_query(confs['wb_query_path'])
        elif self.content_type == 'datasource':
            self.query = self._get_query(confs['ds_query_path'])
        else:
            raise RuntimeError("No valid content type specified")

        self.logger = logging.getLogger('tab-archiver-logger')
        self.logger.debug('Created Selector object\nConnection creds: %s', self.conn_dict)

    def _get_query(self, query_path):
        """
        Read query from file.
        :param query_path:
        :return: Query read from file.
        """
        if query_path:
            with open(query_path, 'r') as qf:
                res_query = "".join(qf.readlines())
        else:
            raise RuntimeError("Error while reading the query from file")

        return res_query

    def _filter(self, col_name, whitelist, wbs):
        return list(filter(lambda wb: wb[col_name] not in whitelist, wbs))

    def _run_query(self, query_path=None):
        """
        Run query provided.
        """
        conn = psycopg2.connect(**self.conn_dict)
        self.logger.debug("Connected to backend DB")
        self.logger.debug("Getting content by running query:\n%s", self.query)
        try:
            with conn.cursor() as curs:
                curs.execute(self.query)
                res_wb = curs.fetchall()
        finally:
            conn.close()
        return res_wb

    def get_content(self):
        """
        Return workbook or datasource details based on query.
        """
        res_wb = self._run_query()
        self.logger.debug("Getting content from postgres")
        if res_wb and len(res_wb[0]) != len(self.query_cols):
            raise RuntimeError(
                "The query did not return required set of fields\nRequired: {}\nReceived: {}".format(self.query_cols,
                                                                                                     res_wb[0]))
        res_wb = [{k: v for k, v in zip(self.query_cols, wb)} for wb in res_wb]
        self.logger.debug("Got %s of documents to archive", len(res_wb))
        # res_wb = self._filter("site_name", self.site_whitelist, res_wb)
        # self.logger.debug("After filtering for sites left with %s documents", len(res_wb))
        # res_wb = self._filter("project_name", self.proj_whitelist, res_wb)
        # self.logger.debug("After filtering for projects left with %s documents", len(res_wb))
        # if self.content_type == 'workbook':
        #     res_wb = self._filter("document_luid", self.wb_whitelist, res_wb)
        #     self.logger.info("After filtering for workbooks left with %s documents", len(res_wb))
        # elif self.content_type == 'datasource':
        #     res_wb = self._filter("document_luid", self.ds_whitelist, res_wb)
        #     self.logger.info("After filtering for datasources left with %s documents", len(res_wb))
        return res_wb


