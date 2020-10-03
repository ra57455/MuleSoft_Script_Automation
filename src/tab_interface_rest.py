################################################################################
# REST Tableau interface implementation
################################################################################

import xml.etree.ElementTree as etree

import tab_interface_base
import requests
import logging

API_VERSION = "api/3.8"
URL_TEMPLATE = "/".join(["https://{host}", API_VERSION, "{request}"]) #URL of API

#SignIN POST body.
SIGNIN_PAYLOAD = """
<tsRequest>
  <credentials name="{username}" password="{password}" >
    <site contentUrl="{content_url}" />
  </credentials>
</tsRequest>
"""

TAB_HEADER = "X-Tableau-Auth"

# Namespace for xml parsing.
XML_NS = {'tab_ns': 'http://tableau.com/api'}


class TabInterface(tab_interface_base.TabInterfaceBase):
    def __init__(self, confs):
        """
        Inializing API details.
        :param confs: Conf file for API details.
        """

        self.username = confs['tab_username']
        self.password = confs['tab_password']
        self.host = confs['tab_host']

        self._reset_connection_variables()

        self.logger = logging.getLogger('tab-archiver-logger')
        self.logger.debug("Created TabInterface")

    def _reset_connection_variables(self):
        """
        Reset Connection to API.
        """
        self.cred_token = None
        self.site_id = None
        self.site_content_url = None
        self.user_id = None

    def sign_in(self, site_name=None):
        """
        Sign IN to Tableau.
        """
        request_endpoint = "auth/signin"

        # Don't log in if there is already a token. To log in again clean the token and call sign_in
        if self.cred_token:
            return

        if not site_name:
            site_name = ""
        # Added to address the default site login issue. This was missed during development of this solution
        if site_name == 'Default':
            site_name = ""

        # self.logger.debug("Inside Sign-In Method %s", site_name)
        url = URL_TEMPLATE.format(host=self.host, request=request_endpoint)
        payload = SIGNIN_PAYLOAD.format(username=self.username, password=self.password, content_url=site_name)

        self.logger.debug("POST to %s with %s", url, payload)
        #requests.add_http_options({'verify': False})

        resp = requests.post(url, payload,verify=False)

        # self.logger.debug("Got response: %s", resp.content.split("\n"))
        self.check_response(resp)
        try:
            resp_elmt = etree.fromstring(resp.content)
            self.logger.debug("Output %s", resp_elmt)
            cred_elmt = resp_elmt.find('tab_ns:credentials', XML_NS)
            self.cred_token = cred_elmt.attrib['token']

            site_elmt = cred_elmt.find('tab_ns:site', XML_NS)
            self.site_id = site_elmt.attrib['id']
            self.site_content_url = site_elmt.attrib['contentUrl']

            user_elmt = cred_elmt.find('tab_ns:user', XML_NS)
            self.user_id = user_elmt.attrib['id']
        except:
            self.logger.exception("Error processing signin response: %s", resp.content)
            raise RuntimeError("Error processing signin response")

        self.logger.info("Successfully signed in to size %s", self.site_content_url)
        return resp

    def is_signed_in(self):
        return self.cred_token is not None

    def sign_out(self):
        """
        Sign OUT of Tableau.
        """
        if not self.cred_token:
            return

        url = URL_TEMPLATE.format(host=self.host, request="auth/signout")

        self.logger.debug("POST to %s", url)
        resp = requests.post(url,
                             headers={TAB_HEADER: self.cred_token},verify=False)

        try:
            self.check_response(resp)
        except:
            pass

        self._reset_connection_variables()
        self.logger.debug("Successfully signed out")
        return resp

    def download_content(self, site_id, content_id, content_type):
        """
        Download file from Tableau server.
        :param content_type: Type of content.
        :param content_id: File LUID.
        :param site_id:Site LUID.
        """
        # Added the includeExtract=false in the request_endpoint
        # Added the content_type to handle datasource and workbook

        if content_type == 'workbook':
            request_endpoint = "sites/{site_id}/workbooks/{workbook_id}/content?includeExtract=false".format(
                site_id=site_id,
                workbook_id=content_id)
        elif content_type == 'datasource':
            request_endpoint = "sites/{site_id}/datasources/{datasource_id}/content?includeExtract=false".format(
                site_id=site_id,
                datasource_id=content_id)
        url = URL_TEMPLATE.format(host=self.host, request=request_endpoint)

        self.logger.debug("GET to %s", url)

        # Added timeout to avoid large file download (for workbooks which over a GB)
        resp = requests.get(url, headers={TAB_HEADER: self.cred_token}, timeout=3600,verify=False)

        self.check_response(resp)

        desc = {kv[0].strip(' "'): kv[1].strip(' "') for kv in
                [part.split("=") for part in resp.headers['content-disposition'].split(';')]}
        self.logger.debug("Successfully downloaded site %s, %s: response headers %s", site_id, content_type,
                          resp.headers)

        if "Content-Encoding" in list(resp.headers.keys()):
            if resp.headers['content-type'] == "application/xml":
                return resp.text, False, desc['filename']
            elif resp.headers['content-type'] == "application/octet-stream":
                return resp.content, True, desc['filename']
            else:
                return resp.text, False, desc['filename']
        else:
            return resp.text, False, desc['filename']

    def delete_content(self, site_id, content_id, content_type):
        """
        Delete content on Tableau Server.
        :return reponse after deleting.
        """
        if content_type == 'workbook':
            request_endpoint = "sites/{site_id}/workbooks/{workbook_id}".format(site_id=site_id,
                                                                                workbook_id=content_id)
        elif content_type == 'datasource':
            request_endpoint = "sites/{site_id}/datasources/{datasource_id}".format(site_id=site_id,
                                                                                    datasource_id=content_id)
        url = URL_TEMPLATE.format(host=self.host, request=request_endpoint)

        self.logger.debug("DELETE to %s", url)
        resp = requests.delete(url, headers={TAB_HEADER: self.cred_token},verify=False)

        self.check_response(resp)
        self.logger.debug("Successfully deleted site %s, workbook %s", site_id, content_id)
        return resp

    def check_response(self, resp):
        """
        Check Response of API
        :param resp:response object of any Interface.
        :return: None
        """
        if resp.status_code < 300:
            return

        try:
            resp_elmt = etree.fromstring(resp.content)
            error_elmt = resp_elmt.find('tab_ns:error', XML_NS)
            summary_elmt = error_elmt.find('tab_ns:summary', XML_NS)
            detail_elmt = error_elmt.find('tab_ns:detail', XML_NS)
            self.logger.error("API error %s (HTTP status code %s): %s/%s", error_elmt.attrib['code'], resp.status_code,
                              summary_elmt.text, detail_elmt.text)
        except:
            self.logger.exception("Error processing API error response: %s", resp.content)
        finally:
            raise RuntimeError("API error {}".format(resp.status_code))
