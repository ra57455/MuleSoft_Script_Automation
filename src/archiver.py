################################################################################
# Implementation of core Tableau archiver logic
################################################################################


import logging
from datetime import datetime as dt
import os
import os.path
import zipfile
import pystache
import itertools
import operator
import smtplib
import csv
import io
import datetime
from email.utils import COMMASPACE
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart



class ContentArchiver(object):

    def __init__(self, interface, confs):
        """
        Initilaize object to with tabarchiver.
        :param interface:
        :param confs:
        """
        self.isdeletearchive = confs['isdeletearchive']
        self.interface = interface
        if self.isdeletearchive:
            try:
                self.cleanup(confs['archive_dir'],confs['days'])
            except:
                self.cleanup(confs['archive_dir'])
            self.archive_dir = confs['archive_dir'] + "/" + dt.now().strftime("%d_%m_%Y_%H_%M_%S")

        #self.warning_email_path = confs['warning_email_path']
        #self.warning_email_template = confs['warning_email_template']

        self.archiving_email_path = confs['archiving_email_path']
        self.archiving_email_template = confs['archiving_email_template']

        self.email_cc_list = confs['email_cc_list']
        self.email_server=confs['email_server']

        self.issendemail = confs['issendemail']
        self.isdeletefiles = confs['isdeletefiles']
        self.isdownloadfiles = confs['isdownloadfiles']


        # be resilient to scalar input

        if not isinstance(self.email_cc_list, list):
            self.email_cc_list = [self.email_cc_list]

        self.email_from = confs['email_from']
        self.export_path = confs['export_dir']
        self.logger = logging.getLogger('tab-archiver-logger')
        self.logger.debug("Created Archiver object")

    def cleanup(self,path_to_delete,days=30):
        deleted_folders_count = 0
        deleted_files_count = 0

        def get_file_or_folder_age(path):
            # getting ctime of the file/folder
            # time will be in seconds
            ctime = os.stat(path).st_ctime

            # returning the time
            return ctime

        def remove_folder(path):

            # removing the folder
            if not shutil.rmtree(path):

                # success message
                print(f"{path_to_delete} is removed successfully")

            else:

                # failure message
                print(f"Unable to delete the {path}")

        def remove_file(path):

            # removing the file
            if not os.remove(path):

                # success message
                print(f"{path} is removed successfully")

            else:

                # failure message
                print(f"Unable to delete the {path}")
        import time,shutil
        seconds = time.time() - (days * 24 * 60 * 60)

        if os.path.exists(path_to_delete):
            for root_folder , folders , files in os.walk(path_to_delete):
                if seconds >= get_file_or_folder_age(path_to_delete):
                    # removing the folder
                    remove_folder(root_folder)
                    deleted_folders_count += 1  # incrementing count

                    # breaking after removing the root_folder
                    break
                else:
                    # checking folder from the root_folder
                    for folder in folders:

                        # folder path
                        folder_path = os.path.join(root_folder, folder)

                        # comparing with the days
                        if seconds >= get_file_or_folder_age(folder_path):
                            # invoking the remove_folder function
                            remove_folder(folder_path)
                            deleted_folders_count += 1  # incrementing count

                    # checking the current directory files
                    for file in files:

                        # file path
                        file_path = os.path.join(root_folder, file)

                        # comparing the days
                        if seconds >= get_file_or_folder_age(file_path):
                            # invoking the remove_file function
                            remove_file(file_path)
                            deleted_files_count += 1  # incrementing count

                        else:

                # if the path is not a directory
                # comparing with the days
                            if seconds >= get_file_or_folder_age(path_to_delete):
                                # invoking the file
                                remove_file(path_to_delete)
                                deleted_files_count += 1  # incrementing count

            else:

                # file/folder is not found
                print(f'"{path_to_delete}" is not found')
                deleted_files_count += 1  # incrementing count

            print(f"Total folders deleted: {deleted_folders_count}")
            print(f"Total files deleted: {deleted_files_count}")

    def process_content(self, content, content_type):
        """
        Download workbook / Datasource , archive it , email it , Delete it.
        :param content: content details.
        :param content_type: TYpe of file.
        """
        wb_deleted_list = []
        wb_warning_list = []
        grouped_contents = itertools.groupby(content, operator.itemgetter('site_name'))

        for group_content in grouped_contents:
            try:
                for doc in group_content[1]:
                    if not self.interface.is_signed_in():
                        # Note: failure to sign in to a given site will skip all workbooks for this site.
                        # Changed to SITE_ID as SITE_NAME will not work
                        self.interface.sign_in(doc['site_id']) #Sign in to your site.

                    extracted_files = []
                    #if doc['inactive_days'] >= self.archiving_days:
                    try:
                        #Download files
                        if self.isdownloadfiles: #Check download flag.
                            content, compressed, fn = self.interface.download_content(doc['site_luid'],
                                                                                      doc['document_luid'],
                                                                                      content_type)  #Download data.
                            self.logger.debug("Got content of %s, compressed? %s", fn, compressed)
                            extracted_files = self.extract_store(content, compressed, doc['site_name'],
                                                                 doc['project_name'], fn) #Get data and store in file.
                            wb_deleted_list.append((doc, extracted_files))

                        else:
                            self.logger.debug(
                                "%s %s, %s not being downloaded" % (
                                content_type, doc['document_name'], doc['document_luid']))

                        if self.isdeletefiles and len(extracted_files) > 0: #Checking flag for deletion and deleting if it is true and files are present
                            self.logger.debug("Number of files extracted %s" % (len(extracted_files)))
                            self.interface.delete_content(doc['site_luid'], doc['document_luid'], content_type) #Deleting in Tableau Server.
                            wb_deleted_list.append((doc, extracted_files))
                        else:
                            self.logger.debug(
                                "%s %s, %s not being deleted" % (
                                content_type, doc['document_name'], doc['document_luid']))

                    except Exception as ex:
                        self.logger.exception("Exception downloading and archiving %s %s", content_type, doc)
                        self.logger.exception(str(ex))

                    # elif doc['inactive_days'] > self.warning_days:
                    #     wb_warning_list.append((doc, None))
            except Exception as ex:
                self.logger.exception("Exception processing %s group", content_type)
                self.logger.exception(str(ex))
            finally:
                if self.interface.is_signed_in():
                    self.interface.sign_out()

        self.logger.debug("Deleted the following %s: %s", content_type, wb_deleted_list)
        #self.logger.debug("Generated inactivity alerts for following %s: %s", content_type, wb_warning_list)

        # added to run the program in readonly mode

        if self.issendemail:
            self.send_alerts(wb_deleted_list, #Sending emaila lert
                             self._get_template(self.archiving_email_path, self.archiving_email_template), content_type)
            # self.send_alerts(wb_warning_list, self._get_template(self.warning_email_path, self.warning_email_template), content_type)
        else:
            self.logger.debug("Not sending warning/archiving email to user as per configuration")

    def extract_store(self, content, compressed, site_name, project_name, fn):
        """
        Extracting dataa nd storing it locally.
        """

        def store_directly(_content, _archive_dir, _fn):
            with open(os.path.join(_archive_dir, _fn), "w") as fh:
                fh.write(_content)
                self.logger.debug("Saved the content file directly to %s", fh.name)

        archive_dir = os.path.join(self.archive_dir, self._sanitize_path(site_name), self._sanitize_path(project_name)) #path to store in local.
        if not os.path.exists(archive_dir): #Check for dir.
            os.makedirs(archive_dir)
        extracted_files = []
        try:
            if compressed:
                zf = zipfile.ZipFile(io.BytesIO(content), mode="r", compression=zipfile.ZIP_DEFLATED,
                                     allowZip64=True)  # Python 3.5
                self.logger.debug("Files in zip %s" % (', '.join([str(f) for f in zf.filelist])))
                files = [f for f in zf.filelist if f.filename.endswith(".twb") or f.filename.endswith(".tds")]
                for f in files:
                    # In most cases there will only be one file
                    zf.extract(f, archive_dir)
                    extracted_files.append(os.path.join(archive_dir, f.filename))
            else:
                store_directly(content, archive_dir, fn)  # Python 3.5
                extracted_files.append(os.path.join(archive_dir, fn))
        except:
            self.logger.exception("Failed to uncompress and storing the file to the filesystem")

        self.logger.debug("Stored content into %s: %s", archive_dir, extracted_files)
        return extracted_files

    def send_alerts(self, content_list, email_template, content_type):
        """
        Fetching email related data and Triggering email.
        :param content_list: List of content
        :param email_template: Template of email.
        :param content_type: Type of content.
        :return:
        """
        renderer = pystache.Renderer()
        content_list_sorted = sorted(content_list, key=lambda wbt: wbt[0]['user_name'])  # Python 3.5
        for k, wbg in itertools.groupby(content_list_sorted, lambda wbt: wbt[0]['user_name']):
            user_email = ""
            user_name = ""
            documents = []
            attached_files = []
            for wb, extracted_files in wbg: #Fetching email accordingly.
                if not user_name:
                    user_name = wb['user_name']
                    if wb['user_name'].isdigit():
                        yes = False
                    else:
                        self.logger.debug(wb['user_email'])
                        yes = True
                    if wb['user_email'] == '':
                        user_name = 'admin'
                        user_email = ''
                    else:
                        #ldap_info = self._get_from_ldap(user_name, 'mail')
                        if len(user_name)<1:
                            user_name = 'user_name'
                        try:
                            #user_email = wb["user_email"]
                            user_email  = "raghavender.allam@salesforce.com"  # Send to Raghav if e-mail is not present.
                        except:
                            user_email = "raghavender.allam@salesforce.com"  # Send to Raghav if e-mail is not present.
                        # try:
                        #     if not ldap_info:
                        #         user_name = 'GBI Tableau Support'
                        #         user_email = 'kns2@apple.com'
                        #     user_name = str(ldap_info.get('cn'))
                        #     user_email = ldap_info.get('mail', wb['user_email'])
                        #     #user_email = 'kns2@apple.com'
                        #     if user_name.find("(V)"):
                        #         user_name = user_name.replace("(V)", "").strip()
                        # except:
                        #     self.logger.debug("Failed to retrive user name or email from LDAP")
                documents.append({ #Storing  content.
                    'site_name': wb['site_name'],
                    'project_name': wb['project_name'],
                    'document_name': wb['document_name'].encode('UTF-8'),
                    'document_type': str(content_type).title(),
                    'inactive_days': wb['inactive_days'],
                    #'size': format((wb['size']/(1024*1024)),'.2f')  ##added to provide workbook size to the email template
                    'size': '{:0,.2f}'.format(wb['size']/(1024*1024))
                })
                if extracted_files:
                    attached_files.extend(extracted_files)
            context = {
                'user_name': wb['friendly_name'],
                'documents': documents} #Dictionary of user details and content
            content = renderer.render(email_template['body'], context)
            self.logger.debug("Sending alert to user %s, email %s:\n%s\nattached files: %s", user_name, user_email,
                              context, attached_files)
            try:
                if not user_email:
                    raise ValueError("Invalid email")
                else:
                    self.logger.debug("Sending Email")
                    self._send_email( [user_email] + self.email_cc_list,self.email_cc_list , self.email_from, email_template['subject'],
                                     content, attached_files)
            except:
                self.logger.exception("Unable to send alert to user %s / email %s", user_name, user_email)

    def exportcsv(self, content):
        """
        Create CSV file ,Write header to file. , Write content to file.
        :param content:
        :return:
        """
        self.logger.debug("Inside export info")
        csvfilename = "Archived_Workbook_{0}.csv".format(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
        csvpath = os.path.join(self.export_path, csvfilename)
        with open(csvpath, 'w') as csvfile: #Create CSV file.
            exportcsv = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            exportcsv.writerow(['Site_Id', #Write header into CSV file.
                                'Site_Name', 'Site_Luid',
                                'Project_Name', 'Project_Luid',
                                'Document_Name', 'Document_Luid',
                                'Workbook_Size', 'Type', 'Repo_URL',
                                'User_Name', 'User_Email',
                                'Inactive_Days'
                                ])
            for row in content: #Write content into CSV file.
                exportcsv.writerow([row['site_id'], row['site_name'], row['site_luid'],
                                    row['project_name'], row['project_luid'],
                                    row['document_name'], row['document_luid'],
                                    row['size'], row['type'], row['repo_url'],
                                    row['user_name'], row['user_email'], row['inactive_days']])
            csvfile.close()

    def _send_email(self, email_to_list, email_cc_list ,email_from, subject, content, attached_files):
        """
        Sending e-mail with this method.
        :param email_to_list: e-mail recipients.
        :param email_from: e-mail sender.
        :param subject: e-mail subject.
        :param content: e-mail content.
        :param attached_files: files attached with email.
        :return:
        """
        msg = MIMEMultipart() #Create Email body object.
        msg['Subject'] = subject
        msg['To'] = COMMASPACE.join(email_to_list)
        msg['Cc'] = COMMASPACE.join(email_cc_list)
        msg['From'] = email_from
        msg.attach(MIMEText(content, 'html'))
        for af in attached_files:
            self.logger.debug(af)
            with open(af, "rb") as fh:
                msg.attach(     #Attach Email body object.
                    MIMEApplication(fh.read(), Content_Disposition='attachment; filename="%s"' % os.path.basename(af),
                                    Name=os.path.basename(af)))
        sender = smtplib.SMTP(self.email_server)  #Choose SMTP Server
        sender.sendmail(email_from, email_to_list, msg.as_string())  #Send Email.
        sender.quit() #Quit
    """
    def _get_from_ldap(self, dsid, itemname):
        try:
            shargs = ['ldapsearch', '-xLLL', '-h', 'lookup.apple.com', '-b', 'ou=People,o=apple',
                      'appleDSID={}'.format(dsid), itemname, 'cn']

            self.logger.debug("Attempting to retrieve from LDAP: %s", shargs)
            res = subprocess.check_output(shargs)
            self.logger.debug(res.decode("utf-8"))
            res_dict = {l[0].strip(): l[1].strip() for l in
                        [v.split(':') for v in res.decode("utf-8").strip('\n').split('\n')]}  # Python 3.5
            self.logger.debug("Retrieved: %s", res_dict)
            return res_dict
        except:
            self.logger.debug("Attempting to retrieve from LDAP: %s failed", shargs)
    """
    def _sanitize_path(self, path_part):


        return "".join([c if c not in "/:" else "_" for c in path_part])

    def _get_template(self, template_path, template):


        if template_path:
            template['body'] = "\n".join(open(template_path['body']).readlines())
            template['subject'] = template_path['subject']

        return template
