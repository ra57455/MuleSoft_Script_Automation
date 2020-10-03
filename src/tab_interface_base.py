import abc 

class TabInterfaceBase(object):
    """ This is the base class for interfacing with Tableau. It can be implemented
        using REST API or tabcmd
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(config):
        pass

    @abc.abstractmethod
    def sign_in(username, password, site=""): 
        pass

    @abc.abstractmethod
    def is_signed_in(self):
        pass

    @abc.abstractmethod
    def query_sites(self):
        pass

    @abc.abstractmethod
    def sign_out(self):
        pass

    @abc.abstractmethod
    def download_workbook(self):
        pass

    @abc.abstractmethod
    def download_content(self):
        pass

    @abc.abstractmethod
    def delete_workbook(self):
        pass

    @abc.abstractmethod
    def delete_content(self):
        pass

    @abc.abstractmethod
    def query_workbook(self):
        pass
    
    @abc.abstractmethod
    def query_user(self):
        pass

