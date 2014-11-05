from xml.sax import saxutils, handler

class JobInfoXML(handler.ContentHandler):
    """
    This class is used to read the jobInfo.xml file.
    It returns the transformation error code (exeErrorCode) and
    the corresponding error message(s) (exeErrorDiag). The latter
    has the format "(error acronym) | (error msg 1) | (error msg 2) | ..."
    """
    def __init__(self):
        """ Default init """
        self.acronym = ""
        self.code = 0
        self.inMessageContent = False
        self.message = ""
        self.messages = []

    def startElement(self, name, attrs):
        """ Overloaded start element reader """
        if name == 'errorcategory':
            self.acronym = attrs.get('acronym', None)
            self.code = int(attrs.get('code', None))
        elif name == 'message':
            self.inMessageContent = True
            self.message = ""

    def endElement(self, name):
        """ Overloaded end element reader """
        if name == 'message':
            self.inMessageContent = False
            
    def characters(self, ch):
        """ Overloaded character reader (i.e. element data) """
        if self.inMessageContent:
            self.message = self.message + ch
            self.messages.append(self.message)

    def getAcronym(self):
        """ Return error acronym """
        return self.acronym

    def getCode(self):
        """ Return error code """
        return self.code
           
    def getMessage(self):
        """ Return the error messages in one single string """
        m = self.messages
        l = len(m)
        if (l == 0):
            return ""
        elif (l == 1):
            return self.acronym + " | " + m[0]
        else:
            msg = self.acronym + " | "
            for i in range(0,l):
                msg += m[i]
                if (i < l-1):
                    msg += " | " # error message separator
            return msg
                                                                                                                   
