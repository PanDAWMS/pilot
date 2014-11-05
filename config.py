#config.py
"""File to store configuration information"""

#class configSiteMover(object):
#    __slots__ = ["COMMAND_MD5", "PERMISSIONS_DIR"]

class configSiteMover:
    COMMAND_MD5 = 'md5sum' # on Linux
    # COMMAND_MD5 = 'md5 -q' # on Mac OSX
    PERMISSIONS_DIR = 0775
    PERMISSIONS_FILE = 0644
    #Default archival type
    ARCH_DEFAULT = ''

config_sm = configSiteMover()
del configSiteMover
