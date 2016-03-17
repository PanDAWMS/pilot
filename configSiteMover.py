# File to store configuration information

class configSiteMover:
    COMMAND_MD5 = 'md5sum'
    COMMAND_AD = 'adler'
    PERMISSIONS_DIR = 0775
    PERMISSIONS_FILE = 0644
    ARCH_DEFAULT = ''

config_sm = configSiteMover()
del configSiteMover
