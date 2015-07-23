"""
  Default Site Mover implementation based on simple `cp` command
  :author: Alexey Anisenkov
"""

from .base import BaseSiteMover

class SiteMover(BaseSiteMover):
    """
        This is the Default SiteMover, the SE has to be locally accessible for all the WNs
        and all commands like cp, mkdir, md5checksum have to be available on files in the SE
        E.g. NFS exported file system
    """

    #name = "cp"
    copyCommand = "cp"


class SiteMover2(BaseSiteMover):

    name = "cp2"
    copyCommand = "cp"
