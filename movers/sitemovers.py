"""
  This file contains the list of ENABLED site movers
"""

from .xrdcp_sitemover import xrdcpSiteMover
from .dcache_sitemover import dcacheSiteMover
from .lcgcp_sitemover import lcgcpSiteMover
from .mv_sitemover import mvSiteMover

from .rucio_sitemover import rucioSiteMover
from .lsm_sitemover import lsmSiteMover
from .objectstore_sitemover import objectstoreSiteMover
from .storm_sitemover import stormSiteMover
from .gfalcopy_sitemover import gfalcopySiteMover
