from futil import *
from pUtil import tolog

from SiteMover import SiteMover                             # OU_OCHEP_SWT2
from dCacheSiteMover import dCacheSiteMover                 # ANALY_AGLT2
from BNLdCacheSiteMover import BNLdCacheSiteMover           # None
from xrootdSiteMover import xrootdSiteMover                 # SLAC, GLOW-ATLAS
from xrdcpSiteMover import xrdcpSiteMover                   # ANALY_CERN_XROOTD
from CastorSiteMover import CastorSiteMover                 #
from dCacheLFCSiteMover import dCacheLFCSiteMover           # UBC
from lcgcpSiteMover import lcgcpSiteMover                   # LYON, CERN, MANC, LANCS, FZK
from lcgcp2SiteMover import lcgcp2SiteMover                 # US sites; AGLT2 
from stormSiteMover import stormSiteMover                   # Bologna
from mvSiteMover import mvSiteMover                         # NDGF
from HUSiteMover import HUSiteMover                         # None
from rfcpLFCSiteMover import rfcpLFCSiteMover               # GLASGOW (works for all DPM sites)
from castorSvcClassSiteMover import castorSvcClassSiteMover # RAL (needs extra configuation to map space tokens to service classes)
from LocalSiteMover import LocalSiteMover                   # HU, MWT2 
from ChirpSiteMover import ChirpSiteMover                   # Munich
from curlSiteMover import curlSiteMover                     # ASGC
from FAXSiteMover import FAXSiteMover                       # CVMFS sites
from objectstoreSiteMover import objectstoreSiteMover       #
from aria2cSiteMover import aria2cSiteMover                 #
from GFAL2SiteMover import GFAL2SiteMover                   # GFAL2
from GSIftpSiteMover import GSIftpSiteMover                 # HPC sites

mover_selector = {
    SiteMover.copyCommand : SiteMover,
    dCacheSiteMover.copyCommand : dCacheSiteMover,
    BNLdCacheSiteMover.copyCommand : BNLdCacheSiteMover,
    xrootdSiteMover.copyCommand : xrootdSiteMover,
    xrdcpSiteMover.copyCommand : xrdcpSiteMover,
    CastorSiteMover.copyCommand : CastorSiteMover,
    dCacheLFCSiteMover.copyCommand : dCacheLFCSiteMover,
    lcgcpSiteMover.copyCommand : lcgcpSiteMover,
    lcgcp2SiteMover.copyCommand : lcgcp2SiteMover,
    stormSiteMover.copyCommand : stormSiteMover,
    mvSiteMover.copyCommand : mvSiteMover,
    HUSiteMover.copyCommand : HUSiteMover,
    rfcpLFCSiteMover.copyCommand : rfcpLFCSiteMover,
    castorSvcClassSiteMover.copyCommand : castorSvcClassSiteMover,
    LocalSiteMover.copyCommand : LocalSiteMover,
    ChirpSiteMover.copyCommand : ChirpSiteMover,
    curlSiteMover.copyCommand : curlSiteMover,
    FAXSiteMover.copyCommand : FAXSiteMover,
    aria2cSiteMover.copyCommand : aria2cSiteMover,
    objectstoreSiteMover.copyCommand : objectstoreSiteMover,
    GFAL2SiteMover.copyCommand : GFAL2SiteMover,
    GSIftpSiteMover.copyCommand : GSIftpSiteMover
    }

def getSiteMover(sitemover, setup_file='', *args, **kwrds):
    """The setup file is singled out from the other arguments in case the farm method would like
    to chek its existence or use it. Anyway if no setup_file is assigned no unnamed arguments are passed (*args is empty)
    """
    tolog("Looking for site mover %s, setup:%s, ar:%s, kw:%s" % (sitemover, setup_file, args, kwrds))
    ret = None
    # Keyword automatic corrections
    if sitemover == 'gridftp':
        sitemover = 'gsiftp'
    elif sitemover == 'lcgcp':
        sitemover = 'lcg-cp'
    elif sitemover == 'lcgcp2':
        sitemover = 'lcg-cp2'
    try:
        ret = mover_selector[sitemover]
    except KeyError:
        tolog("!!WARNING!!2999!! Site mover %s not found (%s), returning SiteMover - using local copy"%(sitemover, mover_selector.keys()))
        ret = SiteMover
        # TODO: ? return OtherMover?
        return ret.getSiteMover(*args, **kwrds)
    tolog("Returning site mover %s (setup: %s)" % (ret, setup_file))
#    if not setup_file:
#        return ret(*args, **kwrds)
#    else:
#        # OK also return ret(*((setup_file,) + args), **kwrds)
    return ret(setup_file, *args, **kwrds)

MT_PLAIN = 0 # These are regular files manageable using the system IO
MT_DCAP = 1 # This includes local dCache access (dccp)  
MT_GFTP = 2 # This class includes http/ftp URLs, GSI authenticated or not and 'file://'+MT_PLAIN URLs
MT_SRM  = 3 # This class includes all SRM valid URLs srm://... + MT_GFTP + MT_DCAP (converted using some door)

def _getMoverType(url1):
    """It tries to guess the mover type from the URL string"""
    if not url1:
        typ1 = MT_PLAIN
    elif url1.startswith('/pnfs'): #TODO: add dcap/dcache
        typ1 = MT_DCAP
    elif url1.startswith('/') or url1.startswith('file://'):
        typ1 = MT_PLAIN
    elif url1.startswith('http://'):
        typ1 = MT_GFTP
    elif url1.startswith('gsiftp://'):
        typ1 = MT_GFTP
#    elif url1.startswith(''):
#        typ1 = MT_       
    elif url1.startswith('srm://'):
        typ1 = MT_SRM
    else:
        typ1 = MT_SRM
    return typ1

def _promoteURL(url, orig_mt, new_mt):
    """Gven the original URL and the original mover type it provides a URL compatible with the new mover type"""
    if orig_mt == new_mt:
        return url
    if url.startswith('/'):
        if new_mt == MT_PLAIN or new_mt == MT_DCAP:
            return url
        else:
            if orig_mt == MT_PLAIN:
                return 'file://'+url
            else:
                return 'dcache://'+url #TODO: will this work?
        

def chooseMover(url1, url2=None):
    mt1 = _getMoverType(url1)
    mt2 = _getMoverType(url2)
    mtres = mt1
    if mt2 != mt1:
        if mt2 > mt1:
            mtres = mt2
            mtalt = mt1
        else:
            mtalt = mt2        
        if mtres != MT_SRM:
            if mtres == MT_GFTP:
                if mtalt == MT_DCAP:
                    mtres = MT_SRM
        url1 = _promoteURL(url1, mt1, mtres)
        url2 = _promoteURL(url2, mt2, mtres)
    return mtres, url1, url2
        
"""    if not url1:
        typ1 = MT_PLAIN
    elif url1.startswith('/pnfs'):
        typ1 = MT_DCAP
    elif url1.startswith('/'):
        typ1 = MT_PLAIN
    elif url1.startswith('http://'):
        typ1 = MT_GFTP
    elif url1.startswith('gsiftp://'):
        typ1 = MT_GFTP
#    elif url1.startswith(''):
#        typ1 = MT_       
    elif url1.startswith('srm://'):
        typ1 = MT_SRM
    else:
        typ1 = MT_SRM
    resm = None
    for i in urls:
        if i.startswith('/pnfs'):
            if resm == 'cp':
                resm = 'dccp'
                continue
        elif i.startswith('/'):
            continue
        elif i.startswith('gsiftp')
        
        
    
    if 
    
    """

def test_comparefiles(fi1, fi2):
    import commands
    eec, eout = commands.getstatusoutput('diff %s %s' % (fi1, fi2))
    if eec == 0:
        print "** Copy succesful"
    elif eec == 256:
        print "** Copy failed, files are different (%s)" % eout
    elif eec == 512:
        print "** Copy failed, file not found (%s)" % eout
    else:
        print "** Copy failed, unknon reason %s (%s)" % (eec, eout)
    if eec < 2:
        eec, eout = commands.getstatusoutput('rm %s' % (fi2))        


def test():
    print "Producing test file"
    MYFNAME = 'transfertest1234.txt'
    DESTDNAME = '/tmp/dst/'
    myf = open(MYFNAME, 'w')
    myf.write('This is a test file that will be copied\n')
    myf.close()
    
    print "** Testing 'cp'"
    mym = getSiteMover('cp')
    ec1 = mym.put_data(MYFNAME, 'file:///tmp/xfer/')
    print "cp-put: %s" % str(ec1)
    ec2 = mym.get_data('file:///tmp/xfer/'+MYFNAME, DESTDNAME)
    print "cp-get: %s" % str(ec2)
    test_comparefiles(MYFNAME, DESTDNAME+MYFNAME)    

    print "Testing 'gsiftp'"
    mym = getSiteMover('gridftp')
    ec1 = mym.put_data(MYFNAME, 'gsiftp://tier2-04.uchicago.edu/share/data1/atlas_se_ddm', extradirs='xfertest')
    print "gridftp-put: %s" % str(ec1)
    ec2 = mym.get_data('gsiftp://tier2-04.uchicago.edu/share/data1/atlas_se_ddm/xfertest/%s' % MYFNAME, DESTDNAME)
    print "gridftp-get: %s" % str(ec2)
    test_comparefiles(MYFNAME, DESTDNAME)

    print "Test 'dccp'"
    mym = getSiteMover('dccp')
    ec1 = mym.put_data(MYFNAME, '/pnfs/uchicago.edu/data/usatlas/', extradirs='xfertest')
    print "dccp-put: %s" % str(ec1)
    ec2 = mym.get_data('gsiftp://tier2-04.uchicago.edu/pnfs/uchicago.edu/data/usatlas/xfertest/%s' % MYFNAME, DESTDNAME)
    print "dccp-get: %s" % str(ec2)
    test_comparefiles(MYFNAME, DESTDNAME)

if __name__ == '__main__':
    test()
