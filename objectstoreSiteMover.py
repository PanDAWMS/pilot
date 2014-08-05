#!/usr/bin/env python

# Copyright European Organization for Nuclear Research (CERN)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wguan@cern.ch>, 2014

# objectstoreSiteMover.py

import os
from config import config_sm

import SiteMover
from xrootdObjectstoreSiteMover import xrootdObjectstoreSiteMover
from S3ObjectstoreSiteMover import S3ObjectstoreSiteMover

class objectstoreSiteMover(SiteMover.SiteMover):
    """
    ObjectstoreSiteMover
    It uses the url to decide which ObjectstoreSiteMover implementation to be used.
    """
    copyCommand = "objectstore"
    checksum_command = "adler32"

    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        if gpfn.startswith("root:"):
            sitemover = xrootdObjectstoreSiteMover(self.getSetup())
            return sitemover.get_data(gpfn, lfn, path, fsize, fchecksum, guid, **pdict)
        if gpfn.startswith("s3:"):
            sitemover = S3ObjectstoreSiteMover(self.getSetup())
            return sitemover.get_data(gpfn, lfn, path, fsize, fchecksum, guid, **pdict)
        return -1, "No objectstore sitemover found for this scheme(%s)" % gpfn

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        logPath = pdict.get('logPath', '')
        if logPath != "":
            surl = logPath
        else:
            surl = os.path.join(destination, lfn)

        if surl.startswith("root:"):
            sitemover = xrootdObjectstoreSiteMover(self.getSetup())
            return sitemover. put_data(source, destination, fsize, fchecksum, **pdict)
        if surl.startswith("s3:"):
            sitemover = S3ObjectstoreSiteMover(self.getSetup())
            return sitemover. put_data(source, surl, fsize, fchecksum, **pdict)
        return -1, "No objectstore sitemover found for this scheme(%s)" % destination, destination, fsize, fchecksum, config_sm.ARCH_DEFAULT


if __name__ == '__main__':

    f = objectstoreSiteMover()

    gpfn = "nonsens_gpfn"
    lfn = "AOD.310713._000004.pool.root.1"
    path = os.getcwd()
    fsize = "4261010441"
    fchecksum = "9145af38"
    dsname = "data11_7TeV.00177986.physics_Egamma.merge.AOD.r2276_p516_p523_tid310713_00"
    report = {}

    #print f.getGlobalFilePaths(dsname)
    #print f.findGlobalFilePath(lfn, dsname)
    #print f.getLocalROOTSetup()

    #path = "root://atlas-objectstore.cern.ch//atlas/eventservice/2181626927" # + your .root filename"
    """
    source = "/bin/hostname"
    dest = "root://eosatlas.cern.ch//eos/atlas/unpledged/group-wisc/users/wguan/"
    lfn = "NTUP_PHOTON.01255150._000001.root.1"
    localSize = 17848
    localChecksum = "89b93830"
    print f.put_data(source, dest, fsize=localSize, fchecksum=localChecksum, prodSourceLabel='ptest', experiment='ATLAS', report =report, lfn=lfn, guid='aa8ee1ae-54a5-468b-a0a0-41cf17477ffc')

    gpfn = "root://eosatlas.cern.ch//eos/atlas/unpledged/group-wisc/users/wguan/NTUP_PHOTON.01255150._000001.root.1"
    lfn = "NTUP_PHOTON.01255150._000001.root.1"
    tmpDir = "/tmp/"
    localSize = 17848
    localChecksum = "89b93830"
    print f.get_data(gpfn, lfn, tmpDir, fsize=localSize, fchecksum=localChecksum, experiment='ATLAS', report =report, guid='aa8ee1ae-54a5-468b-a0a0-41cf17477ffc')
    """

    # test S3 object store
    source = "/bin/hostname"
    dest = "s3://ceph003.usatlas.bnl.gov:8443//wguan_bucket/dir1/dir2/NTUP_PHOTON.01255150._000001.root.1"
    lfn = "NTUP_PHOTON.01255150._000001.root.1"
    localSize = None
    localChecksum = None
    print f.put_data(source, dest, fsize=localSize, fchecksum=localChecksum, prodSourceLabel='ptest', experiment='ATLAS', report =report, lfn=lfn, guid='aa8ee1ae-54a5-468b-a0a0-41cf17477ffc')

    gpfn = "s3://ceph003.usatlas.bnl.gov:8443//wguan_bucket/dir1/dir2/NTUP_PHOTON.01255150._000001.root.1"
    lfn = "NTUP_PHOTON.01255150._000001.root.1"
    tmpDir = "/tmp/"
    localSize = None
    localChecksum = None
    print f.get_data(gpfn, lfn, tmpDir, fsize=localSize, fchecksum=localChecksum, experiment='ATLAS', report =report, guid='aa8ee1ae-54a5-468b-a0a0-41cf17477ffc')
