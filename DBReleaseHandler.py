import os

from PilotErrors import PilotErrors
from pUtil import tolog, readpar

class DBReleaseHandler:
    """
    Methods for handling the DBRelease file and possibly skip it in the input file list
    In the presence of $[VO_ATLAS_SW_DIR|OSG_APP]/database, the pilot will use these methods to:
    1. Extract the requested DBRelease version from the job parameters string, if present
    2. Scan the $[VO_ATLAS_SW_DIR|OSG_APP]/database dir for available DBRelease files
    3. If the requested DBRelease file is available, continue [else, abort at this point]
    4. Create a DBRelease setup file containing necessary environment variables
    5. Create a new DBRelease file only containing the setup file in the input file directory
    6. Update the job state file
    7. Remove the DBRelease file from the input file list if all previous steps finished correctly
    """

    # private data members
    __error = PilotErrors() # PilotErrors object
    __version = ""
    __DBReleaseDir = ""
    __filename = "DBRelease-%s.tar.gz"
    __setupFilename = "setup.py"
    __workdir = ""

    def __init__(self, workdir=""):
        """ Default initialization """

        _path = self.getDBReleaseDir() # _path is a dummy variable
        self.__workdir = workdir

    def removeDBRelease(self, inputFiles, inFilesGuids, realDatasetsIn, dispatchDblock, dispatchDBlockToken, prodDBlockToken):
        """ remove the given DBRelease files from the input file list """
        # will only remove the DBRelease files that are already available locally

        # identify all DBRelease files in the list (mark all for removal)
        # note: multi-trf jobs tend to have the same DBRelease file listed twice
        position = 0
        positions_list = []
        for f in inputFiles:
            if "DBRelease" in f:
                positions_list.append(position)
                tolog("Will remove file %s from input file list" % (f))
            position += 1

        # remove the corresponding guids, datasets and tokens
        for position in positions_list:
            try:
                del(inputFiles[position])
            except Exception, e:
                tolog("!!WARNING!!1990!! Could not delete object %d in inFiles: %s" % (position, str(e)))
            else:
                tolog("Removed item %d in inFiles" % (position))
            try:
                del(inFilesGuids[position])
            except Exception, e:
                tolog("!!WARNING!!1990!! Could not delete object %d in inFilesGuids: %s" % (position, str(e)))
            else:
                tolog("Removed item %d in inFilesGuids" % (position))
            try:
                del(realDatasetsIn[position])
            except Exception, e:
                tolog("!!WARNING!!1990!! Could not delete object %d in realDatasetsIn: %s" % (position, str(e)))
            else:
                tolog("Removed item %d in realDatasetsIn" % (position))
            try:
                del(dispatchDblock[position])
            except Exception, e:
                tolog("!!WARNING!!1990!! Could not delete object %d in dispatchDblock: %s" % (position, str(e)))
            else:
                tolog("Removed item %d in dispatchDblock" % (position))
            try:
                del(dispatchDBlockToken[position])
            except Exception, e:
                tolog("!!WARNING!!1990!! Could not delete object %d in dispatchDBlockToken: %s" % (position, str(e)))
            else:
                tolog("Removed item %d in dispatchDBlockToken" % (position))
            try:
                del(prodDBlockToken[position])
            except Exception, e:
                tolog("!!WARNING!!1990!! Could not delete object %d in prodDBlockToken: %s" % (position, str(e)))
            else:
                tolog("Removed item %d in prodDBlockToken" % (position))

        return inputFiles, inFilesGuids, realDatasetsIn, dispatchDblock, dispatchDBlockToken, prodDBlockToken

    def extractVersion(self, name):
        """ Try to extract the version from the string name """

        version = ""

        import re
        re_v = re.compile('DBRelease-(\d+\.\d+\.\d+)\.tar\.gz')
        v = re_v.search(name)
        if v:
            version = v.group(1)
        else:
            re_v = re.compile('DBRelease-(\d+\.\d+\.\d+\.\d+)\.tar\.gz')
            v = re_v.search(name)
            if v:
                version = v.group(1)

        return version

    def getDBReleaseVersion(self, jobPars=""):
        """ Get the DBRelease version from the job parameters string """

        version = ""

        # get the version from the job parameters
        if jobPars != "":
            version = self.extractVersion(jobPars)
        else:
            # get the version from private data member (already set earlier)
            version = self.__version

        return version

    def getDBReleaseDir(self):
        """ Return the proper DBRelease directory """

        if os.environ.has_key('VO_ATLAS_SW_DIR'):
            path = os.path.expandvars('$VO_ATLAS_SW_DIR/database/DBRelease')
        else:
            path = os.path.expandvars('$OSG_APP/database/DBRelease')
        if path == "" or path.startswith('OSG_APP'):
            tolog("Note: The DBRelease database directory is not available (will not attempt to skip DBRelease stage-in)")
        else:
            if os.path.exists(path):
                tolog("Local DBRelease path verified: %s (will attempt to skip DBRelease stage-in)" % (path))
                self.__DBReleaseDir = path
            else:
                tolog("Note: Local DBRelease path does not exist: %s (will not attempt to skip DBRelease stage-in)" % (path))
                path = ""
        return path

    def isDBReleaseAvailable(self, versionFromJobPars):
        """ Check whether a given DBRelease file is already available """

        status = False
        self.__version = versionFromJobPars

        # do not proceed if
        if os.environ.has_key('ATLAS_DBREL_DWNLD'):
            tolog("ATLAS_DBREL_DWNLD is set: do not skip DBRelease stage-in")
            return status

        # get the local path to the DBRelease directory
        path = self.getDBReleaseDir()

        if path != "":
            if os.path.exists(path):
                # get the list of available DBRelease directories
                dir_list = os.listdir(path)

                # is the required DBRelease version available?
                if dir_list != []:
                    if self.__version != "":
                        if self.__version in dir_list:
                            tolog("Found version %s in path %s (%d releases found)" % (self.__version, path, len(dir_list)))
                            status = True
                        else:
                            tolog("Did not find version %s in path %s (%d releases found)" % (self.__version, path, len(dir_list)))
                else:
                    tolog("Empty directory list: %s" % (path))
        return status

    def createSetupFile(self, version, path):
        """ Create the DBRelease setup file """

        status = False

        # get the DBRelease directory
        d = self.__DBReleaseDir
        if d != "" and version != "":
            # create the python code string to be written to file
            txt = "import os\n"
            txt += "os.environ['DBRELEASE'] = '%s'\n" % (version)
            txt += "os.environ['DATAPATH'] = '%s/%s:' + os.environ['DATAPATH']\n" % (d, version)
            txt += "os.environ['DBRELEASE_REQUIRED'] = '%s'\n" % (version)
            txt += "os.environ['DBRELEASE_REQUESTED'] = '%s'\n" % (version)
            txt += "os.environ['CORAL_DBLOOKUP_PATH'] = '%s/%s/XMLConfig'\n" % (d, version)
            try:
                f = open(os.path.join(path, self.__setupFilename), "w")
            except OSError, e:
                tolog("!!WARNING!!1990!! Failed to create DBRelease %s: %s" % (self.__setupFilename, str(e)))
            else:
                f.write(txt)
                f.close()
                tolog("Created setup file with the following content:.................................\n%s" % (txt))
                tolog("...............................................................................")
                status = True
        return status

    def mkdirs(self, path, d):
        """ Create directory d in path """

        status = False

        try:
            _dir = os.path.join(path, d)
            os.makedirs(_dir)
        except OSError, e:
            tolog("!!WARNING!!1990!! Failed to create directories %s: %s" % (_dir, str(e)))
        else:
            status = True

        return status

    def rmdirs(self, path):
        """ Remove directory in path """

        status = False

        try:
            from shutil import rmtree
            rmtree(path)
        except OSError, e:
            tolog("!!WARNING!!1990!! Failed to remove directories %s: %s" % (path, str(e)))
        else:
            status = True

        return status

    def createDBRelease(self, version, path):
        """ Create the DBRelease file only containing a setup file """

        status = False

        # create the DBRelease and version directories
        DBRelease_path = os.path.join(path, 'DBRelease')        
        if self.mkdirs(DBRelease_path, version):

            # create the setup file in the DBRelease directory
            version_path = os.path.join(DBRelease_path, version)
            if self.createSetupFile(version, version_path):
                tolog("Created DBRelease %s in new directory %s" % (self.__setupFilename, version_path))

                # now create a new DBRelease tarball
                filename = os.path.join(path, self.__filename % (version))
                import tarfile
                tolog("Attempting to create %s" % (filename))
                try:
                    tar = tarfile.open(filename, "w:gz")
                except Exception, e:
                    tolog("!!WARNING!!1990!! Could not create DBRelease tar file: %s" % str(e))
                else:
                    if tar:
                        # add the setup file to the tar file
                        tar.add("%s/DBRelease/%s/%s" % (path, version, self.__setupFilename))

                        # create the symbolic link DBRelease/current ->  12.2.1
                        try:
                            _link = os.path.join(path, "DBRelease/current")
                            os.symlink(version, _link)
                        except Exception, e:
                            tolog("!!WARNING!!1990!! Failed to create symbolic link %s: %s" % (_link, str(e)))
                        else:
                            tolog("Created symbolic link %s" % (_link))

                            # add the symbolic link to the tar file
                            tar.add(_link)

                            # done with the tar archive
                            tar.close()

                            tolog("Created new DBRelease tar file: %s" % filename)
                            status = True
                    else:
                        tolog("!!WARNING!!1990!! Failed to create DBRelease tar file")

                # clean up
                if self.rmdirs(DBRelease_path):
                    tolog("Cleaned up directories in path %s" % (DBRelease_path))
            else:
                tolog("Failed to create DBRelease %s" % (self.__setupFilename))
                if self.rmdirs(DBRelease_path):
                    tolog("Cleaned up directories in path %s" % (DBRelease_path))

        return status

if __name__ == "__main__":

    h = DBReleaseHandler()
    
    jobPars="jobParametersInputEvgenFile=EVNT.162766._000363.pool.root.1 OutputHitsFile=6daee77d-1d29-4901-a04e-d532a5e1812f_1.HITS.pool.root MaxEvents=2 SkipEvents=0 RandomSeed=164832 GeometryVersion=ATLAS-GEO-16-00-00 PhysicsList=QGSP_BERT JobConfig=VertexFromCondDB.py,jobConfig.LooperKiller.py,CalHits.py,jobConfig.LucidOn.py DBRelease=DBRelease-12.2.1.tar.gz ConditionsTag=OFLCOND-SDR-BS7T-02 IgnoreConfigError=False AMITag=s932"

    version = h.getDBReleaseVersion(jobPars=jobPars)
    tolog("Version = %s" % (version))

    if h.createDBRelease(version, os.getcwd()):
        pass
