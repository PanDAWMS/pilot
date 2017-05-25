import os, time

# NOTE: should this be incorporated into new SiteInformation class?

class Site:
    """ site information """

    def setSiteInfo(self, t1=None):
        """ Set the current site info """
        # t1 is a tuple of (sitename, appdir, tmpdir, queuename)

        self.sitename = t1[0]            # site name
        self.appdir = t1[1]              # APPDIR
        self.wntmpdir = t1[2]            # tmp dir
        self.workdir = self.getWorkDir() # site workdir
        self.computingElement = t1[3]    # queuename

    def getWorkDir(self):
        """ When there are multi-jobs, the site workdir needs to be updated """

        jobworkdir = "Panda_Pilot_%d_%s" % (os.getpid(), str(int(time.time())))
        return os.path.join(self.wntmpdir, jobworkdir) # this pilots' workdir
