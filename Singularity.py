import re
import os

from pUtil import tolog, readpar, getExperiment


def extractSingularityOptions():
    """ Extract any singularity options from catchall """

    # e.g. catchall = "somestuff singularity_options=\'-B /etc/grid-security/certificates,/var/spool/slurmd,/cvmfs,/ceph/grid,/data0,/sys/fs/cgroup\'"
    catchall = readpar("catchall")
    tolog("catchall: %s" % catchall)
    pattern = re.compile(r"singularity\_options\=\'?\"?(.+)\'?\"?")
    found = re.findall(pattern, catchall)
    if len(found) > 0:
        singularity_options = found[0]
        if singularity_options.endswith("'") or singularity_options.endswith('"'):
            singularity_options = singularity_options[:-1]
    else:
        singularity_options = ""

    return singularity_options

def getFileSystemRootPath(experiment):
    """ Return the proper file system root path (cvmfs) """

    e = getExperiment(experiment)
    return e.getCVMFSPath()

def extractPlatformAndOS(platform):
    """ Extract the platform and OS substring from platform """
    # platform = "x86_64-slc6-gcc48-opt"
    # return "x86_64-slc6"
    # In case of failure, return the full platform

    pattern = r"([A-Za-z0-9_-]+)-.+-.+"
    a = re.findall(re.compile(pattern), platform)

    if len(a) > 0:
        ret = a[0]
    else:
        tolog("!!WARNING!!7777!! Could not extract architecture and OS substring using pattern=%s from platform=%s (will use %s for image name)" % (pattern, platform, platform))
        ret = platform

    return ret

def getGridImageForSingularity(platform, experiment):
    """ Return the full path to the singularity grid image """

    if not platform or platform == "":
        platform = "x86_64-slc6"
        tolog("!!WARNING!!3333!! Using default platform=%s (cmtconfig not set)" % (platform))

    arch_and_os = extractPlatformAndOS(platform)
    image = arch_and_os + ".img"
    tolog("Constructed image name %s from %s" % (image, platform))

    path = os.path.join(getFileSystemRootPath(experiment), "atlas.cern.ch/repo/images/singularity")
    return os.path.join(path, image)

def singularityWrapper(cmd, platform, experiment="ATLAS"):
    """ Prepend the given command with the singularity execution command """
    # E.g. cmd = /bin/bash hello_world.sh
    # -> singularity_command = singularity exec -B <bindmountsfromcatchall> <img> /bin/bash hello_world.sh
    # singularity exec -B <bindmountsfromcatchall>  /cvmfs/atlas.cern.ch/repo/images/singularity/x86_64-slc6.img <script> 

    # Get the singularity options from catchall field
    singularity_options = extractSingularityOptions()
    if singularity_options != "":
        # Get the image path
        image_path = getGridImageForSingularity(platform, experiment)

        # Does the image exist?
        if os.path.exists(image_path):
            # Prepend it to the given command
            cmd = "singularity exec " + singularity_options + " " + image_path + " \'" + cmd.replace("\'","\\'").replace('\"','\\"') + "\'"
        else:
            tolog("!!WARNING!!4444!! Singularity options found but image does not exist: %s" % (image_path))
    else:
        # Return the original command as it was
        tolog("No singularity options found in catchall field")
#        pass
    tolog("Singularity check: Using command %s" % (cmd))
    return cmd

if __name__ == "__main__":

    cmd = "<some command>"
    platform = "x86_64-slc6"
    print singularityWrapper(cmd, platform)

