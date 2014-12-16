
import argparse
import logging
import os
import sys
from mpi4py import MPI

from pandayoda.yodacore import Yoda
from pandayoda.yodaexe import Droid

import logging
logging.basicConfig(level=logging.DEBUG)


def main(globalWorkDir, localWorkDir):
    comm = MPI.COMM_WORLD
    mpirank = comm.Get_rank()

    # Create separate working directory for each rank
    from os.path import abspath as _abspath, join as _join
    curdir = _abspath (localWorkDir)
    wkdirname = "rank_%s" % str(mpirank)
    wkdir  = _abspath (_join(curdir,wkdirname))
    if not os.path.exists(wkdir):
        os.makedirs (wkdir)
    os.chdir (wkdir)

    if mpirank==0:
        yoda = Yoda.Yoda(globalWorkDir, localWorkDir)
        yoda.run()
    else:
        droid = Droid.Droid(globalWorkDir, localWorkDir)
        droid.run()

if __name__ == "__main__":
    usage = """
usage: %(prog)s <command> [options] [args]

Commands:

    help <command>  Output help for one of the commands below

"""
    oparser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), add_help=True)
    oparser.add_argument('--globalWorkingDir', dest="globalWorkingDir", default=None, help="Global share working directory")
    oparser.add_argument('--localWorkingDir', dest="localWorkingDir", default=None, help="Local working directory. if it's not set, it will use global working directory")

    if len(sys.argv) == 1:
        oparser.print_help()
        sys.exit(-1)

    args = oparser.parse_args(sys.argv[1:])
    if args.globalWorkingDir is None:
        print "Global working directory is needed."
        oparser.print_help()
        sys.exit(-1)
    if args.localWorkingDir is None:
        args.localWorkingDir = args.globalWorkingDir

    try:
        main(args.globalWorkingDir, args.localWorkingDir)
        print "HPCJob-Yoda success"
        sys.exit(0)
    except Exception as e:
        print "HPCJob-Yoda failed"
        print(e)
        sys.exit(1)
