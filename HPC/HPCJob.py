
import argparse
import os
import sys
import traceback
from mpi4py import MPI


def main(globalWorkDir, localWorkDir):
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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
        try:
            from pandayoda.yodacore import Yoda
            yoda = Yoda.Yoda(globalWorkDir, localWorkDir)
            yoda.run()
            print "Rank %s: Yoda finished" % (mpirank)
        except:
            print "Rank %s: Yoda failed: %s" % (mpirank, traceback.format_exc())
        sys.exit(0)
    else:
        try:
            status = 0
            from pandayoda.yodaexe import Droid
            droid = Droid.Droid(globalWorkDir, localWorkDir)
            droid.run()
            # parent process
            #pid, status = os.waitpid(child_pid, 0)
            print "Rank %s: Droid finished status: %s" % (mpirank, status)
        except:
            print "Rank %s: Droid failed: %s" % (mpirank, traceback.format_exc())
        #sys.exit(0)
    return mpirank

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

    rank = None
    try:
        rank = main(args.globalWorkingDir, args.localWorkingDir)
        print "Rank %s: HPCJob-Yoda success" % rank
        #sys.exit(0)
    except Exception as e:
        print "Rank %s: HPCJob-Yoda failed" % rank
        print(e)
        #sys.exit(0)
    #os._exit(0)
