
import argparse
import logging
import os
import sys
import traceback

logging.basicConfig(filename='HPCJob.log', level=logging.DEBUG)


def main(globalWorkDir, localWorkDir, nonMPIMode=False):
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

    if nonMPIMode:
        comm = None
        mpirank = 0
        mpisize = 1
    else:
        try:
            from mpi4py import MPI
            comm = MPI.COMM_WORLD
            mpirank = comm.Get_rank()
            mpisize = comm.Get_size()
        except:
            print "Failed to load mpi4py: %s" % (traceback.format_exc())
            sys.exit(1)

    # Create separate working directory for each rank
    from os.path import abspath as _abspath, join as _join
    curdir = _abspath (localWorkDir)
    wkdirname = "rank_%s" % str(mpirank)
    wkdir  = _abspath (_join(curdir,wkdirname))
    if not os.path.exists(wkdir):
        os.makedirs (wkdir)
    os.chdir (wkdir)

    print "GlobalWorkDir: %s" % globalWorkDir
    print "LocalWorkDir: %s" % localWorkDir
    print "RANK: %s" % mpirank
    if mpirank==0:
        try:
            from pandayoda.yodacore import Yoda
            yoda = Yoda.Yoda(globalWorkDir, localWorkDir, rank=0, nonMPIMode=nonMPIMode)
            yoda.start()

            from pandayoda.yodaexe import Droid
            reserveCores = 0
            if mpisize > 500:
                reserveCores = 4
            elif mpisize > 200:
                reserveCores = 3
            elif mpisize > 100:
                reserveCores = 2
            elif mpisize > 50:
                reserveCores = 1
            droid = Droid.Droid(globalWorkDir, localWorkDir, rank=0, nonMPIMode=True, reserveCores=reserveCores)
            droid.start()

            while True:
                yoda.join(timeout=1)
                if yoda and yoda.isAlive():
                    pass
                else:
                    break
            print "Rank %s: Yoda finished" % (mpirank)
        except:
            print "Rank %s: Yoda failed: %s" % (mpirank, traceback.format_exc())
        sys.exit(0)
    else:
        try:
            status = 0
            from pandayoda.yodaexe import Droid
            droid = Droid.Droid(globalWorkDir, localWorkDir, rank=mpirank, nonMPIMode=nonMPIMode)
            droid.start()
            while (droid and droid.isAlive()):
                droid.join(timeout=1)
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
    oparser.add_argument('--nonMPIMode', default=False, action='store_true', help="Run Yoda in non-MPI mode")
    oparser.add_argument('--verbose', '-v', default=False, action='store_true', help="Print more verbose output.")

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
        print "Start HPCJob"
        rank = main(args.globalWorkingDir, args.localWorkingDir, args.nonMPIMode)
        print "Rank %s: HPCJob-Yoda success" % rank
        #sys.exit(0)
    except Exception as e:
        print "Rank %s: HPCJob-Yoda failed" % rank
        print(e)
        print(traceback.format_exc())
        #sys.exit(0)
    #os._exit(0)
