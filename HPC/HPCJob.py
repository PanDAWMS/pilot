
import argparse
import logging
import os
import sys
import time
import traceback
from mpi4py import MPI
logger = logging.getLogger(__name__)

def main(globalWorkDir, localWorkDir, nonMPIMode=False, outputDir=None, dumpEventOutputs=True):
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

    if nonMPIMode:
        comm = None
        mpirank = 0
        mpisize = 1
    else:
        try:
            comm = MPI.COMM_WORLD
            mpirank = comm.Get_rank()
            mpisize = comm.Get_size()
        except:
            logger.exception("Failed to load mpi4py")
            raise

    # Create separate working directory for each rank
    from os.path import abspath as _abspath, join as _join
    curdir = _abspath (localWorkDir)
    wkdirname = "rank_%s" % str(mpirank)
    wkdir  = _abspath (_join(curdir,wkdirname))
    if not os.path.exists(wkdir):
        os.makedirs (wkdir)
    os.chdir (wkdir)

    logger.info("GlobalWorkDir: %s" % globalWorkDir)
    logger.info("LocalWorkDir: %s" % localWorkDir)
    logger.info("OutputDir: %s" % outputDir)
    logger.info("RANK: %s" % mpirank)

    if mpirank==0:
        try:
            from pandayoda.yodacore import Yoda
            yoda = Yoda.Yoda(globalWorkDir, localWorkDir, rank=0, nonMPIMode=nonMPIMode, outputDir=outputDir, dumpEventOutputs=dumpEventOutputs)
            yoda.start()

            from pandayoda.yodaexe import Droid
            if nonMPIMode:
                reserveCores = 0
            else:
                reserveCores = 1
            droid = Droid.Droid(globalWorkDir, localWorkDir, rank=0, nonMPIMode=True, reserveCores=reserveCores, outputDir=outputDir)
            droid.start()

            i = 30
            while True:
                logger.info("Rank %s: Yoda isAlive %s" % (mpirank, yoda.isAlive()))
                logger.info("Rank %s: Droid isAlive %s" % (mpirank, droid.isAlive()))

                if yoda and yoda.isAlive():
                    time.sleep(60)
                else:
                    break
            logger.info("Rank %s: Yoda finished" % (mpirank))
        except:
            logger.exception("Rank %s: Yoda failed" % mpirank)
            raise
        #os._exit(0)
    else:
        try:
            status = 0
            from pandayoda.yodaexe import Droid
            droid = Droid.Droid(globalWorkDir, localWorkDir, rank=mpirank, nonMPIMode=nonMPIMode, outputDir=outputDir)
            droid.start()
            while (droid and droid.isAlive()):
                droid.join(timeout=1)
            # parent process
            #pid, status = os.waitpid(child_pid, 0)
            logger.info("Rank %s: Droid finished status: %s" % (mpirank, status))
        except:
            logger.exception("Rank %s: Droid failed" % mpirank)
            raise
    return mpirank

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    usage = """
usage: %(prog)s <command> [options] [args]

Commands:

    help <command>  Output help for one of the commands below

"""
    oparser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), add_help=True)
    oparser.add_argument('--globalWorkingDir', dest="globalWorkingDir", default=None, help="Global share working directory")
    oparser.add_argument('--localWorkingDir', dest="localWorkingDir", default=None, help="Local working directory. if it's not set, it will use global working directory")
    oparser.add_argument('--nonMPIMode', default=False, action='store_true', help="Run Yoda in non-MPI mode")
    oparser.add_argument('--outputDir', dest="outputDir", default=None, help="Copy output files to this directory")
    oparser.add_argument('--dumpEventOutputs', default=False, action='store_true', help="Dump event output info to xml")
    oparser.add_argument('--verbose', '-v', default=False, action='store_true', help="Print more verbose output.")

    if len(sys.argv) == 1:
        oparser.print_help()
        sys.exit(-1)

    args = oparser.parse_args(sys.argv[1:])
    if args.globalWorkingDir is None:
        logger.error( "Global working directory is needed.")
        oparser.print_help()
        sys.exit(-1)
    if args.localWorkingDir is None:
        args.localWorkingDir = args.globalWorkingDir

    rank = None
    try:
        logger.info("Start HPCJob")
        rank = main(args.globalWorkingDir, args.localWorkingDir, args.nonMPIMode, args.outputDir, args.dumpEventOutputs)
        logger.info( "Rank %s: HPCJob-Yoda success" % rank )
        if rank == 0:
            # this causes all MPI ranks to exit, uncleanly
            MPI.COMM_WORLD.Abort(-1)
            sys.exit(0)
    except Exception as e:
        logger.exception("Rank " + rank + ": HPCJob-Yoda failed, exiting all ranks.")
        if rank == 0:
            # this causes all MPI ranks to exit, uncleanly
            MPI.COMM_WORLD.Abort(-1)
            sys.exit(-1)
    #os._exit(0)
