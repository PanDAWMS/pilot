import sys
import mpi4py


if __name__ == '__main__':
    try:
        # get rank
        comm = mpi4py.MPI.COMM_WORLD
        mpirank = comm.Get_rank()
        if mpirank == 0:
            # Yoda runs on rank 0
            from pandayoda import Yoda
            yoda = Yoda()
            yoda.run()
        else:
            # droid runs on rank N (N>0)
            pass
    except:
        pass
