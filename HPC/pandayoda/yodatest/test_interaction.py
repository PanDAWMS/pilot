import urllib
from mpi4py import MPI
from pandayoda.yodacore import Interaction

comm = MPI.COMM_WORLD
mpirank = comm.Get_rank()

if mpirank==0:
    rsv = Interaction.Receiver()
    while rsv.activeRanks():
        tmpStat,method,params = rsv.receiveRequest()
        print mpirank,'got',tmpStat,method,params
        print rsv.returnResponse({'msg':'Done'})
        rsv.decrementNumRank()
    print mpirank,"done"
else:
    snd = Interaction.Requester()
    print mpirank,"sending req"
    res = snd.sendRequest('dummy',{1:2,3:4,'rank':mpirank})
    print res
    print mpirank,"done"
    
