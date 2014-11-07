import sys
import json
import pickle
from mpi4py import MPI
from pandayoda.yodacore import Interaction
from pandayoda.yodacore import Yoda
from pandayoda.yodacore import Logger
from pandayoda.yodaexe import Droid

import logging
logging.basicConfig(level=logging.DEBUG)

er = [
    {'eventRangeID':'1-2-3',
     'startEvent':0,
     'lastEvent':9,
     'LFN':'NTUP_SUSY.01272447._000001.root.2',
     'GUID':'c58cc417-f369-44d8-81b4-72a76c1f2b79',
     'scope':'mc12_8TeV'},
    {'eventRangeID':'4-5-6',
     'startEvent':10,
     'lastEvent':19,
     'LFN':'NTUP_SUSY.01272447._000001.root.2',
     'GUID':'c58cc417-f369-44d8-81b4-72a76c1f2b79',
     'scope':'mc12_8TeV'},
    ]

job = {'PandaID':'123',
       'jobsetID':'567',
       }

f = open('job_pickle.txt','w')
pickle.dump(job,f)
f.close()

f = open('eventranges_pickle.txt','w')
pickle.dump(er,f)
f.close()

# get logger
tmpLog = Logger.Logger()


comm = MPI.COMM_WORLD
mpirank = comm.Get_rank()

if mpirank==0:
    yoda = Yoda.Yoda("/afs/cern.ch/user/w/wguan/Panda/Yoda2_ES", "/afs/cern.ch/user/w/wguan/Panda/HPCWoring")
    yoda.run()
else:
    droid = Droid.Droid("/afs/cern.ch/user/w/wguan/Panda/Yoda2_ES", "/afs/cern.ch/user/w/wguan/Panda/HPCWoring")
    droid.run()
