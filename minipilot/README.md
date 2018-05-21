MiniPilot
=========

Basics
------

You should know the next basic things:

1. MiniPilot does not test ENV
2. MiniPilot does not test anything
3. MiniPilot does not setup ENV
4. MiniPilot is not going to setup any ENV for the job
5. Do it yourself, MiniPilot is meant only for testing
6. MiniPilot is not prepared to run on Py 2.6, which is part of `setupATLAS; lsetup rucio`
7. Though MiniPilot is experiment agnostic, it uses AGIS and Rucio, which are not
8. Provided Windows support in Rucio (or whatever-you-use-OS), MiniPilot will run on your machine as well
9. Same if you implement another mover
10. To much libs? I don't bother


Installation
------------

Get git, get python 2.7 working, install the requirements from _requirements.txt_. Better to setup your ENV according to
the next section before installing them. Remember to use the proper version of PIP, by default it is outdated.


Starting
--------

1. Ensure your proxy certificate

2. Change the next script for your environment and `source` it:

    ```bash
    #!/bin/bash
    # This script is OK for lxplus
    
    export PYTHONUSERBASE=~/path/to/python/modules/you/installed
    export VO_ATLAS_SW_DIR=/afs/cern.ch/atlas/
    
    export MYPITHONDIR=/afs/cern.ch/sw/lcg/external/Python/2.7.3/x86_64-slc6-gcc48-opt
    
    export PYTHONDIR=$MYPITHONDIR
    export LD_LIBRARY_PATH=/lib:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/emi/3.17.1-1_v2.sl6/lib64:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/emi/3.17.1-1_v2.sl6/lib:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/emi/3.17.1-1_v2.sl6/alrbUsr/lib64:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/emi/3.17.1-1_v2.sl6/alrbUsr/lib:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/emi/3.17.1-1_v2.sl6/alrbUsr/lib64/dcap:/lib::/opt/rh/python27/root/usr/lib64:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/emi/3.17.1-1_v2.sl6/usr/lib64:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/emi/3.17.1-1_v2.sl6/usr/lib:/opt/rh/python27/root/usr/lib64
    
    setupATLAS
    lsetup rucio
    
    export PYTHONPATH=$PYTHONUSERBASE/lib/python2.7/site-packages:/lib:${PYTHONPATH//$PYTHONUSERBASE"/lib/python2.7/site-packages:/lib:"/}
    export PATH=$HOME/bin:$MYPITHONDIR/bin:${PATH//$HOME"/bin:"$MYPITHONDIR"/bin:"/}
    export LD_LIBRARY_PATH=$PYTHONUSERBASE/lib:$MYPITHONDIR/lib:${LD_LIBRARY_PATH//$PYTHONUSERBASE"/lib:"$MYPITHONDIR"/lib:"/}
    ```

3. Run the MiniPilot
    ```bash
    $ pilot.py --queue <QUEUE_NAME> --job_tag <prod_or_any_other>
    ```


Usage
-----

To get the parameter list with the explanations, use:
```bash
$ pilot.py --help
```

With that you can:

1. Skip server updates
2. Simulate Rucio
3. Setup other paths for certs or else
4. Use another servers & ports
5. Use preloaded descriptions and configs


More usage
----------

See into the depths of the code, there are also some docs.


Even moar usage
---------------

Ask me.
