
From Wen Guan, develop on HPCEvent branch(configured in gitlab)
1) clone from gitlab
 git clone ssh://git@gitlab.cern.ch:7999/wguan/PandaPilot.git
cd PandaPilot/

2) add upstream
#git remote add upstream  https://github.com/PanDAWMS/pilot.git
git remote add upstream ssh://git@github.com/PanDAWMS/pilot.git

3) verify 
git status
git branch --track HPCEvent
git branch -a
git branch -av
git remote -v

4) fetch all branches
git fetch --all; git pull

5)start dev on HPCEvent branch
git checkout HPCEvent
git fetch --all
git pull --all --prune --progress
git checkout -b test_patch
## develop
## git add
## git commit
git push origin test_patch

6) in gitlab, merge branches
# merge reviewed branch

7)refresh branches to local
git fetch --all
git pull --all --prune --progress

8)push finally branches to github
unset SSH_ASKPASS
git push upstream HPCEvent

9) create test tar file
rm wguan-pilot-dev-HPC_recov.tar.gz; tar czf wguan-pilot-dev-HPC_recov.tar.gz *; cp wguan-pilot-dev-HPC_recov.tar.gz ~/www/wiscgroup/
