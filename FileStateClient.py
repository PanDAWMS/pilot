from FileState import FileState
from pUtil import tolog

def createFileStates(workDir, jobId, outFiles=None, inFiles=None, logFile=None, ftype="output"):
    """ Create the initial file state dictionary """

    # file list
    if ftype == "output":
        files = outFiles
        files.append(logFile)
    else:
        files = inFiles

    # create temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, ftype=ftype)
    FS.resetStates(files, ftype=ftype)

    # cleanup
    del FS

def updateFileStates(files, workDir, jobId, mode="file_state", state="not_created", ftype="output"):
    """ Update the current file states (for all files) """

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, ftype=ftype)

    # update all files
    for fileName in files:
        FS.updateState(fileName, mode=mode, state=state)

    # cleanup
    del FS

def updateFileState(fileName, workDir, jobId, mode="file_state", state="not_created", ftype="output"):
    """ Update the current file states (for all files) """

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, ftype=ftype)

    # update this file
    FS.updateState(fileName, mode=mode, state=state)

    # cleanup
    del FS

def dumpFileStates(workDir, jobId, ftype="output"):
    """ Update the current file states (for all files) """

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, ftype=ftype)

    # dump file states for all files
    FS.dumpFileStates(ftype=ftype)

    # cleanup
    del FS

def getFilesOfState(workDir, jobId, ftype="output", state="transferred"):
    """ Return a comma-separated list of files in a given state"""

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, ftype=ftype)

    # get the list
    filenames = FS.getFilesOfState(state=state)

    # cleanup
    del FS

    return filenames

def hasOnlyCopyToScratch(workDir, jobId):
    """ Check if there are only copy_to_scratch tranfer modes in the file dictionary """
    # goal: remove --directIn in cmd3 if there are only transfer mode "copy_to_scratch" files

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, ftype="input")

    # are there only "copy_to_scratch" files in the file dictionary?
    status = FS.hasOnlyCopyToScratch()

    # cleanup
    del FS

    return status

def getFileState(fileName, workDir, jobId, ftype="output"):
    """ Return the current state of a given file """

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, ftype=ftype)

    # update this file
    state = FS.getFileState(fileName)

    # cleanup
    del FS

    return state
