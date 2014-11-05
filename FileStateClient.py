from FileState import FileState
from pUtil import tolog

def createFileStates(workDir, jobId, outFiles=None, inFiles=None, logFile=None, type="output"):
    """ Create the initial file state dictionary """

    # file list
    if type == "output":
        files = outFiles
        files.append(logFile)
    else:
        files = inFiles

    # create temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, type=type)
    FS.resetStates(files, type=type)

    # cleanup
    del FS

def updateFileStates(files, workDir, jobId, mode="file_state", state="not_created", type="output"):
    """ Update the current file states (for all files) """

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, type=type)

    # update all files
    for fileName in files:
        FS.updateState(fileName, mode=mode, state=state)

    # cleanup
    del FS

def updateFileState(fileName, workDir, jobId, mode="file_state", state="not_created", type="output"):
    """ Update the current file states (for all files) """

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, type=type)

    # update this file
    FS.updateState(fileName, mode=mode, state=state)

    # cleanup
    del FS

def dumpFileStates(workDir, jobId, type="output"):
    """ Update the current file states (for all files) """

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, type=type)

    # dump file states for all files
    FS.dumpFileStates(type=type)

    # cleanup
    del FS

def hasOnlyCopyToScratch(workDir, jobId):
    """ Check if there are only copy_to_scratch tranfer modes in the file dictionary """
    # goal: remove --directIn in cmd3 if there are only transfer mode "copy_to_scratch" files

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, type="input")

    # are there only "copy_to_scratch" files in the file dictionary?
    status = FS.hasOnlyCopyToScratch()

    # cleanup
    del FS

    return status

def getFileState(fileName, workDir, jobId, type="output"):
    """ Return the current state of a given file """

    # create a temporary file state object
    FS = FileState(workDir=workDir, jobId=jobId, type=type)

    # update this file
    state = FS.getFileState(fileName)

    # cleanup
    del FS

    return state
