# This script is run by the PanDA pilot
# Its purpose is to extract the VmPeak, its average and the rss value from the *.pmon.gz file(s), and
# place these values in a file (VmPeak_values.txt) which in turn is read back by the pilot
# VmPeak_values.txt has the format:
#    <VmPeak max>,<VmPeam max mean>,<rss mean>
# Note that for a composite trf, there are multiple *.pmon.gz files. The values reported are the max values of all files (thus the 'max average')

# Prerequisite: the ATLAS environment needs to have been setup before the script is run

import os

def processFiles():
    """ Process the PerfMon files using PerfMonComps """

    vmem_peak_max = 0
    vmem_mean_max = 0
    rss_mean_max = 0

    # get list of all PerfMon files
    from glob import glob
    file_list = glob("*.pmon.gz")

    if file_list != []:
        # loop over all files
        for file_name in file_list:
            # process this file using PerfMonComps
            print "[Pilot VmPeak] Processing file: %s" % (file_name)
            info = PerfMonComps.PMonSD.parse(file_name)

            if info:
                vmem_peak = info[0]['special']['values']['vmem_peak']
                vmem_mean = info[0]['special']['values']['vmem_mean']
                rss_mean = info[0]['special']['values']['rss_mean']

                print "[Pilot VmPeak] vmem_peak = %.1f, vmem_mean = %.1f, rss_mean = %.1f" % (vmem_peak, vmem_mean, rss_mean)

                if vmem_peak > vmem_peak_max:
                    vmem_peak_max = vmem_peak
                if vmem_mean > vmem_mean_max:
                    vmem_mean_max = vmem_mean
                if rss_mean > rss_mean_max:
                    rss_mean_max = rss_mean
            else:
                print "!!WARNING!!1212!! PerfMonComps.PMonSD.parse returned None while parsing file %s" % (file_name)

        # convert to integers
        vmem_peak_max = int(vmem_peak_max)
        vmem_mean_max = int(vmem_mean_max)
        rss_mean_max = int(rss_mean_max)
    else:
        print "[Pilot VmPeak] Did not find any PerfMon log files"

    return vmem_peak_max, vmem_mean_max, rss_mean_max

def dumpValues(vmem_peak_max, vmem_mean_max, rss_mean_max):
    """ Create the VmPeak_values.txt file"""

    file_name = os.path.join(os.getcwd(), "VmPeak_values.txt")
    print "[Pilot VmPeak] Creating file: %s" % (file_name)
    try:
        f = open(file_name, "w")
    except OSError, e:
        print "[Pilot VmPeak] Could not create %s" % (file_name)
    else:
        s = "%d,%d,%d" % (vmem_peak_max, vmem_mean_max, rss_mean_max)
        f.write(s)
        f.close()
        print "[Pilot VmPeak] Wrote values to file %s" % (file_name)

# main function

if __name__ == "__main__":
    try:
        import PerfMonComps.PMonSD
    except Exception, e:
        print "Failed to import PerfMonComps.PMonSD: %s" % (e)
        print "Aborting VmPeak script"
    else:
        vmem_peak_max, vmem_mean_max, rss_mean_max = processFiles()
        if vmem_peak_max == 0 and vmem_mean_max == 0 and rss_mean_max == 0:
            print "[Pilot VmPeak] All VmPeak and RSS values zero, will not create VmPeak values file"
        else:
            print "[Pilot VmPeak] vmem_peak_max = %d, vmem_mean_max = %d, rss_mean_max = %d" % (vmem_peak_max, vmem_mean_max, rss_mean_max)

            # create the VmPeak_values.txt file
            dumpValues(vmem_peak_max, vmem_mean_max, rss_mean_max)

            print "[Pilot VmPeak] Done"
