from optparse import OptionParser
from hbasewrapper import HBaseWrapper

import sys

def validate_options(options):
    if options.imp == options.exp:
        print "Only --import xor --export should be specified"
        return False
    if not options.backup:
        print "--backup path should be specified"
        return False   
    return True

if __name__ == "__main__":
    parser = OptionParser()
    
    parser.add_option("-t", "--table", dest="table", help="table to be backuped")
    parser.add_option("-b", "--backup", dest="backup", help="path to backup")
    parser.add_option("-i", "--import", dest="imp", help="restore backup", default=False)           
    parser.add_option("-e", "--export", dest="exp", help="make backup", default=False)

    (options, args) = parser.parse_args()

    if not validate_options(options):
        #sys.exit();
        pass

    wrapper = HBaseWrapper()

    print options

