#!/usr/bin/python

from optparse import OptionParser
from wrappers import HBaseWrapper, HDFSWrapper

import re
import sys
import time
import uuid

def validate_options(options):
    if options.imp == options.exp:
        print "Only --import xor --export should be specified"
        return False
    if not options.backup:
        print "--backup path should be specified"
        return False
    return True

def export_table(table, target, w_hbase, w_hdfs):
    print "Exporting %s to %s" % (table, target)
    w_hdfs.remove(target)
    w_hbase.backup_table(table, target)
    w_hdfs.save_meta(w_hbase.table_meta(table), target)

def main(options):
    w_hbase = HBaseWrapper(options.dry)
    w_hdfs = HDFSWrapper(options.dry)

    if options.exp:
        backup_path = "%s/%s/" % (options.backup, time.strftime("%Y-%m-%d"))
        if options.table:
            if not options.table in w_hbase.get_tables():
                print "Table %s does not exists" % options.table
            export_table(options.table, backup_path + re.sub(r'[^A-Za-z0-9_-]', '_', options.table), w_hbase, w_hdfs)
        else:
            for table in w_hbase.get_tables():
                export_table(table, backup_path + re.sub(r'[^A-Za-z0-9_-]', '_', table), w_hbase, w_hdfs)

    if options.imp:
        w_hbase.import_table(options.table, options.backup)

if __name__ == "__main__":
    parser = OptionParser()

    parser.add_option("-t", "--table", dest="table", help="table to be backuped")
    parser.add_option("-b", "--backup", dest="backup", help="path to backup")
    parser.add_option("-i", "--import", action="store_true", dest="imp", help="restore backup", default=False)
    parser.add_option("-e", "--export", action="store_true", dest="exp", help="make backup", default=False)
    parser.add_option("-d", "--dry-run", action="store_true", dest="dry", help="make backup", default=False)

    (options, args) = parser.parse_args()

    if validate_options(options):
        main(options)
