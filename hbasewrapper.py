import subprocess

class HBaseWrapper:
    def __run(self):
        p = subprocess.Popen([s], stdout=subprocess.PIPE, shell=True)
        (o, e) = p.communicate()
        return o

    def __parse_cf(self, info):
        return [m.group(1) for m in re.finditer(r"NAME\s=>\s'([^']+)", info)]

    def backup_table(table, path):
        self.__run('hbase org.apache.hadoop.hbase.mapreduce.Export "%s" %s' % (table, path))

    def table_meta(table):
        return {
            'name': table,
            'column_families': self__parse_cf(self.__run("echo \"describe '%s'\" | hbase shell" % table))
        }

