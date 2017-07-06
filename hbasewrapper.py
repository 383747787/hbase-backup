import subprocess
import json
import re
import uuid

class ShellWrapper:
    def _run(self, command):
        p = subprocess.Popen([command], stdout=subprocess.PIPE, shell=True)
        (o, e) = p.communicate()
        return o

class HBaseWrapper(ShellWrapper):
    def __parse_cf(self, info):
        return [m.group(1) for m in re.finditer(r"NAME\s=>\s'([^']+)", info)]

    def backup_table(self, table, path):
        self._run('hbase org.apache.hadoop.hbase.mapreduce.Export "%s" %s' % (table, path))

    def drop_table(self, table):
        self._run("echo \"disable '%s'\" | hbase shell" % table)
        self._run("echo \"drop '%s'\" | hbase shell" % table)

    def create_table(self, meta):
        self._run("echo \"create '%s', %s\" | hbase shell" % (meta.table, ["'%s'" % cf for cf in meta.column_families].join(', ')))
        
    def import_table(self, table, path):
        self._run('hbase org.apache.hadoop.hbase.mapreduce.Import "%s" %s' % (table, path))

    def table_meta(self, table):
        return {
            'name': table,
            'column_families': self.__parse_cf(self._run("echo \"describe '%s'\" | hbase shell" % table))
        }

    def get_tables(self):
        return json.loads(self._run('echo list | hbase shell | tail -n 1'))

class HDFSWrapper(ShellWrapper):
    def save_meta(self, meta, path):
        tmp_filename = '/tmp/%s.json' % uuid.uuid4()
        with open(tmp_filename, 'w') as outfile:
            json.dump(meta, outfile)
            outfile.close()
        self._run('hdfs dfs -put %s %s/meta.json' % (tmp_filename, path))

    def load_meta(self, path):
        return json.loads(self._run('hdfs dfs -cat %s/meta.json' % path))

    def remove(self, path):
        self._run("hdfs dfs -rm -r %s" % path);
