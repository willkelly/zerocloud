from contextlib import contextmanager
import os
from random import randint
import re
import socket
from eventlet.green import select, subprocess
from eventlet import GreenPool, listen, sleep, Timeout
from tempfile import mkstemp
import time
from swift.common.utils import mkdirs

REPORT_LENGTH = 5

STATUS_OK = 0
STATUS_ERROR = 1
STATUS_TIMEOUT = 2
STATUS_KILLED = 3
STATUS_OVERFLOW = 4
STATUS_STOP = 5

NO_NODE_ID = '250\nNo node id in request\n'
NO_JOB_ID = '251\nNo job id in request\n'
UNSUPPORTED = '252\nUnsupported\n'
NODE_NOT_FOUND = '253\nNode not found\n'
JOB_NOT_FOUND = '254\nJob not found\n'
UNKNOWN_COMMAND = '255\nUnknown command\n'


class ZerovmExecutor:

    def __init__(self, command_line, manifest_filename, job_id, node_id, stats_file=None, timeout=1000):
        self.command_line = command_line + [manifest_filename]
        self.manifest = manifest_filename
        self.zerovm_timeout = timeout
        self.stdout_data = ''
        self.stderr_data = ''
        self.proc = None
        self.zerovm_stdout_size = 64 * 1024
        self.zerovm_stderr_size = 64 * 1024
        self.zerovm_kill_timeout = 1
        self.rc = None
        self.lines = []
        self.stats = ''
        self.stats_file = stats_file
        self.job_id = job_id
        self.node_id = node_id

    def stop(self, rc):
        if not self.proc:
            return '%d\n%s' % (self.rc, self.stdout_data)
        self.proc.terminate()
        self.rc = rc
        try:
            self._communicate()
        except (Exception, Timeout):
            self.proc.kill()
            self.rc = 3
            self._get_final_status()
        return '%d\n%s' % (self.rc, self.stdout_data)

    def _communicate(self):
        with Timeout(self.zerovm_timeout):
            while len(self.readable) > 0:
                self._read_from_std(self.readable)
                if self._output_overflow():
                    break
            self._get_final_status()

    def run(self):
        self.proc = subprocess.Popen(self.command_line,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)

        self.readable = [self.proc.stdout, self.proc.stderr]
        try:
            self._communicate()
        except (Exception, Timeout):
            self.stop(STATUS_TIMEOUT)

    def _output_overflow(self):
        if len(self.stdout_data) > self.zerovm_stdout_size \
                or len(self.stderr_data) > self.zerovm_stderr_size:
            self.proc.kill()
            self.proc = None
            self.rc = STATUS_OVERFLOW
            return True
        return False

    def _get_final_status(self):
        if not self.proc:
            return None
        (stdout_data, stderr_data) = self.proc.communicate()
        self.stdout_data += stdout_data
        self.stderr_data += stderr_data
        if self.rc is None:
            return_code = STATUS_OK
            if self.proc.returncode:
                return_code = STATUS_ERROR
            self.rc = return_code
        self.proc = None
        self._update_stats(stdout_data)
        try:
            os.unlink(self.manifest)
        except IOError:
            pass

    def _read_from_std(self, readable):
        rlist, _junk, __junk = \
            select.select(readable, [], [], self.zerovm_timeout)
        if rlist:
            for stream in rlist:
                data = os.read(stream.fileno(), 4096)
                if not data:
                    readable.remove(stream)
                    continue
                if stream == self.proc.stdout:
                    self.stdout_data += data
                    self._update_stats(data)
                elif stream == self.proc.stderr:
                    self.stderr_data += data

    def _update_stats(self, data):
        self.lines.extend(data.splitlines())
        start = (len(self.lines) / REPORT_LENGTH - 1) * REPORT_LENGTH
        if start >= 0:
            end = len(self.lines) / REPORT_LENGTH * REPORT_LENGTH
            report = self.lines[start:end]
            self.lines = self.lines[end:]
            stats = '\n'.join(report) + '\n'
            self.stats = stats
        if not self.stats_file:
            return
        if not self.proc:
            try:
                fd = open(self.stats_file, 'wb')
                fd.write(self.stats)
                fd.close()
            except IOError:
                pass

    def pause(self):
        return UNSUPPORTED


class ZerovmDaemon:

    def __init__(self, socket_name):
        self.server_address = socket_name
        self.zerovm_exename = ['zerovm']
        self.pool = GreenPool()
        self.jobs = set()
        self.stats_dir = '/tmp'

    def create_executor(self, zerovm_inputmnfst_fn, job_id=None, node_id=None,
                        stats_file=None, zerovm_args=None, zerovm_timeout=1000):
        cmdline = []
        cmdline += self.zerovm_exename
        if zerovm_args:
            cmdline += zerovm_args
        executor = ZerovmExecutor(cmdline, zerovm_inputmnfst_fn, job_id,
                                  node_id, stats_file, timeout=zerovm_timeout)
        self.jobs.add(executor)
        start = time.time()
        executor.run()
        print "Executor finished in %f sec" % (time.time() - start)
        self.jobs.remove(executor)
        return executor

    def parse_command(self, fd):
        try:
            data = fd.readline().strip()
            m = re.match(r'^(\w+) (\d+)$', data)
            if m.lastindex < 2:
                fd.read()
                return None
            keyword = m.group(1)
            size = int(m.group(2))
            if size <= 0:
                return keyword, ''
            data = fd.read(size)
            return keyword, data
        except IOError:
            return None, None

    def send_response(self, fd, keyword, data):
        resp = '%s %d\n%s' % (keyword, len(data), data)
        try:
            fd.write(resp)
        except IOError:
            pass

    def get_job_id(self, data):
        inputmnfst = data.splitlines()
        job_id = None
        node_id = None
        dl = re.compile("\s*=\s*")
        for line in inputmnfst:
            try:
                (attr, val) = re.split(dl, line.strip(), 1)
                if attr:
                    if 'job' in attr.lower():
                        job_id = val.strip()
                        continue
                    elif 'node' in attr.lower():
                        node_id = val.strip()
                        continue
            except ValueError:
                continue
            if job_id and node_id:
                break
        return job_id, node_id

    def SPAWN(self, data):
        job_id, node_id = self.get_job_id(data)
        if not job_id:
            return NO_JOB_ID
        if not node_id:
            return '%s 0 %s' % (job_id, NO_NODE_ID)
        manifest_name = self.new_manifest(data)
        stats_dir = os.path.join(self.stats_dir, job_id)
        if not os.path.exists(stats_dir):
            mkdirs(stats_dir)
        stats_file = os.path.join(stats_dir, node_id)
        self.pool.spawn_n(self.create_executor, manifest_name,
                          job_id=job_id, node_id=node_id, stats_file=stats_file)
        return '%s %s %d\n' % (job_id, node_id, STATUS_OK)

    def RUN(self, data):
        job_id, node_id = self.get_job_id(data)
        if not job_id:
            return NO_JOB_ID
        if not node_id:
            return '%s 0 %s' % (job_id, NO_NODE_ID)
        manifest_name = self.new_manifest(data)
        executor = self.create_executor(manifest_name, job_id=job_id, node_id=node_id)
        return '%s %s %d\n%s' % (job_id, node_id, executor.rc, executor.stdout_data)

    def _call(self, data, method, *args):
        job_id, node_id = self.get_job_id(data)
        if not job_id:
            return NO_JOB_ID
        status = ''
        for executor in self.jobs:
            if job_id in executor.job_id:
                if node_id:
                    if node_id in executor.node_id:
                        status = '%s %s %s' % (job_id, node_id, getattr(executor, method)(*args))
                else:
                    status += '%s %s %s' % (job_id, executor.node_id, getattr(executor, method)(*args))
        if status:
            return status
        if node_id:
            return '%s %s %s' % (job_id, node_id, NODE_NOT_FOUND)
        return '%s 0 %s' % (job_id, JOB_NOT_FOUND)

    def PAUSE(self, data):
        return self._call(data, 'pause')

    def STOP(self, data):
        self._call(data, 'stop', STATUS_STOP)

    def _read_stats_file(self, job_id, node_id):
        stats_file = os.path.join(self.stats_dir, job_id, node_id)
        try:
            fd = open(stats_file, 'rb')
            stats = fd.read()
            fd.close()
        except IOError:
            return '%s %s %s' % (job_id, node_id, NODE_NOT_FOUND)
        return '%s %s %s' % (job_id, node_id, stats)

    def STATUS(self, data):
        job_id, node_id = self.get_job_id(data)
        if not job_id:
            return NO_JOB_ID
        status = ''
        living = set()
        for executor in self.jobs:
            if job_id in executor.job_id:
                if node_id:
                    if node_id in executor.node_id:
                        living.add(executor)
                        status = '%s %s %s' % (job_id, node_id, executor.stats)
                else:
                    living.add(executor)
                    status += '%s %s %s' % (job_id, executor.node_id, executor.stats)
        if status:
            if node_id:
                return status
            filestats = []
            for node_id in os.listdir(os.path.join(self.stats_dir, job_id)):
                for executor in living:
                    if job_id in executor.job_id and node_id in executor.node_id:
                        continue
                filestats += [self._read_stats_file(job_id, node_id)]
            if filestats:
                status += '\n'.join(filestats)
            return status
        if node_id:
            return self._read_stats_file(job_id, node_id)
        return '%s 0 %s' % (job_id, JOB_NOT_FOUND)

    def handle(self, fd):
        start = time.time()
        keyword, data = self.parse_command(fd)
        if not keyword or not data:
            self.send_response(fd, 'ERROR', '0\n%s\n' % data)
            return
        handler = getattr(self, keyword, None)
        if handler:
            status = handler(data)
        else:
            status = UNKNOWN_COMMAND
        self.send_response(fd, keyword, status)
        print "Handling finished in %f sec" % (time.time() - start)

    def serve(self):
        try:
            os.remove(self.server_address)
        except OSError:
            pass
        server = listen(self.server_address, family=socket.AF_UNIX)
        while True:
            try:
                new_sock, address = server.accept()
                self.pool.spawn_n(self.handle, new_sock.makefile('rw'))
            except (SystemExit, KeyboardInterrupt):
                break

    def new_manifest(self, data):
        fd, name = mkstemp()
        os.write(fd, data)
        os.close(fd)
        return name

if __name__ == '__main__':
    server = ZerovmDaemon('/tmp/daemon_socket')
    server.serve()