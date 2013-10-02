import re
import socket
import time
import sys

server_address = '/tmp/daemon_socket'
data = '''Version = 20130611
Program = /media/40G/zerovm-samples/hello/hello.nexe
Timeout = 5
Memory = 4294967295, 0
NameServer = udp:127.0.0.1:54321
Channel = /dev/null, /dev/stdin, 0, 0, 999999, 999999, 0, 0
Channel = /dev/null, /dev/stdout, 0, 0, 0, 0, 999999, 999999
Channel = /dev/null, /dev/stderr, 0, 0, 0, 0, 999999, 999999
'''
job_id = None
node_id = None
if len(sys.argv) > 2:
    job_id = sys.argv[2]
    data += 'Job = %s\n' % job_id
if len(sys.argv) > 3:
    node_id = sys.argv[3]
    data += 'Node = %s\n' % node_id
command = sys.argv[1]
hdr = "%s %d\n" % (command, len(data))
packet = hdr + data
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
start = time.time()
try:
    sock.connect(server_address)
    sock.sendall(packet)
    print "Client sent:\n%s" % packet
    resp = sock.makefile()
    header = resp.readline().strip()
    m = re.match(r'^(\w+) (\d+)$', header)
    keyword = m.group(1)
    size = int(m.group(2))
    data = resp.read(size)
    print "Client received:\n%s\n%s" % (keyword, data)
finally:
    sock.close()
delay = time.time() - start
print "Finished in %f.2" % delay
