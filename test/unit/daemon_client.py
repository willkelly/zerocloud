import re
import socket
import time
import sys

server_address = '/tmp/daemon_socket'
data = 'Timeout = 5\n'
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
