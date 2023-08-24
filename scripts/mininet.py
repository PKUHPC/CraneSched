import sys
import resource
from time import sleep
from functools import partial
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import Host
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.cli import CLI

MaxLimit = 1048576
try:
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (MaxLimit, MaxLimit))
    print("File descriptor limit set to %d successfully!" % MaxLimit)

except ValueError as e:
    print(f"Error setting limit: {e}")
except resource.error as e:
    print(f"Error setting limit: {e}")

with open('/proc/sys/kernel/pid_max', 'w') as f:
    f.write(str(MaxLimit))

NodeNum = 3
if len(sys.argv) > 1:
    NodeNum = int(sys.argv[1])
NodeName = []


class SingleSwitchTopo(Topo):
    """Single switch connected to n hosts."""

    def __init__(self, n=2, **opts):
        Topo.__init__(self, **opts)
        switch = self.addSwitch('switch1')
        for h in range(n):
            ip_addr = '10.0.0.%d/24' % (h + 1)
            NodeName.append('h%s' % (h + 1))
            host = self.addHost(NodeName[h], ip=ip_addr)
            # 10 Mbps, 5ms delay, 0% Loss, 1000 packet queue
            self.addLink(host, switch, bw=100, delay='1ms', loss=0, max_queue_size=1000, use_htb=True)


def perf_test():
    """Create network and run simple performance test"""
    topo = SingleSwitchTopo(n=NodeNum)
    private_dirs = [('/tmp/cranectld', '/tmp/cranectld'), ('/tmp/output', '/tmp/output'), '/tmp/crane']
    host = partial(Host, privateDirs=private_dirs)
    net = Mininet(topo=topo, host=host, link=TCLink)
    net.addController("c1")
    net.addNAT(ip='10.0.0.254').configDefault()
    net.start()
    # print("Dumping host connections")
    # dumpNodeConnections(net.hosts)
    # print("Testing network connectivity")
    # net.pingAll()
    hosts = net.hosts
    if True:
        hosts.pop()
        print("Starting crane...")
        cranectld = hosts[0]
        cranectld.cmd('echo > /tmp/output/cranectld.out')
        cranectld.cmd('echo > /tmp/output/cranectld.err')
        cranectld.cmdPrint('nohup /home/llx/Documents/Crane/cmake-build-debug-vmmaster/src/CraneCtld/cranectld -H',
                           cranectld.name,
                           '-C', '/etc/crane/config-mininet.yaml',
                           '>', '/tmp/output/cranectld.out',
                           '2>', '/tmp/output/cranectld.err',
                           '&')
        sleep(3)

        for h in hosts:
            # Create and/or erase output files
            outfiles = '/tmp/output/%s.out' % h.name
            errfiles = '/tmp/output/%s.err' % h.name
            h.cmd('echo >', outfiles)
            h.cmd('echo >', errfiles)
            # Start pings
            h.cmdPrint('nohup /home/llx/Documents/Crane/cmake-build-debug-vmmaster/src/Craned/craned -H', h.name,
                       '-C', '/etc/crane/config-mininet.yaml',
                       '>', outfiles,
                       '2>', errfiles,
                       '&')
            sleep(0.1)

    CLI(net)
    cranectld.cmd('pkill -2 cranectld')
    for h in hosts:
        h.cmd('pkill -2 craned')
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    perf_test()
