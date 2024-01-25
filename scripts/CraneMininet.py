#!/usr/bin/env python3

"""
Craned in Mininet
CraneCtld should be manually started on the head node.
Tested on Rocky Linux 9, Python 3.9.18, Mininet 2.3.1b4.
"""

import os
import pty
import yaml
import select
import argparse
import resource
import ipaddress as ipa
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import Host
from mininet.link import TCLink
from mininet.log import error, setLogLevel
from mininet.cli import CLI
from mininet.clean import cleanup

# Constants
HostPath = "/etc/hosts"
LogPath = "/tmp/output/{}.log"
StdoutPath = "/tmp/output/{}.out"
StderrPath = "/tmp/output/{}.err"
HostName = "craned{}"


class NodeConfig:
    """Node configuration"""

    def __init__(self, name) -> None:
        self.name = name
        self.num = 3
        self.offset = 1
        self.subnet = ipa.IPv4Network("10.0.0.0/8", strict=True)
        self.addr = ipa.IPv4Interface("192.168.0.10/24")

    def __str__(self) -> str:
        return (
            f"NodeConfig(name={self.name}, num={self.num}, "
            f"offset={self.offset}, subnet={self.subnet}, addr={self.addr})"
        )

    def hosts(self, cidr=True):
        for idx in range(self.offset, self.num + self.offset):
            yield (
                HostName.format(idx),
                f"{self.subnet[idx - self.offset + 1]}"
                + (f"/{self.subnet.prefixlen}" if cidr else ""),
            )


class ClusterConfig:
    """Cluster configuration"""

    def __init__(self, args) -> None:
        self.nodes = {}
        self.this = NodeConfig("")
        thisname = os.popen("hostname").read().strip()
        try:
            with open(args.conf, "r") as file:
                config = yaml.safe_load(file)  # type: dict
            for name, params in config["cluster"].items():
                # Parse config into NodeConfig
                if name == thisname:
                    node = self.setThisNode(thisname, params, args)
                else:
                    node = NodeConfig(name)
                    node.num = params["HostNum"]
                    node.offset = params["Offset"]
                    node.subnet = ipa.IPv4Network(params["Subnet"])
                    node.addr = ipa.IPv4Interface(params["NodeAddr"])
                self.nodes[name] = node
            if len(self.this.name) == 0 and not args.head:
                # If config not found and is not head, set it to defaults
                print(f"Cannot find config for `{thisname}`, use defaults for it")
                self.nodes[thisname] = self.setThisNode(thisname, {}, args)
        except FileNotFoundError or TypeError or KeyError or ValueError:
            print("Invalid config file, ignore and fall back to defaults")
            self.nodes = {thisname: self.setThisNode(thisname, {}, args)}

    def __str__(self) -> str:
        return f"ClusterConfig(this={self.this}, nodes={self.nodes})"

    def setThisNode(self, name: str, param: dict, args) -> NodeConfig:
        """
        Set `.this`. Specify default values here.
        """
        self.this.name = name
        self.this.num = args.num if args.num else param.get("HostNum", 3)
        self.this.offset = args.offset if args.offset else param.get("Offset", 1)
        self.this.subnet = ipa.IPv4Network(
            args.subnet if args.subnet else param.get("Subnet", "10.0.0.0/8")
        )
        self.this.addr = ipa.IPv4Interface(
            args.addr if args.addr else param.get("NodeAddr", "192.168.0.10/24")
        )
        return self.this

    def getHostEntry(self) -> list[tuple[str, str]]:
        entry = []
        for n in sorted(self.nodes, key=lambda i: self.nodes[i].offset):
            for name, addr in self.nodes[n].hosts(cidr=False):
                entry.append((name, addr))
        return entry

    def getRouteEntry(self) -> list[tuple[str, str]]:
        """
        Get needed routes for `this` host to reach subnets on other hosts.
        """
        entry = []
        for name, node in self.nodes.items():
            if name == self.this.name:
                continue
            entry.append(
                (
                    f"{node.subnet.network_address}/{node.subnet.prefixlen}",
                    str(node.addr.ip),
                )
            )
        return entry


class CranedHost(Host):
    """
    Virtual host for Craned
    """

    # Craned executable
    CranedExec = "CraneSched/ReleaseBuild/src/Craned/craned"
    # (A, B) means contents in B will be persisted in A.
    PersistList = [("/tmp/output", "/tmp/output")]
    # Each host's temporary directory is invisible to others.
    TempList = ["/tmp/crane", "/tmp/craned"]

    def __init__(self, name, **params):
        """
        With private dirs set
        """
        super().__init__(
            name=name,
            inNamespace=True,
            privateDirs=self.PersistList + self.TempList,
            **params,
        )

    # Command support via shell process in namespace
    def startShell(self, mnopts=None):
        """
        Copied and modified from `mininet/node.py`.
        Start a shell process for running commands
        """
        if self.shell:
            error(f"{self.name}: shell is already running\n")
            return
        # mnexec: (c)lose descriptors, (d)etach from tty,
        # (p)rint pid, and run in (n)amespace
        opts = "-cd" if mnopts is None else mnopts
        if self.inNamespace:
            opts += "n"

        # Modified here, add a seperated uts ns
        cmd = ["unshare", "--uts"]

        # bash -i: force interactive
        # -s: pass $* to shell, and make process easy to find in ps
        # prompt is set to sentinel chr( 127 )
        cmd += [
            "mnexec",
            opts,
            "env",
            "PS1=" + chr(127),
            "bash",
            "--norc",
            "--noediting",
            "-is",
            "mininet:" + self.name,
            ]

        # Spawn a shell subprocess in a pseudo-tty, to disable buffering
        # in the subprocess and insulate it from signals (e.g. SIGINT)
        # received by the parent
        self.master, self.slave = pty.openpty()
        self.shell = self._popen(
            cmd, stdin=self.slave, stdout=self.slave, stderr=self.slave, close_fds=False
        )
        # XXX BL: This doesn't seem right, and we should also probably
        # close our files when we exit...
        self.stdin = os.fdopen(self.master, "r")
        self.stdout = self.stdin
        self.pid = self.shell.pid
        self.pollOut = select.poll()
        self.pollOut.register(self.stdout)
        # Maintain mapping between file descriptors and nodes
        # This is useful for monitoring multiple nodes
        # using select.poll()
        self.outToNode[self.stdout.fileno()] = self
        self.inToNode[self.stdin.fileno()] = self
        self.execed = False
        self.lastCmd = None
        self.lastPid = None
        self.readbuf = ""
        # Wait for prompt
        while True:
            data = self.read(1024)
            if data[-1] == chr(127):
                break
            self.pollOut.poll()
        self.waiting = False
        # +m: disable job control notification
        self.cmd("unset HISTFILE; stty -echo; set +m")

        # Prepare environment for Craned
        self.setHostname()
        self.setCgroup(ver=1)

    def terminate(self):
        """
        Explicitly kill Craned process
        """
        # Kill all processes in subtree
        self.cmd(r"pkill -SIGKILL -e -f '^craned\s'")
        super().terminate()

    def setHostname(self, hostname=""):
        """
        Set hostname in new UTS namespace
        """
        if hostname == "":
            hostname = self.name
        self.cmd(f"hostname {hostname}")

    def setCgroup(self, ver=2):
        """
        Setup Cgroup for Crane
        """
        if ver == 1:
            self.cmd("mount -t tmpfs tmpfs /sys/fs/cgroup")
            # pid
            self.cmd("mkdir /sys/fs/cgroup/pids")
            self.cmd("mount -t cgroup pids -opids /sys/fs/cgroup/pids")
            # freezer
            self.cmd("mkdir /sys/fs/cgroup/freezer")
            self.cmd("mount -t cgroup freezer -ofreezer /sys/fs/cgroup/freezer")
            # cpuset
            self.cmd("mkdir /sys/fs/cgroup/cpuset")
            self.cmd("mount -t cgroup cpuset -ocpuset /sys/fs/cgroup/cpuset")
            # cpu, cpuacct
            self.cmd("mkdir /sys/fs/cgroup/cpu,cpuacct")
            self.cmd("mount -t cgroup cpu,cpuacct -ocpu,cpuacct /sys/fs/cgroup/cpu,cpuacct")
            # memory
            self.cmd("mkdir /sys/fs/cgroup/memory")
            self.cmd("mount -t cgroup memory -omemory /sys/fs/cgroup/memory")
            # devices
            self.cmd("mkdir /sys/fs/cgroup/devices")
            self.cmd("mount -t cgroup devices -odevices /sys/fs/cgroup/devices")
            # blkio
            self.cmd("mkdir /sys/fs/cgroup/blkio")
            self.cmd("mount -t cgroup blkio -oblkio /sys/fs/cgroup/blkio")
        elif ver == 2:
            self.cmd(
                "mount -t cgroup2 -o",
                "rw,nosuid,nodev,noexec,relatime,seclabel,nsdelegate,memory_recursiveprot",
                "cgroup2",
                "/sys/fs/cgroup",
            )

            # Enable controllers for subtree
            self.cmd(
                r"echo '+cpuset +cpu +io +memory +pids' > /sys/fs/cgroup/cgroup.subtree_control"
            )
        else:
            raise ValueError(f"Illegal Cgroup version: {ver}")

    def launch(self, logfile: str, stdout: str, stderr: str, reset=True):
        """
        Launch Craned process
        """
        if reset:
            self.cmd("echo >", logfile)
            self.cmd("echo >", stdout)
            self.cmd("echo >", stderr)

        self.cmdPrint(
            self.CranedExec,
            "-C",
            ConfPath,
            "-L",
            logfile,
            ">",
            stdout,
            "2>",
            stderr,
            "&",
        )


class SingleSwitchTopo(Topo):
    """Single switch connected to n hosts."""

    def __init__(self, config: NodeConfig, **opts):
        Topo.__init__(self, **opts)
        switch = self.addSwitch("switch1")

        for name, ip in config.hosts(cidr=True):
            host = self.addHost(name=name, ip=ip)
            self.addLink(
                host,
                switch,
                # bw=100,
                # delay="1ms",
                # loss=0,
                # max_queue_size=1000,
                # use_htb=True,
            )


def writeHostfile(entry: list[tuple[str, str]] = [], clean=False):
    """Generate hostfile for Crane"""
    smark = "# BEGIN Mininet hosts #\n"
    emark = "# END Mininet hosts #\n"

    # Read all and check
    with open(HostPath, "r") as file:
        lines = file.readlines()

    try:
        start = lines.index(smark)
        end = lines.index(emark) + 1
        del lines[start:end]
    except ValueError:
        if clean:
            return

    # Write back and mark
    with open(HostPath, "w") as file:
        file.writelines(lines)
        if not clean:
            file.write(smark)
            for hostname, addr in entry:
                file.write(f"{addr}\t{hostname}\n")
            file.write(emark)


def writeRoute(entry: list[tuple[str, str]], clean=False):
    """
    Check and add required route.
    Note: Routes are temporarily added. Reboot will clean them.
    """
    for dest, nexthop in entry:
        # Clean existing routes
        ret = os.popen(f"ip route del {dest}").read()
        if len(ret) and "No such process" not in ret:
            print(ret)
        if clean:
            continue

        # Write new routes
        ret = os.popen(f"ip route add {dest} via {nexthop}").read()
        if len(ret):
            print(ret)


def reset():
    cleanup()
    # Kill all craned
    os.system(r"pkill -SIGKILL -e -f '^craned\s'")
    # Reset cgroup
    os.system(r'pushd /sys/fs/cgroup/cpu; for i in $(ls | grep Crane); do cgdelete "cpu:$i" ; done; popd')
    os.system(r'pushd /sys/fs/cgroup/memory; for i in $(ls | grep Crane); do cgdelete "memory:$i" ; done; popd')
    # Reset hosts and routes
    writeHostfile(clean=True)
    try:
        writeRoute(Cluster.getRouteEntry(), clean=True)
    except NameError:
        pass


def setMaxLimit():
    """
    Set the max limit of file descriptors and process number
    """
    maxLimit = 4194304
    try:
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (maxLimit, maxLimit))
        print(f"File descriptor limit set to {maxLimit} successfully!")
    except ValueError as e:
        print(f"Error setting limit: {e}")
    except resource.error as e:
        print(f"Error setting limit: {e}")

    # Kernel settings
    kernelParams = {
        "kernel.pid_max": "4194304",
        "kernel.threads-max": "8388608",
        "net.core.somaxconn": "8192",
        "vm.max_map_count": "1677720",
        "net.ipv6.conf.default.disable_ipv6": "1",
        "net.ipv6.conf.all.disable_ipv6": "1",
    }
    for param, value in kernelParams.items():
        ret = os.system(f"sysctl -w {param}={value}")
        if ret != 0:
            print(f"Error setting {param} to {value}")
        else:
            print(f"Set {param} to {value}")


def Run(config: NodeConfig):
    """Create network and run the simulation"""
    topo = SingleSwitchTopo(config)
    net = Mininet(
        ipBase=str(config.subnet),
        topo=topo,
        host=CranedHost,  # customized host
        link=TCLink,
    )
    net.addController("c1")

    # We DO NOT need NAT. Only use this to create a gateway node.
    nat = net.addNAT(ip=f"{config.subnet[-2]}/{config.subnet.prefixlen}")
    nat.configDefault()
    # Disable firewall on gateway node
    nat.cmd("iptables -t nat -F")
    nat.cmd("iptables -F")

    net.start()

    print("Testing connectivity")
    if (
            net.pingAll()
            if config.num < 5
            else net.ping(hosts=[net.hosts[0], net.hosts[-1]])
    ) > 0:
        print("Network not fully connected, exiting...")
        return

    print("Starting craned..." + ("Dryrun=True, won't start craned" if Dryrun else ""))
    for h in net.hosts:
        # Ignore NATs
        if not isinstance(h, CranedHost):
            continue

        cranedlog = LogPath.format(h.name)
        outfile = StdoutPath.format(h.name)
        errfile = StderrPath.format(h.name)

        if not Dryrun:
            h.launch(cranedlog, outfile, errfile)
            # sleep(0.1)

    # Open CLI for debugging
    CLI(net)
    net.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Craned in Mininet")
    parser.add_argument(
        "-c",
        "--conf",
        type=str,
        default="config.yaml",
        help="cluster configuration in YAML format",
    )
    parser.add_argument("-n", "--num", type=int, help="number of virtual hosts")
    parser.add_argument(
        "--offset", type=int, help="naming offset of virtual hosts, default=1"
    )
    parser.add_argument("--subnet", type=str, help="subnet for virtual hosts")
    parser.add_argument("--crane-conf", type=str, help="`config.yaml` for Craned")
    parser.add_argument(
        "--addr", type=str, help="primary IP (CIDR) of this node used in the cluster"
    )
    parser.add_argument("--head", action="store_true", help="generate hosts and routes only")
    parser.add_argument("--dryrun", action="store_true", help="prepare the env but not start Craned")
    parser.add_argument("--clean", action="store_true", help="clean the environment")

    args = parser.parse_args()

    reset()
    if args.clean:
        exit()
    setLogLevel("info")
    setMaxLimit()

    # Always from CLI
    ConfPath = os.path.abspath(
        args.crane_conf if args.crane_conf else "/etc/crane/config.yaml"
    )
    Dryrun = args.dryrun if args.dryrun else False

    # Build ClusterConfig
    Cluster = ClusterConfig(args)

    # Generate hostfile and route
    writeHostfile(Cluster.getHostEntry())
    if len(Cluster.nodes) > 1:
        writeRoute(Cluster.getRouteEntry())

    # Only generate files for head node, do not run Mininet
    if not args.head:
        Run(Cluster.this)
