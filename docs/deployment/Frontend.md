# Frontend Deployment
>This tutorial has been tested on Rocky Linux 9. In theory, it should work on any system that uses systemd (e.g., Debian/Ubuntu/AlmaLinux/Fedora, etc.).
>The software involved in this tutorial targets x86-64. If you use architectures such as ARM64, adjust the download links accordingly.
>Run all commands as the root user throughout. It is recommended to complete the backend environment installation first.

For a demo cluster with nodes:

- login01: where user login and submit job
- cranectld: control node
- craned[1-4]: compute nodes

## 1. Install Golang
```bash
GOLANG_TARBALL=go1.22.0.linux-amd64.tar.gz
# ARM architecture: wget https://dl.google.com/go/go1.22.0.linux-arm64.tar.gz
curl -L https://go.dev/dl/${GOLANG_TARBALL} -o /tmp/go.tar.gz

# Remove old Golang environment
rm -rf /usr/local/go

tar -C /usr/local -xzf /tmp/go.tar.gz && rm /tmp/go.tar.gz
echo 'export GOPATH=/root/go' >> /etc/profile.d/go.sh
echo 'export PATH=$GOPATH/bin:/usr/local/go/bin:$PATH' >> /etc/profile.d/go.sh
echo 'go env -w GO111MODULE=on' >> /etc/profile.d/go.sh
echo 'go env -w GOPROXY=https://goproxy.cn,direct' >> /etc/profile.d/go.sh

source /etc/profile.d/go.sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## 2. Install Protoc
```bash
PROTOC_ZIP=protoc-30.2-linux-x86_64.zip
curl -L https://github.com/protocolbuffers/protobuf/releases/download/v30.2/${PROTOC_ZIP} -o /tmp/protoc.zip
# ARM architecture: curl -L https://github.com/protocolbuffers/protobuf/releases/download/v23.2/protoc-23.2-linux-aarch_64.zip -o /tmp/protoc.zip
unzip /tmp/protoc.zip -d /usr/local
rm /tmp/protoc.zip /usr/local/readme.txt
```

## 3. Pull the project
```bash
git clone https://github.com/PKUHPC/CraneSched-FrontEnd.git
```

## 4. Build the project and deploy the frontend

The working directory is CraneSched-FrontEnd. In this directory, compile all Golang components and install.
```bash
cd CraneSched-FrontEnd
make
make install
```

## 5. Deploy frontend to all node

```bash
pdcp login01,cranectld,craned[1-4] build/bin/* /usr/local/bin/
pdcp login01,cranectld,craned[1-4] etc/* /usr/lib/systemd/system/

# If you need to submit interactive jobs (crun, calloc), enable Cfored:
pdsh login01,craned[1-4] systemctl enable cfored
pdsh login01,craned[1-4] systemctl start cfored
# If you configured with plugin, enable cplugind
pdsh login01,cranectld,craned[1-4] systemctl enable cplugind
pdsh login01,cranectld,craned[1-4] systemctl start cplugind
```

## 5. Install Cwrapper aliases (optional)
You can install Slurm-style aliases for Crane using the following commands, allowing you to use Crane with Slurm command forms:
```bash
cat > /etc/profile.d/cwrapper.sh << 'EOF'
alias sbatch='cwrapper sbatch'
alias sacct='cwrapper sacct'
alias sacctmgr='cwrapper sacctmgr'
alias scancel='cwrapper scancel'
alias scontrol='cwrapper scontrol'
alias sinfo='cwrapper sinfo'
alias squeue='cwrapper squeue'
alias srun='cwrapper srun'
alias salloc='cwrapper salloc'
EOF
pdcp login01,craned[1-4] /etc/profile.d/cwrapper.sh /etc/profile.d/cwrapper.sh
pdsh login01,craned[1-4] chmod 644 /etc/profile.d/cwrapper.sh
```