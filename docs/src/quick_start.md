# QuickStart

Run kisekifs in a light-weight virtual machine.

## 1. Install lima

https://github.com/lima-vm/lima

## 2. Start a ubuntu virtual machine

1. create a virtual machine
```shell
limactl start \
  --name=kiseki \
  --cpus=2 \
  --memory=8 \
  template://ubuntu-lts
```

2. go into the virtual machine
```shell
limactl shell kiseki
```

3. Install git and download repo

```shell
sudo apt-get update -y && sudo apt-get install git

git clone https://github.com/crrow/kisekifs && cd kisekifs
```

4. Prepare environments

```shell
# Set proxy: export HTTP_PROXY=http://192.168.5.2:7890,HTTPS_PROXY=http://192.168.5.2:7890
sudo bash ./hack/ubuntu/prepare.sh

echo 'export PATH=$HOME/.cargo/bin/:$PATH' >> $HOME/.bashrc
```

5. Build kisekifs

```shell
just build
```

6. Run 

```shell
# command [just mount] will mount kisekifs on /tmp/kiseki
just minio && just mount
```