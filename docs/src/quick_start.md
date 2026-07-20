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
sudo bash ./script/ubuntu/dep.sh

echo 'export PATH=$HOME/.cargo/bin/:$PATH' >> $HOME/.bashrc
```

5. Build kisekifs

```shell
just build
```

6. Run with local object storage

```shell
# `just mount` explicitly uses file:///tmp/kiseki.data and mounts on /tmp/kiseki
just mount
```

To use S3, pass the same bucket and prefix on every mount of the volume:

```shell
KISEKI_OBJECT_STORAGE_DSN='s3://my-volume-bucket/kisekifs' just mount
```

The S3 client uses the standard AWS environment and workload-identity chain.
Configure credentials through the standard AWS credential environment
variables, an ECS task role, an EC2 instance role, or web identity. Never add
credentials to the DSN. Non-secret options may be supplied as query parameters,
for example `?region=us-east-1`; a custom HTTP endpoint must also set
`allow_http=true`.

Supported DSNs are `file:///absolute/path` and `s3://bucket[/prefix]`.
`memory://` exists only for tests and does not persist data.
