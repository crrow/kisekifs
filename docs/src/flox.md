# Quick Start

## 1. Start virtual machine locally

This project provides a lima file for setting up development quickly.

All you need to do is installing the [lima](https://lima-vm.io/),
and run the following code. It will setup an virtual machine, and installing some dependencies,
and you don't need to worry about messing up your system.

But before you start, you have to modify the mount point of the configuration, replace the mount path
to your's.

### 1. Create virtual machine

```shell
just create-lima
```

### 2. Start virtual machine

```shell
just start-lima
```

### 3. Go into virtual machine and install dependencies

```shell
sudo bash ./script/ubuntu/dep.sh
```

### 4. Project has prepared

