#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from .util import load_fabfile_env, execute
from fabric.api import abort, local, run, put, output, settings, prompt
from fabric.network import disconnect_all
import os, os.path, urllib2, shutil, socket, time


__all__ = ['vm_build', 'vm_start', 'vm_stop', 'vm_shell', 'vm_vnc']


BUILDBOT_ROOT = "./vm"
IMG_DIR = os.path.join(BUILDBOT_ROOT, "img")
CTL_DIR = os.path.join(BUILDBOT_ROOT, "ctl")
TMP_DIR = os.path.join(BUILDBOT_ROOT, "tmp")

DATA_ROOT = "./fabfile/data/bb"

VM_DISK = "8G"
VM_MEM = "512"

DEBIAN6_ISO_URLS = [
    "http://cdimage.debian.org/cdimage/archive/6.0.3/i386/iso-cd/debian-6.0.3-i386-netinst.iso",
    "http://cdimage.debian.org/cdimage/release/6.0.3/i386/iso-cd/debian-6.0.3-i386-netinst.iso",
]
WGET_INSTALL_URL = "http://downloads.sourceforge.net/gnuwin32/wget-1.11.4-1-setup.exe"
WINDOWS5_ISO_FILES = [
        "en_win_srv_2003_r2_standard_with_sp2_cd1_x13-04790.iso",
        "en_windows_xp_professional_with_service_pack_3_x86_cd_x14-80428.iso",
]


class VM(object):
    
    name = None

    @classmethod
    def list(cls):
        vm_classes = [cls]
        idx = 0
        while idx < len(vm_classes):
            vm_class = vm_classes[idx]
            vm_classes.extend(vm_class.__subclasses__())
            idx += 1
        vm_classes = [vm_class for vm_class in vm_classes
                               if vm_class.name is not None]
        return vm_classes

    @classmethod
    def make(cls, name):
        vm_classes = cls.list()
        vm_classes = [vm_class for vm_class in vm_classes
                               if vm_class.name == name]
        assert len(vm_classes) <= 1, name
        if len(vm_classes) != 1:
            return
        [vm_class] = vm_classes
        return vm_class()

    def __init__(self):
        assert self.name is not None
        self.img_path = os.path.join(IMG_DIR, "%s.qcow2" % self.name)
        self.ctl_path = os.path.join(CTL_DIR, "%s.ctl" % self.name)

    def present(self):
        return os.path.isfile(self.img_path)

    def running(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(self.ctl_path)
            sock.close()
        except socket.error:
            return False
        return True

    def wait(self):
        while self.running():
            time.sleep(1.0)

    def kvm_img(self):
        local("kvm-img create -f qcow2 %s %s" % (self.img_path, VM_DISK))

    def kvm(self, opts=None):
        net_model = "virtio"
        if "win" in self.name or 'ms' in self.name:
            net_model = "rtl8139"
        local("kvm -name %s -monitor unix:%s,server,nowait"
              " -drive file=%s,cache=writeback"
              " -net nic,model=%s -net user -vga cirrus"
              " -rtc clock=vm"
              % (self.name, self.ctl_path, self.img_path, net_model)
              + ((" "+opts) if opts else "")
              + ("" if output.debug else " -vnc none"))

    def ctl(self, command):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.ctl_path)
        time.sleep(0.1)
        data = sock.recv(4096)
        if output.debug:
            print "[%s]> %s" % (self.name, data)
        if output.debug:
            print "[%s]< %s" % (self.name, command)
        sock.send(command+"\n")
        time.sleep(0.1)
        data = sock.recv(4096)
        if output.debug:
            print "[%s]> %s" % (self.name, data)
        sock.close()

    def forward(self, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('127.0.0.1', port+10000))
            sock.close()
            is_taken = True
        except socket.error:
            is_taken = False
        port_path = os.path.join(CTL_DIR, "tcp.%s" % port)
        if is_taken:
            if os.path.exists(port_path):
                name = open(port_path).read().strip()
                if name != self.name:
                    abort("port %s is already taken by VM %r"
                          % (port, name))
            else:
                abort("port %s is already taken" % port)
        self.ctl("hostfwd_add tcp:127.0.0.1:%s-:%s"
                 % (port+10000, port))
        open(port_path, 'w').write("%s\n" % self.name)

    def unforward(self, port):
        self.ctl("hostfwd_remove tcp::%s-:%s"
                 % (port+10000, port))
        port_path = os.path.join(CTL_DIR, "tcp.%s" % port)
        if os.path.exists(port_path):
            os.unlink(port_path)

    def build(self):
        assert False

    def start(self):
        if not self.present():
            abort("VM %r is not built" % self.name)
        if self.running():
            abort("VM %r is already running" % self.name)
        self.kvm("-daemonize")

    def stop(self):
        if not self.present():
            abort("VM %r is not built" % self.name)
        if not self.running():
            abort("VM %r is not running" % self.name)
        self.ctl("quit")
        self.wait()


class Debian6VM(VM):

    name = 'debian6'

    def build(self):
        if self.present():
            abort("VM %r is already built" % self.name)

        for path in [IMG_DIR, CTL_DIR, TMP_DIR]:
            if not os.path.isdir(path):
                os.makedirs(path)

        print "Building VM %r:" % self.name

        debian6_path = os.path.join(TMP_DIR,
                                    os.path.basename(DEBIAN6_ISO_URLS[0]))
        if not os.path.exists(debian6_path):
            print "- downloading Debian ISO image"
            print "=> %s" % debian6_path
            data = None
            for url in DEBIAN6_ISO_URLS:
                try:
                    data = urllib2.urlopen(url).read()
                except urllib2.HTTPError:
                    pass
                else:
                    break
            if data is None:
                abort("cannot download Debian ISO image")
            stream = open(debian6_path, 'w')
            stream.write(data)
            stream.close()

        identity_path = os.path.join(CTL_DIR, "identity")
        config_path = os.path.join(CTL_DIR, "ssh_config")
        if not (os.path.exists(identity_path) and 
                os.path.join(config_path)):
            print "- generating an SSH key"
            local("ssh-keygen -q -N \"\" -f %s" % identity_path)
            config_template_path = os.path.join(DATA_ROOT, "ssh_config")
            config_template = open(config_template_path).read()
            config = config_template.replace("$BUILDBOT_ROOT",
                                             BUILDBOT_ROOT)
            open(config_path, 'w').write(config)

        print "- repacking the ISO image"
        image_path = os.path.join(TMP_DIR, "debian6")
        if os.path.exists(image_path):
            shutil.rmtree(image_path)
        local("7z x %s -o%s >/dev/null" % (debian6_path, image_path))
        for src, dst in [("debian6-isolinux.cfg", "isolinux/isolinux.cfg"),
                         ("debian6-preseed.cfg", "preseed.cfg"),
                         ("debian6-install.sh", "install.sh")]:
            src = os.path.join(DATA_ROOT, src)
            dst = os.path.join(image_path, dst)
            print "%s => %s" % (src, dst)
            shutil.copy(src, dst)
        src = identity_path+".pub"
        dst = os.path.join(image_path, "identity.pub")
        print "%s => %s" % (src, dst)
        shutil.copy(src, dst)
        local("cd %s && md5sum"
              " `find ! -name \"md5sum.txt\""
              " ! -path \"./isolinux/*\" -follow -type f` > md5sum.txt"
              % image_path)
        iso_path = os.path.join(TMP_DIR, "debian6.iso")
        if os.path.exists(iso_path):
            os.unlink(iso_path)
        local("mkisofs -o %s"
              " -q -r -J -no-emul-boot -boot-load-size 4 -boot-info-table"
              " -b isolinux/isolinux.bin -c isolinux/boot.cat %s"
              % (iso_path, image_path))
        shutil.rmtree(image_path)

        print "- installing Debian VM"
        try:
            self.kvm_img()
            self.kvm("-cdrom %s -boot d" % iso_path)
        except:
            if os.path.exists(self.img_path):
                os.unlink(self.img_path)
            raise
        os.unlink(iso_path)

        print "Building VM %r: DONE" % self.name


class LinuxBenchVM(Debian6VM):

    name = None
    port = None

    def build(self):
        if self.present():
            abort("VM %r is already built" % self.name)

        debian_vm = VM.make("debian6")
        if not debian_vm.present():
            debian_vm.build()
        if debian_vm.running():
            abort("VM %r must be stopped before building VM %r"
                  % (debian_vm.name, self.name))

        identity_path = os.path.join(CTL_DIR, "identity")
        try:
            print "%s => %s" % (debian_vm.img_path, self.img_path)
            shutil.copy(debian_vm.img_path, self.img_path)

            self.kvm("-daemonize")
            self.forward(22)
            time.sleep(60.0)
            with settings(host_string="root@127.0.0.1:10022",
                          no_keys=True,
                          key_filename=identity_path):
                local_path = os.path.join(DATA_ROOT,
                                          "%s-update.sh" % self.name)
                put(local_path, "/root/update.sh",
                    mirror_local_mode=True)
                run("/root/update.sh")
                run("rm /root/update.sh")
                run("poweroff")
                disconnect_all()
            self.wait()

            self.kvm("-daemonize")
            time.sleep(60.0)
            self.ctl("savevm begin")
            self.ctl("quit")
            self.wait()
        except:
            if self.running():
                self.stop()
            if os.path.exists(self.img_path):
                os.unlink(self.img_path)
            raise

    def start(self):
        if not self.present():
            abort("VM %r is not built" % self.name)
        if self.running():
            abort("VM %r is already running" % self.name)
        self.kvm("-daemonize -loadvm begin")
        if self.port is not None:
            self.forward(self.port)


class Py25BenchVM(LinuxBenchVM):

    name = 'py25'
    port = 22


class Py26BenchVM(LinuxBenchVM):

    name = 'py26'
    port = 22


class Py27BenchVM(LinuxBenchVM):

    name = 'py27'
    port = 22


class PGSQL84BenchVM(LinuxBenchVM):

    name = 'pgsql84'
    port = 5432


class PGSQL90BenchVM(LinuxBenchVM):

    name = 'pgsql90'
    port = 5432


class MySQL51BenchVM(LinuxBenchVM):

    name = 'mysql51'
    port = 3306


class Oracle10gBenchVM(LinuxBenchVM):

    name = 'oracle10g'
    port = 1521


class Windows5VM(VM):

    name = 'windows5'

    def build(self):
        if self.present():
            abort("VM %r is already built" % self.name)

        for path in [IMG_DIR, CTL_DIR, TMP_DIR]:
            if not os.path.isdir(path):
                os.makedirs(path)

        print "Building VM %r:" % self.name

        path = None
        windows5_iso = os.environ.get('WINDOWS5_ISO')
        if windows5_iso and os.path.isfile(windows5_iso):
            path = windows5_iso
        if path is None:
            for filename in WINDOWS5_ISO_FILES:
                output = local("locate --null %s || true"
                               % filename, capture=True)
                for line in output.split('\0'):
                    if not line:
                        continue
                    if os.path.isfile(line):
                        path = line
                        break
                if path is not None:
                    break
        if path is None:
            path = prompt("Path to Windows XP/2003 ISO:")
            assert os.path.isfile(path)
        root = os.path.splitext(path)[0]
        key_path = root + ".key"
        if os.path.isfile(key_path):
            key = open(key_path).readline().strip()
        else:
            key = prompt("Key to the ISO:")

        wget_path = os.path.join(TMP_DIR, os.path.basename(WGET_INSTALL_URL))
        if not os.path.exists(wget_path):
            data = urllib2.urlopen(WGET_INSTALL_URL).read()
            stream = open(wget_path, 'w')
            stream.write(data)
            stream.close()

        identity_path = os.path.join(CTL_DIR, "identity")
        config_path = os.path.join(CTL_DIR, "ssh_config")
        if not (os.path.exists(identity_path) and 
                os.path.join(config_path)):
            print "- generating an SSH key"
            local("ssh-keygen -q -N \"\" -f %s" % identity_path)
            config_template_path = os.path.join(DATA_ROOT, "ssh_config")
            config_template = open(config_template_path).read()
            config = config_template.replace("$BUILDBOT_ROOT",
                                             BUILDBOT_ROOT)
            open(config_path, 'w').write(config)

        print "- repacking the ISO image"
        image_path = os.path.join(TMP_DIR, self.name)
        if os.path.exists(image_path):
            shutil.rmtree(image_path)
        local("7z x %s -o%s >/dev/null" % (path, image_path))
        sif = open(os.path.join(DATA_ROOT, "windows5-winnt.sif")).read()
        sif = sif.replace("#####-#####-#####-#####-#####", key)
        open(os.path.join(image_path, "I386/WINNT.SIF"), 'w').write(sif)
        install_path = os.path.join(image_path, "$OEM$/$1/INSTALL")
        os.makedirs(install_path)
        shutil.copy(wget_path, install_path)
        shutil.copy(identity_path+".pub", install_path)
        shutil.copy(os.path.join(DATA_ROOT, "windows5-install.cmd"),
                    os.path.join(install_path, "INSTALL.CMD"))

        iso_path = os.path.join(TMP_DIR, self.name+".iso")
        if os.path.exists(iso_path):
            os.unlink(iso_path)
        local("mkisofs -o %s -q -iso-level 2 -J -l -D -N"
              " -joliet-long -relaxed-filenames -no-emul-boot"
              " -boot-load-size 4 -b '[BOOT]/Bootable_NoEmulation.img'"
              " %s" % (iso_path, image_path))
        shutil.rmtree(image_path)

        print "- installing Windows VM"
        try:
            self.kvm_img()
            self.kvm("-cdrom %s -boot d" % iso_path)
        except:
            if os.path.exists(self.img_path):
                os.unlink(self.img_path)
            raise
        os.unlink(iso_path)

        print "Building VM %r: DONE" % self.name


class WindowsBenchVM(Windows5VM):

    name = None
    port = None

    def build(self):
        if self.present():
            abort("VM %r is already built" % self.name)

        windows_vm = VM.make("windows5")
        if not windows_vm.present():
            windows_vm.build()
        if windows_vm.running():
            abort("VM %r must be stopped before building VM %r"
                  % (windows_vm.name, self.name))

        identity_path = os.path.join(CTL_DIR, "identity")
        try:
            print "%s => %s" % (windows_vm.img_path, self.img_path)
            shutil.copy(windows_vm.img_path, self.img_path)

            self.kvm("-daemonize")
            self.forward(22)
            time.sleep(120.0)
            with settings(host_string="Administrator@127.0.0.1:10022",
                          no_keys=True,
                          key_filename=identity_path):
                local_path = os.path.join(DATA_ROOT,
                                          "%s-update.cmd" % self.name)
                put(local_path, "/cygdrive/c/INSTALL/UPDATE.CMD")
                run("reg add 'HKLM\Software\Microsoft\Windows\CurrentVersion\RunOnce'"
                    " /v %s /t REG_SZ /d 'C:\INSTALL\UPDATE.CMD' /f" % self.name)
                run("shutdown /r /t 0 /f")
                disconnect_all()
            self.wait()

            self.kvm("-daemonize")
            time.sleep(120.0)
            self.ctl("savevm begin")
            self.ctl("quit")
            self.wait()
        except:
            if self.running():
                self.stop()
            if os.path.exists(self.img_path):
                os.unlink(self.img_path)
            raise

    def start(self):
        if not self.present():
            abort("VM %r is not built" % self.name)
        if self.running():
            abort("VM %r is already running" % self.name)
        self.kvm("-daemonize -loadvm begin")
        if self.port is not None:
            self.forward(self.port)


class MSSQL2005VM(WindowsBenchVM):

    name = 'mssql2005'
    port = 1433


class MSSQL2008VM(WindowsBenchVM):

    name = 'mssql2008'
    port = 1433


def vm_build(*names):
    """build a virtual machine"""
    load_fabfile_env()
    vms = []
    for name in names:
        vm = VM.make(name)
        if vm is None:
            abort("unknown VM %r" % name)
        vms.append(vm)
    if not vms:
        vms = [vm_class() for vm_class in VM.list()]
    for vm in vms:
        if vm.present():
            print "VM %r already exists" % vm.name
            continue
        vm.build()


def vm_delete(*names):
    """delete a virtual machine"""
    load_fabfile_env()


def vm_start(*names):
    """start a virtual machine"""
    load_fabfile_env()
    if not names:
        abort("a VM name is expected")
    vms = []
    for name in names:
        vm = VM.make(name)
        if vm is None:
            abort("unknown VM %r" % name)
        if not vm.present():
            abort("VM %r is not built" % name)
        vms.append(vm)
    for vm in vms:
        if vm.running():
            print "VM %r is already running" % vm.name
            continue
        vm.start()


def vm_stop(*names):
    """stop a virtual machine"""
    load_fabfile_env()
    vms = []
    for name in names:
        vm = VM.make(name)
        if vm is None:
            abort("unknown VM %r" % name)
        if not vm.present():
            abort("VM %r is not built" % name)
        vms.append(vm)
    if not vms:
        vms = [vm_class() for vm_class in VM.list()]
    for vm in vms:
        if vm.running():
            vm.stop()


def vm_shell(name):
    """open a shell to a virtual machine"""
    load_fabfile_env()
    vm = VM.make(name)
    if vm is None:
        abort("unknown VM %r" % name)
    if not vm.running():
        print "VM %r is not running" % vm.name
        return
    vm.forward(22)
    host = "linux-vm"
    if 'win' in name or 'ms' in name:
        host = "windows-vm"
    execute("ssh -F %s %s"
            % (os.path.join(CTL_DIR, "ssh_config"), host))


def vm_vnc(name):
    """open a VNC session to a virtual machine"""
    load_fabfile_env()


