#!/bin/sh


if [ "$BUILDBOT_DEBUG" = true ]; then
    set -e
    set -x
fi

LINUX_BENCHES="py25 py26 pgsql84 pgsql90 mysql51 oracle10g"
WINDOWS_BENCHES="mssql2005 mssql2008"
BENCHES="$LINUX_BENCHES $WINDOWS_BENCHES"

IMG=$BUILDBOT_ROOT/img
CTL=$BUILDBOT_ROOT/ctl
TMP=$BUILDBOT_ROOT/tmp

LINUX_ISO_URL=http://cdimage.debian.org/debian-cd/6.0.0/i386/iso-cd/debian-6.0.0-i386-netinst.iso
LINUX_ISO=`basename $LINUX_ISO_URL`

WGET_URL=http://downloads.sourceforge.net/gnuwin32/wget-1.11.4-1-setup.exe
WGET=`basename $WGET_URL`

WINDOWS_ISO_FILES="
    en_win_srv_2003_r2_standard_with_sp2_cd1_x13-04790.iso
    en_windows_xp_professional_with_service_pack_3_x86_cd_x14-80428.iso
"

VM_TYPE=

DATA_ROOT=test/buildbot/data

FMT=qcow2
SZ=8G
MEM=512

STATE=ready


prepare() {
    echo "Checking prerequisites..."

    if [ -z "$BUILDBOT_ROOT" ]; then
        echo "ERROR: \$BUILDBOT_ROOT variable is not set!"
        exit 2
    fi

    mkdir -p $BUILDBOT_ROOT $IMG $CTL $TMP

    for cmd in ssh ssh-keygen scp wget 7z md5sum mkisofs socat kvm kvm-img; do
        if [ -z `which $cmd` ]; then
            echo "ERROR: command not found: $cmd"
            exit 2
        fi
    done

    echo "Checking prerequisites: DONE"
}


vm_create() {
    local VM="$1"

	kvm-img create -f $FMT $IMG/$VM.$FMT $SZ >/dev/null
}


vm_launch() {
    local VM="$1"
    local OPTS="$2"

    case $VM_TYPE in
    linux)
        local NIC_MODEL=virtio
        local VGA_TYPE=vmware;;
    windows)
        local NIC_MODEL=rtl8139
        local VGA_TYPE=cirrus;;
    esac

    case $BUILDBOT_DEBUG in
    true)
        local VNC="";;
    *)
        local VNC="-vnc none";;
    esac

    kvm -name $VM -m $MEM -monitor unix:$CTL/$VM,server,nowait  \
        -drive file=$IMG/$VM.$FMT,cache=writeback               \
        -net nic,model=$NIC_MODEL -net user -vga $VGA_TYPE $VNC \
        -rtc clock=vm $OPTS
}


vm_ctl() {
    local VM="$1"
    local CMD="$2"

    echo "$CMD" | socat stdin unix-connect:$CTL/$VM
}


vm_wait() {
    local VM="$1"

    while echo '' | socat stdin unix-connect:$CTL/$VM >/dev/null 2>&1; do
        sleep 3
    done
}


vm_forward() {
    local VM="$1"
    local PORTS="$2"

    vm_ctl $VM "hostfwd_add tcp:127.0.0.1$PORTS"
}


vm_unforward() {
    local VM="$1"
    local PORTS="$2"

    vm_ctl $VM "hostfwd_remove tcp:$PORTS"
}


vm_exec() {
    local VM="$1"
    local CMD="$2"

    vm_forward $VM :10022-:22
    case $BUILDBOT_DEBUG in
    true)
        ssh -F $CTL/ssh_config $VM_TYPE-vm "$CMD";;
    *)
        ssh -F $CTL/ssh_config $VM_TYPE-vm "$CMD" >/dev/null 2>&1;;
    esac
    vm_unforward $VM :10022-:22
}


vm_cp() {
    local VM="$1"
    local FROM="$2"
    local TO="$3"

    case $VM_TYPE in
    linux)
        local SSH_USER=root;;
    windows)
        local SSH_USER=Administrator;;
    esac

    vm_forward $VM :10022-:22
    scp -F $CTL/ssh_config "$FROM" $VM_TYPE-vm:"$TO" >/dev/null 2>&1
    vm_unforward $VM :10022-:22
}


build_linux_vm() {
    local VM_TYPE=linux

    if [ -f "$IMG/linux-vm.$FMT" ]; then
        echo "Building a Linux VM: already exists"
        return
    fi

    echo "Building a Linux VM..."

    if [ ! -f "$TMP/$LINUX_ISO" ]; then
        wget -q $LINUX_ISO_URL -O $TMP/$LINUX_ISO
    fi

    if [ ! -f "$CTL/identity" ]; then
        ssh-keygen -q -N "" -f $CTL/identity
    fi

    if [ ! -f "$CTL/ssh_config" ]; then
        sed -e s#\$BUILDBOT_ROOT#$BUILDBOT_ROOT# $DATA_ROOT/ssh_config >$CTL/ssh_config
    fi

    7z x $TMP/$LINUX_ISO -o$TMP/linux-vm-iso >/dev/null

    cp $DATA_ROOT/linux-isolinux.cfg $TMP/linux-vm-iso/isolinux/isolinux.cfg
    cp $DATA_ROOT/linux-preseed.cfg $TMP/linux-vm-iso/preseed.cfg
    cp $DATA_ROOT/linux-install.sh $TMP/linux-vm-iso/install.sh
    cp $CTL/identity.pub $TMP/linux-vm-iso/identity.pub

    cd $TMP/linux-vm-iso
    md5sum `find ! -name "md5sum.txt" ! -path "./isolinux/*" -follow -type f` > md5sum.txt
    cd $OLDPWD

    mkisofs -o $TMP/linux-vm.iso                                    \
        -q -r -J -no-emul-boot -boot-load-size 4 -boot-info-table	\
        -b isolinux/isolinux.bin -c isolinux/boot.cat               \
        $TMP/linux-vm-iso

    rm -rf $TMP/linux-vm-iso

    vm_create linux-vm
    vm_launch linux-vm "-cdrom $TMP/linux-vm.iso -boot d"

    rm $TMP/linux-vm.iso

    echo "Building a Linux VM: DONE"
}


build_linux_bench() {
    local NAME="$1"
    local VM="$NAME-vm"

    local VM_TYPE=linux

    if [ -f "$IMG/$VM.$FMT" ]; then
        echo "Building a test bench '$NAME': already exists"
        return
    fi

    echo "Building a test bench '$NAME'..."

    cp $IMG/linux-vm.$FMT $IMG/$VM.$FMT

	vm_launch $VM "-daemonize"
    sleep 60

    vm_cp $VM $DATA_ROOT/$NAME-update.sh /root/update.sh
    vm_exec $VM "/root/update.sh"
    vm_exec $VM "rm /root/update.sh"
    vm_exec $VM "poweroff"
    vm_wait $VM

	vm_launch $VM "-daemonize"
    sleep 60
    vm_ctl $VM "savevm $STATE"
    vm_ctl $VM "quit"
    vm_wait $VM

    echo "Building a test bench '$NAME': DONE"
}


build_windows_vm() {
    local VM_TYPE=windows

    if [ -f "$IMG/windows-vm.$FMT" ]; then
        echo "Building a MS Windows VM: already exists"
        return
    fi

    echo "Building a MS Windows VM..."

    if [ -z "$WINDOWS_ISO_PATH" ]; then
        for ISO_FILE in $WINDOWS_ISO_FILES; do
            local ISO_PATH=`locate $ISO_FILE | head -n 1`
            if [ -n "$ISO_PATH" -a -f "$ISO_PATH" ]; then
                WINDOWS_ISO_PATH=$ISO_PATH
                break
            fi
        done
    fi

    if [ -z "$WINDOWS_ISO_PATH" -o ! -f "$WINDOWS_ISO_PATH" ]; then
        echo "ERROR: \$WINDOWS_ISO_PATH variable is not set or not a file"
        exit 2
    fi

    if [ -z "$WINDOWS_KEY_PATH" ]; then
        WINDOWS_KEY_PATH=`dirname "$WINDOWS_ISO_PATH"`/`basename "$WINDOWS_ISO_PATH" .iso`.key
    fi

    if [ -z "$WINDOWS_KEY_PATH" -o ! -f "$WINDOWS_KEY_PATH" ]; then
        echo "ERROR: \$WINDOWS_KEY_PATH variable is not set or not a file"
        exit 2
    fi

    WINDOWS_KEY=`cat "$WINDOWS_KEY_PATH" | head -n 1`

    if [ ! -f "$TMP/$WGET" ]; then
        wget -q $WGET_URL -O $TMP/$WGET
    fi

    if [ ! -f "$CTL/identity" ]; then
        ssh-keygen -q -N "" -f $CTL/identity
    fi

    7z x "$WINDOWS_ISO_PATH" -o$TMP/windows-vm-iso >/dev/null

    cp $DATA_ROOT/windows-winnt.sif $TMP/windows-vm-iso/I386/WINNT.SIF
    sed -i -e "s/#####-#####-#####-#####-#####/$WINDOWS_KEY/" $TMP/windows-vm-iso/I386/WINNT.SIF
    mkdir -p $TMP/windows-vm-iso/'$OEM$'/'$1'/INSTALL
    cp $TMP/$WGET $TMP/windows-vm-iso/'$OEM$'/'$1'/INSTALL
    cp $CTL/identity.pub $TMP/windows-vm-iso/'$OEM$'/'$1'/INSTALL
    cp $DATA_ROOT/windows-install.cmd $TMP/windows-vm-iso/'$OEM$'/'$1'/INSTALL/INSTALL.CMD

	mkisofs -o $TMP/windows-vm.iso -q -iso-level 2 -J -l -D -N      \
        -joliet-long -relaxed-filenames -no-emul-boot               \
        -boot-load-size 4 -b '[BOOT]/Bootable_NoEmulation.img'      \
        $TMP/windows-vm-iso >/dev/null 2>&1

    rm -rf $TMP/windows-vm-iso

    vm_create windows-vm
    vm_launch windows-vm "-cdrom $TMP/windows-vm.iso -boot d"

    rm $TMP/windows-vm.iso

    echo "Building a MS Windows VM: DONE"
}


build_windows_bench() {
    local NAME="$1"
    local VM="$NAME-vm"

    local VM_TYPE=windows

    if [ -f "$IMG/$VM.$FMT" ]; then
        echo "Building a test bench '$NAME': already exists"
        return
    fi

    echo "Building a test bench '$NAME'..."

    cp $IMG/windows-vm.$FMT $IMG/$VM.$FMT

	vm_launch $VM "-daemonize"
    sleep 120

    vm_cp $VM $DATA_ROOT/$NAME-update.cmd /cygdrive/c/INSTALL/UPDATE.CMD
    vm_exec $VM "reg add 'HKLM\Software\Microsoft\Windows\CurrentVersion\RunOnce' /v $VM /t REG_SZ /d 'C:\INSTALL\UPDATE.CMD' /f"
    vm_exec $VM "shutdown /r /t 0 /f"
    vm_wait $VM

	vm_launch $VM "-daemonize"
    sleep 120
    vm_ctl $VM "savevm $STATE"
    vm_ctl $VM "quit"
    vm_wait $VM

    echo "Building a test bench '$NAME': DONE"
}


start_linux_bench() {
    local NAME="$1"
    local PORTS="$2"
    local VM="$NAME-vm"

    local VM_TYPE=linux

    echo "Starting a test bench '$NAME'..."

	vm_launch $VM "-daemonize -loadvm $STATE"
    vm_forward $VM $PORTS
    sleep 5

    echo "Forwarding ports $PORTS"
    echo "Starting a test bench '$NAME': DONE"
}


start_windows_bench() {
    local NAME="$1"
    local PORTS="$2"
    local VM="$NAME-vm"

    local VM_TYPE=windows

    echo "Starting a test bench '$NAME'..."

	vm_launch $VM "-daemonize -loadvm $STATE"
    vm_forward $VM $PORTS
    sleep 5

    echo "Forwarding ports $PORTS"
    echo "Starting a test bench '$NAME': DONE"
}


stop_bench() {
    local NAME="$1"
    local VM="$NAME-vm"

    echo "Stopping a test bench '$NAME'..."

    vm_ctl $VM "quit"
    vm_wait $VM

    echo "Stopping a test bench '$NAME': DONE"
}


check() {
    local NAME="$1"
    local TARGET="$2"

    local VM="$NAME-vm"
    local CMD="cd src/htsql; make -s $TARGET"

    vm_forward $VM :10022-:22

    ssh -F $CTL/ssh_config linux-vm "$CMD" >$TMP/check-output 2>&1

    if [ $? -ne 0 ]; then
        echo "************************************************************"
        cat $TMP/check-output
        echo "************************************************************"
    fi
    rm $TMP/check-output

    vm_unforward $VM :10022-:22
}


usage() {
    echo "Usage:"
    echo "  $0 build [<bench>...]"
    echo "  $0 check [<bench>...]"
    echo "  $0 start [<bench>...]"
    echo "  $0 stop [<bench>...]"
    echo "where <bench> is one of:"
    echo "  $BENCHES"
    exit 2
}


failure() {
    echo "Fatal error, killing any stray KVM processes and existing..."
    killall -q kvm
    exit 2
}


found() {
    local VALUE="$1"
    local SET="$2"

    for ELEMENT in $SET; do
        if [ "$VALUE" = "$ELEMENT" ]; then
            return 0
        fi
    done

    return 1
}


not_found() {
    if found "$1" "$2"; then
        return 1
    else
        return 0
    fi
}


do_build() {
    local LIST="$1"

    prepare

    for BENCH in $LINUX_BENCHES; do
        if found $BENCH "$LIST"; then
            build_linux_vm
            build_linux_bench $BENCH
        fi
    done

    for BENCH in $WINDOWS_BENCHES; do
        if found $BENCH "$LIST"; then
            build_windows_vm
            build_windows_bench $BENCH
        fi
    done
}


do_check() {
    local LIST="$1"

    for CLIENT in py25 py26; do

        if ! found $CLIENT "$LIST"; then
            continue
        fi

        start_linux_bench $CLIENT :10022-:22 >/dev/null

        echo " * Testing HTSQL/$CLIENT installation"
        check $CLIENT update
        check $CLIENT deps
        check $CLIENT install
        check $CLIENT test-routine

        echo " * Testing HTSQL/$CLIENT on SQLite"
        check $CLIENT test-sqlite

        if found pgsql84 "$LIST"; then
            echo " * Testing HTSQL/$CLIENT on Postgresql 8.4"
            start_linux_bench pgsql84 :15432-:5432 >/dev/null
            check $CLIENT "test-pgsql \
                            PGSQL_HOST=10.0.2.2 \
                            PGSQL_PORT=15432 \
                            PGSQL_ADMIN_USERNAME=postgres \
                            PGSQL_ADMIN_PASSWORD=admin"
            stop_bench pgsql84 >/dev/null
        fi

        if found pgsql90 "$LIST"; then
            echo " * Testing HTSQL/$CLIENT on PostgreSQL 9.0"
            start_linux_bench pgsql90 :15432-:5432 >/dev/null
            check $CLIENT "test-pgsql \
                            PGSQL_HOST=10.0.2.2 \
                            PGSQL_PORT=15432 \
                            PGSQL_ADMIN_USERNAME=postgres \
                            PGSQL_ADMIN_PASSWORD=admin"
            stop_bench pgsql90 >/dev/null
        fi

        if found mysql51 "$LIST"; then
            echo " * Testing HTSQL/$CLIENT on MySQL 5.1"
            start_linux_bench mysql51 :13306-:3306 >/dev/null
            check $CLIENT "test-mysql \
                            MYSQL_HOST=10.0.2.2 \
                            MYSQL_PORT=13306 \
                            MYSQL_ADMIN_USERNAME=root \
                            MYSQL_ADMIN_PASSWORD=admin"
            stop_bench mysql51 >/dev/null
        fi

        if found oracle10g "$LIST"; then
            echo " * Testing HTSQL/$CLIENT on Oracle 10g"
            start_linux_bench oracle10g :11521-:1521 >/dev/null
            check $CLIENT "test-oracle \
                            ORACLE_SID=XE \
                            ORACLE_HOST=10.0.2.2 \
                            ORACLE_PORT=11521 \
                            ORACLE_ADMIN_USERNAME=system \
                            ORACLE_ADMIN_PASSWORD=admin"
            stop_bench oracle10g >/dev/null
        fi

        if found mssql2005 "$LIST"; then
            echo " * Testing HTSQL/$CLIENT on MS SQL Server 2005"
            start_windows_bench mssql2005 :11433-:1433 >/dev/null
            check $CLIENT "test-mssql \
                            MSSQL_HOST=10.0.2.2 \
                            MSSQL_PORT=11433 \
                            MSSQL_ADMIN_USERNAME=sa \
                            MSSQL_ADMIN_PASSWORD=admin"
            stop_bench mssql2005 >/dev/null
        fi

        if found mssql2008 "$LIST"; then
            echo " * Testing HTSQL/$CLIENT on MS SQL Server 2008"
            start_windows_bench mssql2008 :11433-:1433 >/dev/null
            check $CLIENT "test-mssql \
                            MSSQL_HOST=10.0.2.2 \
                            MSSQL_PORT=11433 \
                            MSSQL_ADMIN_USERNAME=sa \
                            MSSQL_ADMIN_PASSWORD=admin"
            stop_bench mssql2008 >/dev/null
        fi

        stop_bench $CLIENT >/dev/null

    done
}


do_start() {
    local LIST="$1"

    if found py25 "$LIST"; then
        start_linux_bench py25 :10022-:22
        echo For remote shell, type \'ssh -F $CTL/ssh_config linux-vm\'
    fi

    if found py26 "$LIST"; then
        start_linux_bench py26 :10022-:22
        echo For remote shell, type \'ssh -F $CTL/ssh_config linux-vm\'
    fi

    if found pgsql84 "$LIST"; then
        start_linux_bench pgsql84 :15432-:5432
    fi

    if found pgsql90 "$LIST"; then
        start_linux_bench pgsql90 :15432-:5432
    fi

    if found mysql51 "$LIST"; then
        start_linux_bench mysql51 :13306-:3306
    fi

    if found oracle10g "$LIST"; then
        start_linux_bench oracle10g :11521-:1521
    fi

    if found mssql2005 "$LIST"; then
        start_windows_bench mssql2005 :11433-:1433
    fi

    if found mssql2008 "$LIST"; then
        start_windows_bench mssql2008 :11433-:1433
    fi
}


do_stop() {
    local LIST="$1"

    for BENCH in $BENCHES; do
        if found $BENCH "$LIST"; then
            stop_bench $BENCH
        fi
    done
}


main() {
    if [ "$#" -eq "0" ]; then
        usage
    fi

    local SUBROUTINE=$1
    shift
    local ARGUMENTS="$@"

    if [ -z "$ARGUMENTS" ]; then
        ARGUMENTS="$BENCHES"
    fi

    for ARG in $ARGUMENTS; do
        if not_found $ARG "$BENCHES"; then
            echo "ERROR: invalid argument: $ARG"
            echo
            usage
        fi
    done

    case $SUBROUTINE in

    build)
        do_build "$ARGUMENTS"
        return;;

    check)
        do_check "$ARGUMENTS"
        return;;

    start)
        do_start "$ARGUMENTS"
        return;;

    stop)
        do_stop "$ARGUMENTS"
        return;;

    *)
        echo "ERROR: invalid subroutine: $SUBROUTINE"
        echo
        usage;;

    esac
}

main "$@"
exit

