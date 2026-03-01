package shim

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/containerd/log"
	"github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

// cleanIPCShm removes all SysV shared memory segments from the container's IPC
// namespace. CRIU's restore uses shmget with IPC_CREAT|IPC_EXCL, which fails
// if segments already exist in the pod's IPC namespace (kept alive by the pause
// container across checkpoint/restore cycles).
func cleanIPCShm(ctx context.Context, spec *specs.Spec) error {
	ipcNSPath, err := GetIPCNS(spec)
	if err != nil {
		return fmt.Errorf("get IPC namespace path: %w", err)
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	origFd, err := unix.Open("/proc/self/ns/ipc", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open current IPC ns: %w", err)
	}
	defer unix.Close(origFd)

	targetFd, err := unix.Open(ipcNSPath, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open target IPC ns %s: %w", ipcNSPath, err)
	}
	defer unix.Close(targetFd)

	if err := unix.Setns(targetFd, unix.CLONE_NEWIPC); err != nil {
		return fmt.Errorf("setns to target IPC ns: %w", err)
	}
	defer func() {
		if err := unix.Setns(origFd, unix.CLONE_NEWIPC); err != nil {
			log.G(ctx).Errorf("failed to restore original IPC ns: %s", err)
		}
	}()

	shmIDs, err := parseSysVShmIDs()
	if err != nil {
		return fmt.Errorf("parse sysvipc shm: %w", err)
	}
	if len(shmIDs) == 0 {
		return nil
	}

	removed := 0
	for _, id := range shmIDs {
		if _, err := unix.SysvShmCtl(id, unix.IPC_RMID, nil); err != nil {
			log.G(ctx).Warnf("shmctl(IPC_RMID, %d): %s", id, err)
			continue
		}
		removed++
	}

	log.G(ctx).Infof("cleaned %d/%d SysV shm segments from IPC namespace", removed, len(shmIDs))
	return nil
}

const sysVShmPath = "/proc/sysvipc/shm"

func parseSysVShmIDs() ([]int, error) {
	f, err := os.Open(sysVShmPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ids []int
	scanner := bufio.NewScanner(f)
	scanner.Scan()
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		id, err := strconv.Atoi(fields[1])
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, scanner.Err()
}
