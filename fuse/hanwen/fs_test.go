package hanwen

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/posixtest"
	"go.uber.org/zap"

	"github.com/beyondstorage/go-fs/vfs"
	"github.com/beyondstorage/go-service-fs/v3"
	"github.com/beyondstorage/go-storage/v4/pairs"
)

func initFS(t *testing.T) (mountPath string, srv *fuse.Server) {
	underPath := t.TempDir()
	mountPath = t.TempDir()

	logger, _ := zap.NewDevelopment()

	store, err := fs.NewStorager(pairs.WithWorkDir(underPath))
	if err != nil {
		t.Fatal("new storage", err)
	}

	srv, err = New(&Config{
		FileSystem: vfs.New(&vfs.Config{
			Store:  store,
			Logger: logger,
		}),
		MountPoint: mountPath,
		Logger:     logger,
	})
	if err != nil {
		t.Fatal("new hanwen fuse", err)
	}

	go srv.Serve()
	return
}

func TestFS(t *testing.T) {
	mountPath, srv := initFS(t)
	t.Cleanup(func() {
		err := srv.Unmount()
		t.Errorf("umount: %v", err)
	})

	for name, fn := range posixtest.All {
		t.Logf("run test for %v", name)
		fn(t, mountPath)
	}
}
