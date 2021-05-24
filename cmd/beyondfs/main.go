package main

import (
	"github.com/beyondstorage/go-fs/fuse/hanwen"
	"github.com/beyondstorage/go-fs/vfs"
	"github.com/beyondstorage/go-service-fs/v3"
	"github.com/beyondstorage/go-storage/v4/pairs"
	"go.uber.org/zap"
	"os"
)

func main() {
	logger, _ := zap.NewDevelopment()

	store, err := fs.NewStorager(pairs.WithWorkDir(os.Getenv("BEYONDFS_UNDER_PATH")))
	if err != nil {
		logger.Error("new storage", zap.Error(err))
		return
	}

	srv, err := hanwen.New(&hanwen.Config{
		FileSystem: vfs.New(&vfs.Config{
			Store:  store,
			Logger: logger,
		}),
		MountPoint: os.Getenv("BEYONDFS_MOUNT_PATH"),
		Logger:     logger,
	})
	if err != nil {
		logger.Error("new hanwen fuse", zap.Error(err))
		return
	}

	srv.Serve()
}
