package main

import (
	"os"

	"go.uber.org/zap"

	"github.com/beyondstorage/beyond-fs/fuse/hanwen"
	"github.com/beyondstorage/beyond-fs/vfs"
)

func main() {
	logger, _ := zap.NewDevelopment()

	cfg := &vfs.Config{
		StoragePath: os.Getenv("BEYONDFS_UNDER_PATH"),

		Logger: logger,
	}

	fs, err := vfs.NewFS(cfg)
	if err != nil {
		logger.Error("new fs", zap.Error(err))
		return
	}

	srv, err := hanwen.New(&hanwen.Config{
		FileSystem: fs,
		MountPoint: os.Getenv("BEYONDFS_MOUNT_PATH"),
		Logger:     logger,
	})
	if err != nil {
		logger.Error("new hanwen fuse", zap.Error(err))
		return
	}

	srv.Serve()
}
