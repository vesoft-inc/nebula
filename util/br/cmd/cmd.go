package cmd

import "github.com/vesoft-inc/nebula-storage/util/br/pkg/config"

var (
	backupConfig  config.BackupConfig
	restoreConfig config.RestoreConfig
	// for cleanup
	cleanupConfig config.CleanupConfig
)
