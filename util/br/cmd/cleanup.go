package cmd

import (
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/cleanup"
	"go.uber.org/zap"
)

func NewCleanupCmd() *cobra.Command {
	cleanupCmd := &cobra.Command{
		Use:          "cleanup",
		Short:        "Clean up temporary files in backup",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewProduction()

			defer logger.Sync() // flushes buffer, if any
			c := cleanup.NewCleanup(cleanupConfig, logger)

			err := c.Run()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cleanupCmd.PersistentFlags().StringVar(&cleanupConfig.BackupName, "backup_name", "", "backup name")
	cleanupCmd.MarkPersistentFlagRequired("backup_name")
	cleanupCmd.PersistentFlags().StringSliceVar(&cleanupConfig.MetaServer, "meta", nil, "meta server")
	cleanupCmd.MarkPersistentFlagRequired("meta")

	return cleanupCmd
}
