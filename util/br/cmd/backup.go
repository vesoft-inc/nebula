package cmd

import (
	"fmt"
	"io/ioutil"

	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/backup"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

var backupConfigFile string

func NewBackupCmd() *cobra.Command {
	backupCmd := &cobra.Command{
		Use:          "backup",
		Short:        "backup Nebula Graph Database",
		SilenceUsage: true,
	}

	backupCmd.AddCommand(newFullBackupCmd())
	backupCmd.PersistentFlags().StringVar(&backupConfigFile, "config", "backup.yaml", "config file path")
	backupCmd.MarkPersistentFlagRequired("config")

	return backupCmd
}

func newFullBackupCmd() *cobra.Command {
	fullBackupCmd := &cobra.Command{
		Use:   "full",
		Short: "full backup Nebula Graph Database",
		Args: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewProduction()
			defer logger.Sync() // flushes buffer, if any

			yamlFile, err := ioutil.ReadFile(backupConfigFile)
			if err != nil {
				return err
			}

			err = yaml.Unmarshal(yamlFile, &backupConfig)
			if err != nil {
				return err
			}

			for _, n := range backupConfig.MetaNodes {
				err := checkSSH(n.Addrs, n.User, logger)
				if err != nil {
					return err
				}

				if !checkPathAbs(n.RootDir) {
					logger.Error("meta's datadir must be an absolute path..", zap.String("dir", n.RootDir))
					return fmt.Errorf("meta's datadir must be an absolute path.")
				}
			}
			for _, n := range backupConfig.StorageNodes {
				err := checkSSH(n.Addrs, n.User, logger)
				if err != nil {
					return err
				}

				if !checkPathAbs(n.RootDir) {
					logger.Error("storage's datadir must be an absolute path..", zap.String("dir", n.RootDir))
					return fmt.Errorf("storage's datadir must be an absolute path.")
				}
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewProduction()

			defer logger.Sync() // flushes buffer, if any
			b := backup.NewBackupClient(backupConfig, logger)

			err := b.BackupCluster()
			if err != nil {
				return err
			}
			return nil
		},
	}

	return fullBackupCmd
}
