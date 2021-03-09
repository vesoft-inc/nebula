package cmd

import (
	"fmt"
	"io/ioutil"

	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-storage/util/br/pkg/restore"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

var configFile string

func NewRestoreCMD() *cobra.Command {
	restoreCmd := &cobra.Command{
		Use:          "restore",
		Short:        "restore Nebula Graph Database",
		SilenceUsage: true,
	}

	restoreCmd.AddCommand(newFullRestoreCmd())
	restoreCmd.PersistentFlags().StringVar(&configFile, "config", "restore.yaml", "config file path")
	restoreCmd.MarkPersistentFlagRequired("config")

	return restoreCmd
}

func newFullRestoreCmd() *cobra.Command {
	fullRestoreCmd := &cobra.Command{
		Use:   "full",
		Short: "full restore Nebula Graph Database",
		Args: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewProduction()
			defer logger.Sync() // flushes buffer, if any
			yamlFile, err := ioutil.ReadFile(configFile)
			if err != nil {
				return err
			}

			err = yaml.Unmarshal(yamlFile, &restoreConfig)
			if err != nil {
				return err
			}

			for _, n := range restoreConfig.MetaNodes {
				err := checkSSH(n.Addrs, n.User, logger)
				if err != nil {
					return err
				}
				if !checkPathAbs(n.DataDir) {
					logger.Error("StorageDataDir must be an absolute path.", zap.String("dir", n.DataDir))
					return fmt.Errorf("StorageDataDir must be an absolute path")
				}

				if !checkPathAbs(n.RootDir) {
					logger.Error("StorageDataDir must be an absolute path.", zap.String("dir", n.RootDir))
					return fmt.Errorf("StorageDataDir must be an absolute path")
				}
			}

			if restoreConfig.MaxConcurrent <= 0 {
				restoreConfig.MaxConcurrent = 5
			}

			if len(restoreConfig.BackupName) == 0 {
				logger.Error("The backup_name configuration must be set")
				return fmt.Errorf("The backup_name configuration must be set")
			}

			return nil
		},

		RunE: func(cmd *cobra.Command, args []string) error {
			// nil mean backup all space
			logger, _ := zap.NewProduction()

			defer logger.Sync() // flushes buffer, if any

			r := restore.NewRestore(restoreConfig, logger)
			err := r.RestoreCluster()
			if err != nil {
				return err
			}
			return nil
		},
	}

	return fullRestoreCmd
}
