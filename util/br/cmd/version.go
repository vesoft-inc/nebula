package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var version string = "2.0"

func NewVersionCmd() *cobra.Command {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "print the version of nebula br",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println(version)
			return nil
		},
	}
	return versionCmd
}
