package config

type NodeInfo struct {
	Addrs   string `yaml:"addrs"`
	RootDir string `yaml:"root"`
	DataDir string `yaml:"data"`
	User    string `yaml:"user"`
}

type BackupConfig struct {
	MetaNodes     []NodeInfo `yaml:"meta_nodes"`
	StorageNodes  []NodeInfo `yaml:"storage_nodes"`
	SpaceNames    []string   `yaml:"space_names"`
	BackendUrl    string     `yaml:"backend"`
	MaxConcurrent int        `yaml:"max_concurrent"`
}

type RestoreConfig struct {
	MetaNodes    []NodeInfo `yaml:"meta_nodes,flow"`
	StorageNodes []NodeInfo `yaml:"storage_nodes,flow"`

	BackendUrl string `yaml:"backend"`
	BackupName string `yaml:"backup_name"`
}

type CleanupConfig struct {
	BackupName string
	MetaServer []string
}
