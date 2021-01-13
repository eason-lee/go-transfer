package utils

import (
	"fmt"
	"go-transfer/config"
)

// GetEsEtcdConfKey 获取 taillog 的 etcd key
func GetEsEtcdConfKey() string {
	return fmt.Sprintf("%s-%s", config.Conf.Name, config.Conf.EsConf.EtcdKey)
}
