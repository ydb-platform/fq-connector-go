package table_store_type_cache

import (
	"fmt"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type Key struct {
	Host     string
	Port     uint32
	Database string
	Table    string
}

func (k Key) serialize() string {
	return fmt.Sprintf("%s:%d_%s_%s", k.Host, k.Port, k.Database, k.Table)
}

func NewKeyFromDatasourceInstance(
	dsi *api_common.TGenericDataSourceInstance,
	table string,
) *Key {
	return &Key{
		Host:     dsi.Endpoint.Host,
		Port:     dsi.Endpoint.Port,
		Database: dsi.Database,
		Table:    table,
	}
}

type Cache interface {
	Put(*Key, options.StoreType)
	Get(*Key) (storeType options.StoreType, found bool)
}
