package conversion

import (
	"github.com/ydb-platform/fq-connector-go/app/config"
)

func NewCollection(cfg *config.TConversionConfig) Collection {
	if cfg.UseUnsafeConverters {
		return collectionUnsafe{}
	}

	return collectionDefault{}
}
