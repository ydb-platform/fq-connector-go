package config

import (
	_ "embed"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

var (
	//go:embed config.debug.txt
	prototextConfig string
	//go:embed config.debug.yaml
	yamlConfig string
)

func TestConfig(t *testing.T) {
	files := map[string]string{
		"prototext": prototextConfig,
		"yaml":      yamlConfig,
	}

	for key, body := range files {
		key := key
		body := body

		t.Run(key, func(t *testing.T) {
			f, err := os.CreateTemp("", "test-config")
			require.NoError(t, err)

			path := f.Name()

			_, err = f.WriteString(body)
			require.NoError(t, err)

			err = f.Close()
			require.NoError(t, err)

			defer os.Remove(path)

			cfg, err := NewConfigFromFile(path)
			require.NoError(t, err)
			require.NotNil(t, cfg)

			require.Equal(t, "0.0.0.0", cfg.ConnectorServer.Endpoint.Host)
			require.Equal(t, uint32(2130), cfg.ConnectorServer.Endpoint.Port)
			require.Equal(t, config.ELogLevel_DEBUG, cfg.Logger.LogLevel)
			require.Equal(t, true, cfg.Logger.EnableSqlQueryLogging)
			require.Equal(t, "0.0.0.0", cfg.PprofServer.Endpoint.Host)
			require.Equal(t, uint32(6060), cfg.PprofServer.Endpoint.Port)
			require.Equal(t, uint64(4*(1<<20)), cfg.Paging.BytesPerPage)
			require.Equal(t, uint32(2), cfg.Paging.PrefetchQueueCapacity)
			require.Equal(t, true, cfg.Conversion.UseUnsafeConverters)
		})
	}
}
