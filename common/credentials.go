package common

import (
	"os"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

func MaybeInjectTokenToDataSourceInstance(dsi *api_common.TDataSourceInstance) {
	// securely override credentials
	if token := os.Getenv("IAM_TOKEN"); token != "" {
		dsi.Credentials = &api_common.TCredentials{
			Payload: &api_common.TCredentials_Token{
				Token: &api_common.TCredentials_TToken{
					Type:  "IAM",
					Value: token,
				},
			},
		}
	}
}
