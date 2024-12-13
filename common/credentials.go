package common

import (
	"os"

	api_common "github.com/ydb-platform/fq-connector-go/api/common"
)

func MaybeInjectTokenToDataSourceInstance(dsi *api_common.TGenericDataSourceInstance) {
	// securely override credentials
	if token := os.Getenv("IAM_TOKEN"); token != "" {
		dsi.Credentials = &api_common.TGenericCredentials{
			Payload: &api_common.TGenericCredentials_Token{
				Token: &api_common.TGenericCredentials_TToken{
					Type:  "IAM",
					Value: token,
				},
			},
		}
	}
}
