package logging

import (
	"fmt"
	"math/rand"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

type staticResolver struct {
	cfg *config.TLoggingConfig_TStaticResolving
}

func (r *staticResolver) resolve(request *resolveParams) (*resolveResponse, error) {
	if len(r.cfg.Databases) == 0 {
		return nil, fmt.Errorf("no YDB endpoints provided")
	}

	// get random YDB endpoint from provided list
	ix := rand.Intn(len(r.cfg.Databases))

	endpoint := r.cfg.Databases[ix].Endpoint
	databaseName := r.cfg.Databases[ix].Name

	// pick a preconfigured folder
	folder, exists := r.cfg.Folders[request.folderId]
	if !exists {
		return nil, fmt.Errorf("folder_id '%s' is missing", request.folderId)
	}

	// resolve log group name into log group id
	logGroupId, exists := folder.LogGroups[request.logGroupName]
	if !exists {
		return nil, fmt.Errorf("log group '%s' is missing", request.logGroupName)
	}

	// FIXME: hardcoded cloud name is a mistake
	tableName := fmt.Sprintf("logs/origin/yc.logs.cloud/%s/%s", request.folderId, logGroupId)

	return &resolveResponse{
		sources: []*ydbSource{
			{
				endpoint:     endpoint,
				tableName:    tableName,
				databaseName: databaseName,
			},
		},
	}, nil
}

func (r *staticResolver) Close() error { return nil }

func newResolverStatic(cfg *config.TLoggingConfig_TStaticResolving) Resolver {
	return &staticResolver{
		cfg: cfg,
	}
}
