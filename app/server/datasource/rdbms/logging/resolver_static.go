package logging

import (
	"fmt"
	"html/template"
	"math/rand"
	"strings"

	"github.com/ydb-platform/fq-connector-go/app/config"
)

type staticResolver struct {
	cfg *config.TLoggingConfig_TStaticResolving
}

func (r *staticResolver) resolve(request *resolveRequest) (*resolveResponse, error) {
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

	// render table name from template and params
	tmpl, err := template.New("table_name").Parse(r.cfg.TableNamingPattern)
	if err != nil {
		return nil, fmt.Errorf("parse table naming pattern: %w", err)
	}

	params := struct {
		CloudName  string
		FolderID   string
		LogGroupID string
	}{
		CloudName:  folder.CloudName,
		FolderID:   request.folderId,
		LogGroupID: logGroupId,
	}

	var buf strings.Builder

	if err := tmpl.Execute(&buf, params); err != nil {
		return nil, fmt.Errorf("render table name from template `%s`: %w", r.cfg.TableNamingPattern, err)
	}

	return &resolveResponse{
		sources: []*ydbSource{
			{
				endpoint:     endpoint,
				tableName:    buf.String(),
				databaseName: databaseName,
				credentials:  request.credentials,
			},
		},
	}, nil
}

func (staticResolver) Close() error { return nil }

func newResolverStatic(cfg *config.TLoggingConfig_TStaticResolving) Resolver {
	return &staticResolver{
		cfg: cfg,
	}
}
