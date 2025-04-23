package observation

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed templates/* assets/*
var embeddedFiles embed.FS

// getTemplates loads templates from the embedded files
func getTemplates() (*template.Template, error) {
	// Create a new template with function map
	tmpl := template.New("").Funcs(template.FuncMap{
		"inc": func(i int) int {
			return i + 1
		},
		"lower": func(v any) string {
			// Handle different types gracefully
			switch value := v.(type) {
			case string:
				return strings.ToLower(value)
			case QueryState:
				return strings.ToLower(string(value))
			default:
				return strings.ToLower(fmt.Sprintf("%v", v))
			}
		},
	})

	// Parse all template files
	templateFiles, err := fs.Glob(embeddedFiles, "templates/*.html")
	if err != nil {
		return nil, err
	}

	// Read and parse each template
	for _, file := range templateFiles {
		content, err := embeddedFiles.ReadFile(file)
		if err != nil {
			return nil, err
		}

		// Add the template to the template set
		tmpl, err = tmpl.New(file).Parse(string(content))
		if err != nil {
			return nil, err
		}
	}

	return tmpl, nil
}

// getAssetHandler returns an HTTP handler for serving static assets
func getAssetHandler() http.Handler {
	// Create a sub-filesystem containing only assets
	assets, _ := fs.Sub(embeddedFiles, "assets")

	// Return a file server that serves files from this sub-filesystem
	return http.FileServer(http.FS(assets))
}
