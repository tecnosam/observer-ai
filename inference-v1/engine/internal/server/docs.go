package server

import (
	_ "embed"
	"net/http"
)

// openAPISpec describes the HTTP API. Keep it in step with the handlers;
// TestOpenAPICoversRoutes fails if a route is missing from it.
//
//go:embed openapi.yaml
var openAPISpec []byte

// docsPage renders the spec with Swagger UI. The UI's assets come from a CDN,
// so the page needs internet access; /openapi.yaml does not.
const docsPage = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Observer.ai inference engine API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#swagger-ui"});
    };
  </script>
</body>
</html>
`

func (s *Server) handleOpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(openAPISpec)
}

func (s *Server) handleDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(docsPage))
}
