package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewOpenAPIHandler(t *testing.T) {
	handler := NewOpenAPIHandler()
	if handler == nil {
		t.Fatal("NewOpenAPIHandler returned nil")
	}
}

func TestOpenAPIHandler_ServeHTTP_GET(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got '%s'", ct)
	}

	// Parse the response
	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// Verify OpenAPI version
	if openapi, ok := spec["openapi"].(string); !ok || openapi != "3.0.3" {
		t.Errorf("Expected openapi '3.0.3', got '%v'", spec["openapi"])
	}
}

func TestOpenAPIHandler_ServeHTTP_MethodNotAllowed(t *testing.T) {
	handler := NewOpenAPIHandler()

	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/openapi.json", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("Expected status 405 for %s, got %d", method, rec.Code)
			}

			var resp map[string]interface{}
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("Failed to decode response: %v", err)
			}

			if resp["error"] != "Method Not Allowed" {
				t.Errorf("Expected error 'Method Not Allowed', got '%v'", resp["error"])
			}
			if resp["code"] != float64(405) {
				t.Errorf("Expected code 405, got '%v'", resp["code"])
			}
		})
	}
}

func TestOpenAPIHandler_Spec_Info(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	info, ok := spec["info"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'info' object in spec")
	}

	// Verify info fields
	if info["title"] != "Caddy DuckDB REST API" {
		t.Errorf("Expected title 'Caddy DuckDB REST API', got '%v'", info["title"])
	}
	if info["version"] != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%v'", info["version"])
	}

	// Verify contact
	contact, ok := info["contact"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'contact' object in info")
	}
	if contact["url"] != "https://github.com/tobilg/caddyserver-duckdb-module" {
		t.Errorf("Unexpected contact URL: %v", contact["url"])
	}

	// Verify license
	license, ok := info["license"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'license' object in info")
	}
	if license["name"] != "MIT" {
		t.Errorf("Expected license 'MIT', got '%v'", license["name"])
	}
}

func TestOpenAPIHandler_Spec_Servers(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	servers, ok := spec["servers"].([]interface{})
	if !ok {
		t.Fatal("Expected 'servers' array in spec")
	}
	if len(servers) != 1 {
		t.Errorf("Expected 1 server, got %d", len(servers))
	}

	server, ok := servers[0].(map[string]interface{})
	if !ok {
		t.Fatal("Expected server object")
	}
	if server["url"] != "/duckdb" {
		t.Errorf("Expected server url '/duckdb', got '%v'", server["url"])
	}
}

func TestOpenAPIHandler_Spec_Tags(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	tags, ok := spec["tags"].([]interface{})
	if !ok {
		t.Fatal("Expected 'tags' array in spec")
	}
	if len(tags) != 3 {
		t.Errorf("Expected 3 tags, got %d", len(tags))
	}

	// Verify tag names
	expectedTags := map[string]bool{"CRUD": false, "Query": false, "OpenAPI": false}
	for _, tag := range tags {
		tagMap, ok := tag.(map[string]interface{})
		if !ok {
			continue
		}
		if name, ok := tagMap["name"].(string); ok {
			expectedTags[name] = true
		}
	}

	for name, found := range expectedTags {
		if !found {
			t.Errorf("Expected tag '%s' not found", name)
		}
	}
}

func TestOpenAPIHandler_Spec_Paths(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	paths, ok := spec["paths"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'paths' object in spec")
	}

	// Verify expected paths exist
	expectedPaths := []string{"/openapi.json", "/api/{table}", "/query", "/query/{sql}/result.{format}"}
	for _, path := range expectedPaths {
		if _, ok := paths[path]; !ok {
			t.Errorf("Expected path '%s' not found", path)
		}
	}
}

func TestOpenAPIHandler_Spec_CRUDOperations(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	paths := spec["paths"].(map[string]interface{})
	apiPath, ok := paths["/api/{table}"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected '/api/{table}' path")
	}

	// Verify all CRUD methods exist
	methods := []string{"get", "post", "put", "delete"}
	for _, method := range methods {
		if _, ok := apiPath[method]; !ok {
			t.Errorf("Expected '%s' method on /api/{table}", method)
		}
	}

	// Verify GET operation has required parameters
	getOp, ok := apiPath["get"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected GET operation")
	}

	params, ok := getOp["parameters"].([]interface{})
	if !ok {
		t.Fatal("Expected parameters in GET operation")
	}

	// Check for pagination parameters
	paramNames := make(map[string]bool)
	for _, param := range params {
		paramMap, ok := param.(map[string]interface{})
		if !ok {
			continue
		}
		if name, ok := paramMap["name"].(string); ok {
			paramNames[name] = true
		}
	}

	expectedParams := []string{"page", "limit", "filter", "sort", "links"}
	for _, name := range expectedParams {
		if !paramNames[name] {
			t.Errorf("Expected parameter '%s' in GET /api/{table}", name)
		}
	}
}

func TestOpenAPIHandler_Spec_QueryOperations(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	paths := spec["paths"].(map[string]interface{})

	// Verify POST /query
	queryPath, ok := paths["/query"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected '/query' path")
	}
	if _, ok := queryPath["post"]; !ok {
		t.Error("Expected 'post' method on /query")
	}

	// Verify GET /query/{sql}/result.{format}
	queryGetPath, ok := paths["/query/{sql}/result.{format}"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected '/query/{sql}/result.{format}' path")
	}
	if _, ok := queryGetPath["get"]; !ok {
		t.Error("Expected 'get' method on /query/{sql}/result.{format}")
	}
}

func TestOpenAPIHandler_Spec_Components(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	components, ok := spec["components"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'components' object in spec")
	}

	// Verify security schemes
	securitySchemes, ok := components["securitySchemes"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'securitySchemes' in components")
	}
	apiKeyAuth, ok := securitySchemes["ApiKeyAuth"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'ApiKeyAuth' security scheme")
	}
	if apiKeyAuth["type"] != "apiKey" {
		t.Errorf("Expected type 'apiKey', got '%v'", apiKeyAuth["type"])
	}
	if apiKeyAuth["in"] != "header" {
		t.Errorf("Expected in 'header', got '%v'", apiKeyAuth["in"])
	}
	if apiKeyAuth["name"] != "X-API-Key" {
		t.Errorf("Expected name 'X-API-Key', got '%v'", apiKeyAuth["name"])
	}
}

func TestOpenAPIHandler_Spec_Schemas(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	components := spec["components"].(map[string]interface{})
	schemas, ok := components["schemas"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'schemas' in components")
	}

	// Verify all expected schemas exist
	expectedSchemas := []string{
		"ErrorResponse",
		"SuccessResponse",
		"DryRunResponse",
		"ReadResponse",
		"Pagination",
		"HATEOASLinks",
		"UpdateRequest",
		"FilterCondition",
		"QueryRequest",
		"QueryResponse",
	}

	for _, schemaName := range expectedSchemas {
		if _, ok := schemas[schemaName]; !ok {
			t.Errorf("Expected schema '%s' not found", schemaName)
		}
	}
}

func TestOpenAPIHandler_Spec_ErrorResponseSchema(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	components := spec["components"].(map[string]interface{})
	schemas := components["schemas"].(map[string]interface{})
	errorSchema, ok := schemas["ErrorResponse"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'ErrorResponse' schema")
	}

	// Verify required fields
	required, ok := errorSchema["required"].([]interface{})
	if !ok {
		t.Fatal("Expected 'required' array in ErrorResponse")
	}

	requiredFields := make(map[string]bool)
	for _, field := range required {
		if fieldName, ok := field.(string); ok {
			requiredFields[fieldName] = true
		}
	}

	expectedRequired := []string{"error", "message", "code", "request_id"}
	for _, field := range expectedRequired {
		if !requiredFields[field] {
			t.Errorf("Expected '%s' to be required in ErrorResponse", field)
		}
	}
}

func TestOpenAPIHandler_Spec_ResponseFormats(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	paths := spec["paths"].(map[string]interface{})
	apiPath := paths["/api/{table}"].(map[string]interface{})
	getOp := apiPath["get"].(map[string]interface{})
	responses := getOp["responses"].(map[string]interface{})
	okResponse := responses["200"].(map[string]interface{})
	content := okResponse["content"].(map[string]interface{})

	// Verify all supported formats are documented
	expectedFormats := []string{
		"application/json",
		"text/csv",
		"application/parquet",
		"application/vnd.apache.arrow.stream",
	}

	for _, format := range expectedFormats {
		if _, ok := content[format]; !ok {
			t.Errorf("Expected format '%s' in response content", format)
		}
	}
}

func TestOpenAPIHandler_Spec_Security(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	paths := spec["paths"].(map[string]interface{})

	// Verify CRUD operations require API key auth
	apiPath := paths["/api/{table}"].(map[string]interface{})
	for _, method := range []string{"get", "post", "put", "delete"} {
		op := apiPath[method].(map[string]interface{})
		security, ok := op["security"].([]interface{})
		if !ok {
			t.Errorf("Expected 'security' in %s /api/{table}", method)
			continue
		}
		if len(security) == 0 {
			t.Errorf("Expected at least one security requirement in %s /api/{table}", method)
		}
	}
}

func TestOpenAPIHandler_Spec_FilterOperators(t *testing.T) {
	handler := NewOpenAPIHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	var spec map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&spec); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	components := spec["components"].(map[string]interface{})
	schemas := components["schemas"].(map[string]interface{})
	filterCondition := schemas["FilterCondition"].(map[string]interface{})
	properties := filterCondition["properties"].(map[string]interface{})
	opProp := properties["op"].(map[string]interface{})
	enumValues, ok := opProp["enum"].([]interface{})
	if !ok {
		t.Fatal("Expected 'enum' in op property")
	}

	// Verify all operators are documented
	expectedOps := map[string]bool{
		"eq":   false,
		"ne":   false,
		"gt":   false,
		"gte":  false,
		"lt":   false,
		"lte":  false,
		"like": false,
		"in":   false,
	}

	for _, val := range enumValues {
		if op, ok := val.(string); ok {
			expectedOps[op] = true
		}
	}

	for op, found := range expectedOps {
		if !found {
			t.Errorf("Expected operator '%s' in FilterCondition enum", op)
		}
	}
}

// Benchmark the OpenAPI handler
func BenchmarkOpenAPIHandler_ServeHTTP(b *testing.B) {
	handler := NewOpenAPIHandler()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}
}
