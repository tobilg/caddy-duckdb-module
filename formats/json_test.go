package formats

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestWriteJSON_BasicOutput(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if err := insertTestData(db); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	rows, err := getTestRows(db)
	if err != nil {
		t.Fatalf("Failed to get test rows: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteJSON(rec, rows, 0, 0, 0, false, 0, nil)
	if err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}

	// Verify response
	if rec.Code != 200 {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got '%s'", ct)
	}

	// Parse response
	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON response: %v", err)
	}

	// Check data array exists
	data, ok := result["data"].([]interface{})
	if !ok {
		t.Fatal("Expected 'data' array in response")
	}
	if len(data) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(data))
	}

	// Check first row has expected fields
	firstRow, ok := data[0].(map[string]interface{})
	if !ok {
		t.Fatal("Expected first row to be an object")
	}
	if _, ok := firstRow["id"]; !ok {
		t.Error("Expected 'id' field in row")
	}
	if _, ok := firstRow["name"]; !ok {
		t.Error("Expected 'name' field in row")
	}
}

func TestWriteJSON_WithPagination(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if err := insertTestData(db); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	rows, err := getTestRows(db)
	if err != nil {
		t.Fatalf("Failed to get test rows: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteJSON(rec, rows, 1, 10, 100, true, 0, nil)
	if err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON response: %v", err)
	}

	// Check pagination metadata
	pagination, ok := result["pagination"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected 'pagination' object in response")
	}
	if page := pagination["page"].(float64); page != 1 {
		t.Errorf("Expected page 1, got %v", page)
	}
	if limit := pagination["limit"].(float64); limit != 10 {
		t.Errorf("Expected limit 10, got %v", limit)
	}
	if totalRows := pagination["total_rows"].(float64); totalRows != 100 {
		t.Errorf("Expected total_rows 100, got %v", totalRows)
	}
	if totalPages := pagination["total_pages"].(float64); totalPages != 10 {
		t.Errorf("Expected total_pages 10, got %v", totalPages)
	}
}

func TestWriteJSON_WithHATEOASLinks(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if err := insertTestData(db); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	rows, err := getTestRows(db)
	if err != nil {
		t.Fatalf("Failed to get test rows: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	linksConfig := &LinksConfig{
		Enabled:  true,
		BasePath: "/duckdb/api/users",
		Query:    url.Values{"limit": []string{"10"}},
	}
	err = WriteJSON(rec, rows, 2, 10, 50, true, 0, linksConfig)
	if err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON response: %v", err)
	}

	// Check HATEOAS links
	links, ok := result["_links"].(map[string]interface{})
	if !ok {
		t.Fatal("Expected '_links' object in response")
	}

	// Check required links exist
	requiredLinks := []string{"self", "first", "last", "prev", "next"}
	for _, linkName := range requiredLinks {
		if _, ok := links[linkName]; !ok {
			t.Errorf("Expected '%s' link", linkName)
		}
	}
}

func TestWriteJSON_NullValues(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if err := insertNullData(db); err != nil {
		t.Fatalf("Failed to insert null data: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test_data WHERE id = 4")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteJSON(rec, rows, 0, 0, 0, false, 0, nil)
	if err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON response: %v", err)
	}

	data := result["data"].([]interface{})
	if len(data) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(data))
	}

	row := data[0].(map[string]interface{})
	// Check that NULL values are represented as nil/null
	if row["name"] != nil {
		t.Errorf("Expected null for 'name', got %v", row["name"])
	}
	if row["age"] != nil {
		t.Errorf("Expected null for 'age', got %v", row["age"])
	}
}

func TestWriteJSON_EmptyResult(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	rows, err := getEmptyRows(db)
	if err != nil {
		t.Fatalf("Failed to get empty rows: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	err = WriteJSON(rec, rows, 0, 0, 0, false, 0, nil)
	if err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON response: %v", err)
	}

	data := result["data"].([]interface{})
	if len(data) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(data))
	}
}

func TestWriteJSON_SafetyLimitTruncation(t *testing.T) {
	db, err := createTestDB()
	if err != nil {
		t.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if err := insertTestData(db); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	rows, err := getTestRows(db)
	if err != nil {
		t.Fatalf("Failed to get test rows: %v", err)
	}
	defer rows.Close()

	rec := httptest.NewRecorder()
	// Safety limit of 3 with 10 total rows - should trigger truncation message
	err = WriteJSON(rec, rows, 0, 0, 10, false, 3, nil)
	if err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON response: %v", err)
	}

	// Check truncation fields
	truncated, ok := result["truncated"].(bool)
	if !ok || !truncated {
		t.Error("Expected 'truncated' to be true")
	}
	if _, ok := result["message"]; !ok {
		t.Error("Expected 'message' field for truncated results")
	}
	if _, ok := result["total_available"]; !ok {
		t.Error("Expected 'total_available' field for truncated results")
	}
}

func TestGenerateHATEOASLinks(t *testing.T) {
	tests := []struct {
		name       string
		basePath   string
		query      url.Values
		page       int
		limit      int
		totalPages int
		wantLinks  []string
	}{
		{
			name:       "first page",
			basePath:   "/api/users",
			query:      url.Values{},
			page:       1,
			limit:      10,
			totalPages: 5,
			wantLinks:  []string{"self", "first", "last", "next"},
		},
		{
			name:       "middle page",
			basePath:   "/api/users",
			query:      url.Values{},
			page:       3,
			limit:      10,
			totalPages: 5,
			wantLinks:  []string{"self", "first", "last", "prev", "next"},
		},
		{
			name:       "last page",
			basePath:   "/api/users",
			query:      url.Values{},
			page:       5,
			limit:      10,
			totalPages: 5,
			wantLinks:  []string{"self", "first", "last", "prev"},
		},
		{
			name:       "single page",
			basePath:   "/api/users",
			query:      url.Values{},
			page:       1,
			limit:      10,
			totalPages: 1,
			wantLinks:  []string{"self", "first", "last"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			links := generateHATEOASLinks(tt.basePath, tt.query, tt.page, tt.limit, tt.totalPages)

			for _, linkName := range tt.wantLinks {
				if _, ok := links[linkName]; !ok {
					t.Errorf("Expected '%s' link to be present", linkName)
				}
			}

			// Check no unexpected links
			for linkName := range links {
				found := false
				for _, want := range tt.wantLinks {
					if linkName == want {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Unexpected link '%s' present", linkName)
				}
			}
		})
	}
}

func TestGenerateHATEOASLinks_PreservesQueryParams(t *testing.T) {
	query := url.Values{
		"filter": []string{"status:eq:active"},
		"sort":   []string{"name:asc"},
	}
	links := generateHATEOASLinks("/api/users", query, 2, 10, 5)

	// Check that filter and sort are preserved in links
	selfLink := links["self"]
	if selfLink == "" {
		t.Fatal("Expected 'self' link")
	}

	// Parse the URL to check query params
	parsedURL, err := url.Parse(selfLink)
	if err != nil {
		t.Fatalf("Failed to parse self link: %v", err)
	}

	queryParams := parsedURL.Query()
	if queryParams.Get("filter") != "status:eq:active" {
		t.Error("Expected filter param to be preserved")
	}
	if queryParams.Get("sort") != "name:asc" {
		t.Error("Expected sort param to be preserved")
	}
	if queryParams.Get("page") != "2" {
		t.Error("Expected page param to be set")
	}
	if queryParams.Get("links") != "true" {
		t.Error("Expected links param to be set to true")
	}
}

// Benchmark JSON writing
func BenchmarkWriteJSON(b *testing.B) {
	db, err := createTestDB()
	if err != nil {
		b.Fatalf("Failed to create test DB: %v", err)
	}
	defer db.Close()

	if err := createTestTable(db); err != nil {
		b.Fatalf("Failed to create test table: %v", err)
	}
	if err := insertTestData(db); err != nil {
		b.Fatalf("Failed to insert test data: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := getTestRows(db)
		rec := httptest.NewRecorder()
		WriteJSON(rec, rows, 1, 10, 3, true, 0, nil)
		rows.Close()
	}
}
