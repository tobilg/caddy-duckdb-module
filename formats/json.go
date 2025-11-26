package formats

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// LinksConfig contains configuration for generating HATEOAS links.
type LinksConfig struct {
	Enabled  bool       // Whether to include _links in response
	BasePath string     // Base path for generating links (e.g., "/duckdb/api/users")
	Query    url.Values // Original query parameters to preserve
}

// WriteJSON writes query results as JSON with pagination.
func WriteJSON(w http.ResponseWriter, rows *sql.Rows, page, limit int, totalRows int64, paginationRequested bool, safetyLimit int, linksConfig *LinksConfig) error {
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Prepare data structure
	data := make([]map[string]interface{}, 0)
	rowCount := 0

	// Scan rows
	for rows.Next() {
		rowCount++

		// Create a slice of interface{} to hold each column
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Create a map for this row
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// Handle NULL values and byte arrays
			switch v := val.(type) {
			case nil:
				rowMap[col] = nil
			case []byte:
				rowMap[col] = string(v)
			default:
				rowMap[col] = v
			}
		}

		data = append(data, rowMap)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	// Build response
	response := map[string]interface{}{
		"data": data,
	}

	// Add pagination metadata if requested
	if paginationRequested && limit > 0 {
		totalPages := 0
		if totalRows > 0 {
			totalPages = int((totalRows + int64(limit) - 1) / int64(limit))
		}

		response["pagination"] = map[string]interface{}{
			"page":        page,
			"limit":       limit,
			"total_rows":  totalRows,
			"total_pages": totalPages,
		}

		// Add HATEOAS links if enabled
		if linksConfig != nil && linksConfig.Enabled {
			links := generateHATEOASLinks(linksConfig.BasePath, linksConfig.Query, page, limit, totalPages)
			response["_links"] = links
		}
	} else if !paginationRequested {
		// No pagination requested - check if results were truncated by safety limit
		truncated := false
		if safetyLimit > 0 && int64(rowCount) >= int64(safetyLimit) && int64(rowCount) < totalRows {
			truncated = true
		}

		if truncated {
			response["truncated"] = true
			response["message"] = fmt.Sprintf("Results limited to %d rows by safety limit. Use pagination (?limit=X&page=Y) to access more data.", safetyLimit)
			response["total_available"] = totalRows
		}
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(response)
}

// generateHATEOASLinks generates navigation links for paginated responses.
func generateHATEOASLinks(basePath string, query url.Values, page, limit, totalPages int) map[string]string {
	links := make(map[string]string)

	// Helper to build URL with page parameter
	buildURL := func(targetPage int) string {
		q := make(url.Values)
		// Copy existing query params except page
		for key, values := range query {
			if key != "page" && key != "links" {
				for _, v := range values {
					q.Add(key, v)
				}
			}
		}
		q.Set("page", fmt.Sprintf("%d", targetPage))
		q.Set("links", "true")
		return fmt.Sprintf("%s?%s", basePath, q.Encode())
	}

	// Self link (current page)
	links["self"] = buildURL(page)

	// First page link
	links["first"] = buildURL(1)

	// Last page link
	if totalPages > 0 {
		links["last"] = buildURL(totalPages)
	}

	// Previous page link (if not on first page)
	if page > 1 {
		links["prev"] = buildURL(page - 1)
	}

	// Next page link (if not on last page)
	if page < totalPages {
		links["next"] = buildURL(page + 1)
	}

	return links
}
