# Real Estate Analytics API Documentation

## Base URL

All endpoints are relative to the base URL: `http://your-api-domain.com`

Replace `your-api-domain.com` with the actual domain where your API is hosted.

## Endpoints

### 1. Historic Sold Median Price

Retrieves the historic median sold price data for a specific geographic area and property type.

**Endpoint:** `/metrics/historic_sold/price`

**Method:** GET

**Query Parameters:**
- `geog_id` (required): Geographic ID to filter by
- `property_type` (optional): Property type to filter by. Use 'ALL' or leave empty for all property types combined.
- `return_geojson` (optional): Set to 'true' to return data in GeoJSON format. Default is 'false'.


**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/historic_sold/price?geog_id=g20_dpz9hct3&property_type=ALL"
```

**Response Example:**
```json
{
  "geog_id": "g20_dpz9hct3",
  "property_type": "ALL",
  "data": [
    {
      "month": "2019-08",
      "value": 600000
    },
    {
      "month": "2019-09",
      "value": 706500
    },
    {
      "month": "2019-10",
      "value": 678500
    },
    {
      "month": "2024-06",
      "value": 935000
    },
    {
      "month": "2024-07",
      "value": 943500
    }
  ]
}
```

**Response Structure:**

- `geog_id` (string): The geographic ID used for filtering the data.
- `property_type` (string): The property type used for filtering the data.
- `data` (array of objects): A list of date-value pairs representing the median sold price for each time period.
  - `month` (string): The date for the data point, formatted as `YYYY-MM`.
  - `value` (number): The median sold price for that specific month.


**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/historic_sold/price?geog_id=g20_dpz9hct3&property_type=ALL&return_geojson=true"
```

**Response Example (GeoJSON format):**
```json
{
  "type": "Feature",
  "geometry": null,
  "properties": {
    "name": "Scarborough",
    "geog_id": "g20_dpz9hct3",
    "property_type": "ALL",
    "level": 20,
    "parent_geog_id": "g30_dpz89rm7",
    "has_children": true,
    "metric_type": "median_price",
    "metric_data": [
      {
        "month": "2019-08",
        "value": 600000
      },
      {
        "month": "2019-09",
        "value": 706500
      },
      // ... more data points ...
    ]
  }
}
```

**Response Structure:**
- `type` (string): Always "Feature" for GeoJSON format.
- `geometry` (null): Geometry is not included in this response.
- `properties` (object): Contains the geographic properties and metric data.
  - `name` (string): The name of the geographic area.
  - `geog_id` (string): The geographic ID used for filtering the data.
  - `property_type`: The property type for which the data is reported.
  - `level` (integer): The geographic level.
  - `parent_geog_id` (string): The ID of the parent geographic area, if any.
  - `has_children` (boolean): Indicates whether this geographic area has child areas.
  - `metric_type` (string): The type of metric, in this case "median_price".
  - `metric_data` (array of objects): The time series data for the metric.
    - `month` (string): The month for the data point, formatted as "YYYY-MM".
    - `value` (number): The median price value for that month.

### 2. Historic Sold Median Days on Market (DOM)

Retrieves the historic median days on market data for a specific geographic area and property type.

**Endpoint:** `/metrics/historic_sold/dom`

**Method:** GET

**Query Parameters:**
- `geog_id` (required): Geographic ID to filter by
- `property_type` (optional): Property type to filter by. Use 'ALL' or leave empty for all property types combined.
- `return_geojson` (optional): Set to 'true' to return data in GeoJSON format. Default is 'false'.


**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/historic_sold/dom?geog_id=g20_dpz9hct3&property_type=ALL"
```

**Response Example:**
```json
{
  "geog_id": "g20_dpz9hct3",
  "property_type": "ALL",
  "data": [
    {
      "month": "2019-08",
      "value": 14
    },
    {
      "month": "2019-09",
      "value": 13
    },
    {
      "month": "2024-06",
      "value": 11
    },
    {
      "month": "2024-07",
      "value": 17
    }
  ]
}
```

**Response Structure:**

- `geog_id` (string): The geographic ID used for filtering the data.
- `property_type` (string): The property type used for filtering the data.
- `data` (array of objects): A list of date-value pairs representing the median days on market (DOM) for each time period.
  - `month` (string): The date for the data point, formatted as `YYYY-MM`.
  - `value` (number): The median days on market for that specific month.

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/historic_sold/dom?geog_id=g20_dpz9hct3&property_type=ALL&return_geojson=true"
```

**Response Example (GeoJSON format):**
```json
{
  "type": "Feature",
  "geometry": null,
  "properties": {
    "name": "Scarborough",
    "geog_id": "g20_dpz9hct3",
    "property_type": "ALL",
    "level": 20,
    "parent_geog_id": "g30_dpz89rm7",
    "has_children": true,
    "metric_type": "median_dom",
    "metric_data": [
      {
        "month": "2019-08",
        "value": 14
      },
      {
        "month": "2019-09",
        "value": 13
      },
      // ... more data points ...
    ]
  }
}
```

**Response Structure:** (Same as the price endpoint, with "metric_type" being "median_dom")


### 3. Historic Sold Over Ask Percentage

Retrieves the historic percentage of properties sold over asking price for a specific geographic area and property type.

**Endpoint:** `/metrics/historic_sold/over-ask`

**Method:** GET

**Query Parameters:**
- `geog_id` (required): Geographic ID to filter by
- `property_type` (optional): Property type to filter by. Use 'ALL' or leave empty for all property types combined.
- `return_geojson` (optional): Set to 'true' to return data in GeoJSON format. Default is 'false'.

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/historic_sold/over-ask?geog_id=g20_dpz9hct3&property_type=DETACHED"
```

**Response Example:**
```json
{
  "geog_id": "g20_dpz9hct3",
  "property_type": "DETACHED",
  "data": [
    {
      "month": "2019-08",
      "value": 46.15384615384615
    },
    {
      "month": "2019-09",
      "value": 37.77777777777778
    },
    {
      "month": "2024-06",
      "value": 62.23404255319149
    },
    {
      "month": "2024-07",
      "value": 41.7989417989418
    }
  ]
}
```

**Response Structure:**

- `geog_id` (string): The geographic ID used for filtering the data.
- `property_type` (string): The property type used for filtering the data.
- `data` (array of objects): A list of date-value pairs representing the percentage of properties sold over the asking price for each time period.
  - `month` (string): The date for the data point, formatted as `YYYY-MM`.
  - `value` (number): The percentage of properties sold over the asking price for that specific month.

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/historic_sold/over-ask?geog_id=g20_dpz9hct3&property_type=DETACHED&return_geojson=true"
```

**Response Example (GeoJSON format):**
```json
{
  "type": "Feature",
  "geometry": null,
  "properties": {
    "name": "Scarborough",
    "geog_id": "g20_dpz9hct3",
    "property_type": "DETACHED",
    "level": 20,
    "parent_geog_id": "g30_dpz89rm7",
    "has_children": true,
    "metric_type": "over_ask_percentage",
    "metric_data": [
      {
        "month": "2019-08",
        "value": 46.15
      },
      {
        "month": "2019-09",
        "value": 37.78
      },
      // ... more data points ...
    ]
  }
}
```

**Response Structure:** (Same as the price endpoint, with "metric_type" being "over_ask_percentage")


### 4. Historic Sold Under Ask Percentage

Retrieves the historic percentage of properties sold under asking price for a specific geographic area and property type.

**Endpoint:** `/metrics/historic_sold/under-ask`

**Method:** GET

**Query Parameters:**
- `geog_id` (required): Geographic ID to filter by
- `property_type` (optional): Property type to filter by. Use 'ALL' or leave empty for all property types combined.
- `return_geojson` (optional): Set to 'true' to return data in GeoJSON format. Default is 'false'.

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/historic_sold/under-ask?geog_id=g20_dpz9hct3&property_type=CONDO"
```

**Response Example:**
```json
{
  "geog_id": "g20_dpz9hct3",
  "property_type": "CONDO",
  "data": [
    {
      "month": "2019-08",
      "value": 70.83333333333334
    },
    {
      "month": "2019-09",
      "value": 53.70370370370371
    },
    {
      "month": "2024-06",
      "value": 77
    },
    {
      "month": "2024-07",
      "value": 67.07317073170732
    }
  ]
}
```

**Response Structure:**

- `geog_id` (string): The geographic ID used for filtering the data.
- `property_type` (string): The property type used for filtering the data.
- `data` (array of objects): A list of date-value pairs representing the percentage of properties sold under the asking price for each time period.
  - `month` (string): The date for the data point, formatted as `YYYY-MM`.
  - `value` (number): The percentage of properties sold under the asking price for that specific month.

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/historic_sold/under-ask?geog_id=g20_dpz9hct3&property_type=CONDO&return_geojson=true"
```

**Response Example (GeoJSON format):**
```json
{
  "type": "Feature",
  "geometry": null,
  "properties": {
    "name": "Scarborough",
    "geog_id": "g20_dpz9hct3",
    "property_type": "CONDO",
    "level": 20,
    "parent_geog_id": "g30_dpz89rm7",
    "has_children": true,
    "metric_type": "under_ask_percentage",
    "metric_data": [
      {
        "month": "2019-08",
        "value": 53.85
      },
      {
        "month": "2019-09",
        "value": 62.22
      },
      // ... more data points ...
    ]
  }
}
```

**Response Structure:** (Same as the price endpoint, with "metric_type" being "under_ask_percentage")


### 5. Last Month Metrics

Retrieves various metrics for the last month for a specific geographic area and property type.

**Endpoint:** `/metrics/last-month`

**Method:** GET

**Query Parameters:**
- `geog_id` (required): Geographic ID to filter by
- `property_type` (optional): Property type to filter by. Use 'ALL' or leave empty for all property types combined.
- `return_geojson` (optional): Set to 'true' to return data in GeoJSON format. Default is 'false'.


**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/last-month?geog_id=g20_dpz9hct3&property_type=DETACHED"
```

**Response Example:**
```json
{
  "month": "202407",
  "geog_id": "g20_dpz9hct3",
  "property_type": "DETACHED",
  "median_price": 1187000,
  "new_listings_count": 288
}
```

**Response Structure:**

- `month` (string): The year and month for which the metrics are calculated, formatted as `YYYYMM`.
- `geog_id` (string): The geographic ID used for filtering the data.
- `property_type` (string): The property type used for filtering the data.
- `median_price` (number): The median sold price for properties in that geographic area and property type during the specified month.
- `new_listings_count` (number): The number of new listings in that geographic area and property type during the specified month.

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/last-month?geog_id=g20_dpz9hct3&property_type=ALL&return_geojson=true"
```

**Response Example (GeoJSON format):**
```json
{
  "type": "Feature",
  "geometry": null,
  "properties": {
    "name": "Scarborough",
    "geog_id": "g20_dpz9hct3",
    "property_type": "ALL",
    "level": 20,
    "parent_geog_id": "g30_dpz89rm7",
    "has_children": true,
    "metric_type": "last_month_metrics",
    "metric_data": {
      "month": "202407",
      "median_price": 1187000,
      "new_listings_count": 288
    }
  }
}
```

**Response Structure:**
- `type` (string): Always "Feature" for GeoJSON format.
- `geometry` (null): Geometry is not included in this response.
- `properties` (object): Contains the geographic properties and metric data.
  - `name` (string): The name of the geographic area.
  - `geog_id` (string): The geographic ID used for filtering the data.
  - `property_type` (string): The property type for which the monthly metrics are calculated.
  - `level` (integer): The geographic level.
  - `parent_geog_id` (string): The ID of the parent geographic area, if any.
  - `has_children` (boolean): Indicates whether this geographic area has child areas.
  - `metric_type` (string): The type of metric, in this case "last_month_metrics".
  - `metric_data` (object): The last month's metrics data.
    - `month` (string): The month for which the data is reported, formatted as "YYYYMM".
    - `median_price` (number): The median sold price for the last month.
    - `new_listings_count` (integer): The number of new listings in the last month.


### 6. Absorption Rate

Retrieves the absorption rate for a specific geographic area and property type.

**Endpoint:** `/metrics/absorption-rate`

**Method:** GET

**Query Parameters:**
- `geog_id` (required): Geographic ID to filter by
- `property_type` (optional): Property type to filter by. Use 'ALL' or leave empty for all property types combined.
- `return_geojson` (optional): Set to 'true' to return data in GeoJSON format. Default is 'false'.


**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/absorption-rate?geog_id=g20_dpz9hct3&property_type=ALL"
```

**Response Example:**
```json
{
  "month": "202408",
  "geog_id": "g20_dpz9hct3",
  "property_type": "ALL",
  "sold_count": 373,
  "current_count": 718,
  "absorption_rate": 0.5194986072423399
}
```

**Response Structure:**

- `month` (string): The year and month for which the absorption rate is calculated, formatted as `YYYYMM`.
- `geog_id` (string): The geographic ID used for filtering the data.
- `property_type` (string): The property type used for filtering the data.
- `sold_count` (number): The number of properties sold during the specified month.
- `current_count` (number): The number of properties currently on the market during the specified month.
- `absorption_rate` (number): The absorption rate, calculated as the ratio of `sold_count` to `current_count`.

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/metrics/absorption-rate?geog_id=g20_dpz9hct3&property_type=ALL&return_geojson=true"
```

**Response Example (GeoJSON format):**
```json
{
  "type": "Feature",
  "geometry": null,
  "properties": {
    "name": "Scarborough",
    "geog_id": "g20_dpz9hct3",
    "property_type": "ALL",
    "level": 20,
    "parent_geog_id": "g30_dpz89rm7",
    "has_children": true,    
    "metric_type": "absorption_rate",
    "metric_data": {
      "month": "202408",
      "sold_count": 376,
      "current_count": 695,
      "absorption_rate": 0.5410071942446043
    }
  }
}
```

**Response Structure:**
- `type` (string): Always "Feature" for GeoJSON format.
- `geometry` (null): Geometry is not included in this response.
- `properties` (object): Contains the geographic properties and metric data.
  - `name` (string): The name of the geographic area.
  - `geog_id` (string): The geographic ID used for filtering the data.
  - `property_type` (string): The property type for which the absorption rate is calculated.
  - `level` (integer): The geographic level.
  - `parent_geog_id` (string): The ID of the parent geographic area, if any.
  - `has_children` (boolean): Indicates whether this geographic area has child areas.
  - `metric_type` (string): The type of metric, in this case "absorption_rate".
  - `metric_data` (object): The absorption rate data.
    - `month` (string): The month for which the data is reported, formatted as "YYYYMM".
    - `sold_count` (integer): The number of properties sold during the specified month.
    - `current_count` (integer): The number of properties currently on the market.
    - `absorption_rate` (number): The absorption rate, calculated as the ratio of sold_count to current_count.


### 7. Get Geographic Entities

Retrieves a list of geographic entities, optionally filtered by level and/or parent.

**Endpoint:** `/geos`

**Method:** GET

**Query Parameters:**
- `level` (optional): Filter by geographic level (e.g., 10, 20, 30, 35, 40)
- `parent_id` (optional): Filter by parent geographic ID
- `geometry` (optional): Include simplified geometry data if set to `true`

**Example CURL requests:**
```bash
curl -X GET "http://your-api-domain.com/geos"
curl -X GET "http://your-api-domain.com/geos?level=30"
curl -X GET "http://your-api-domain.com/geos?parent_id=g20_dpz9hct3"
curl -X GET "http://your-api-domain.com/geos?parent_id=g20_dpz9hct3&level=30&geometry=true"
```

**Response Example:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [-79.13454, 43.80196],
            [-79.13535, 43.801],
            [-79.13454, 43.80196]
          ]
        ]
      },
      "properties": {
        "name": "West Rouge",
        "geog_id": "g10_dpz9p7cv",
        "level": 10,
        "parent_geog_id": "g20_dpz9hct3",
        "has_children": false
      }
    },
    {
      "type": "Feature",
      "geometry": null,
      "properties": {
        "name": "Centennial Scarborough",
        "geog_id": "g10_dpz8sdmg",
        "level": 10,
        "parent_geog_id": "g20_dpz9hct3",
        "has_children": false
      }
    }
  ]
}
```

**Response Structure:**
- `type`: Always "FeatureCollection" for this endpoint
- `features`: An array of GeoJSON Feature objects, each representing a geographic entity
  - `type`: Always "Feature" for each entity
  - `geometry`: The simplified geometry of the entity (if requested), or `null` if not included
  - `properties`: Contains the attributes of the geographic entity
    - `name`: The name of the geographic entity
    - `geog_id`: The unique identifier for the geographic entity
    - `level`: The hierarchical level of the geographic entity
    - `parent_geog_id`: The ID of the parent geographic entity, if applicable
    - `has_children`: Indicates whether this geographic entity has child entities


### 8. Search Geographic Entities

Searches for geographic entities by name, optionally filtered by level.

**Endpoint:** `/geos/search`

**Method:** GET

**Query Parameters:**
- `query` (required): Search query string
- `level` (optional): Filter by geographic level (e.g., 10, 20, 30, 35, 40)
- `geometry` (optional): Include simplified geometry data if set to `true`

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/geos/search?query=Scarborough&level=10&geometry=true"
```

**Response Example:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [-79.23454, 43.76196],
            [-79.23535, 43.761],
            [-79.23454, 43.76196]
          ]
        ]
      },
      "properties": {
        "name": "Scarborough Village",
        "geog_id": "g10_dpz8v6u2",
        "level": 10,
        "parent_geog_id": "g20_dpz9hct3",
        "has_children": false
      }
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [-79.25454, 43.78196],
            [-79.25535, 43.781],
            [-79.25454, 43.78196]
          ]
        ]
      },
      "properties": {
        "name": "Scarborough City Centre",
        "geog_id": "g10_dpz9h2fm",
        "level": 10,
        "parent_geog_id": "g20_dpz9hct3",
        "has_children": false
      }
    }
  ]
}
```

**Response Structure:**
Same as the "Get Geographic Entities" endpoint.

**Notes:**
- Returns an empty list (`[]`) if no matching geographic entities are found.

### 9. Get Specific Geographic Entity

Retrieves details of a specific geographic entity by its ID.

**Endpoint:** `/geos/{geog_id}`

**Method:** GET

**Path Parameters:**
- `geog_id` (required): The ID of the geographic entity to retrieve

**Query Parameters:**
- `geometry` (optional): Include full geometry data if set to `true`

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/geos/g10_dpz9p7cv?geometry=true"
```

**Response Example:**
```json
{
  "type": "Feature",
  "geometry": {
    "type": "Polygon",
    "coordinates": [
      [
        [-79.13454, 43.80196],
        [-79.13535, 43.801],
        [-79.13454, 43.80196]
      ]
    ]
  },
  "properties": {
    "name": "West Rouge",
    "geog_id": "g10_dpz9p7cv",
    "level": 10,
    "parent_geog_id": "g20_dpz9hct3",
    "has_children": false
  }
}
```

**Response Structure:**
- `type`: Always "Feature" for this endpoint
- `geometry`: The full geometry of the entity (if requested), or `null` if not included
- `properties`: Contains the attributes of the geographic entity (same as in previous endpoints)


### 10. Get Geographic Hierarchy

Retrieves the full hierarchy (ancestors) of a geographic entity.

**Endpoint:** `/geos/hierarchy/{geog_id}`

**Method:** GET

**Path Parameters:**
- `geog_id` (required): The ID of the geographic entity for which to retrieve the hierarchy

**Query Parameters:**
- `geometry` (optional): Include simplified geometry data if set to `true`

**Example CURL request:**
```bash
curl -X GET "http://your-api-domain.com/geos/hierarchy/g20_dpz9hct3?geometry=true"
```

**Response Example:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [-79.63454, 43.70196],
            [-79.63535, 43.701],
            [-79.63454, 43.70196]
          ]
        ]
      },
      "properties": {
        "name": "Greater Toronto Area",
        "geog_id": "g40_dpz3tpuh",
        "level": 40,
        "parent_geog_id": null,
        "has_children": true
      }
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [-79.33454, 43.75196],
            [-79.33535, 43.751],
            [-79.33454, 43.75196]
          ]
        ]
      },
      "properties": {
        "name": "Toronto",
        "geog_id": "g30_dpz89rm7",
        "level": 30,
        "parent_geog_id": "g40_dpz3tpuh",
        "has_children": true
      }
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [-79.23454, 43.77196],
            [-79.23535, 43.771],
            [-79.23454, 43.77196]
          ]
        ]
      },
      "properties": {
        "name": "Scarborough",
        "geog_id": "g20_dpz9hct3",
        "level": 20,
        "parent_geog_id": "g30_dpz89rm7",
        "has_children": true
      }
    }
  ]
}
```

**Response Structure:**
Same as the "Get Geographic Entities" endpoint, with features ordered from the top-most ancestor to the specified geographic entity.


## Notes

- All endpoints return GeoJSON-compliant responses.
- The `geometry` field is only included when the `geometry` query parameter is set to `true`.
- For endpoints returning multiple entities (`/geos`, `/geos/search`, `/geos/hierarchy/{geog_id}`), a simplified geometry is provided when requested.
- For the single entity endpoint (`/geos/{geog_id}`), the full geometry is provided when requested.
- If no data is found for the specified parameters, the API will return a 404 error with an appropriate error message (except for `/geos/search` which returns an empty list).
- The `/geos/search` endpoint returns an empty FeatureCollection if no matching entities are found.
- The `property_type` parameter is case-sensitive. Common values include 'DETACHED', 'SEMI-DETACHED', 'CONDO', etc. Use 'ALL' or omit the parameter to get data for all property types combined.
- Historic sold metrics (price, DOM, over-ask, under-ask) provide time series data over a period of time, typically the last 5 years.
- Last month metrics and absorption rate provide the most recent monthly data point.
- Geographic entity endpoints support hierarchical data retrieval and navigation.
