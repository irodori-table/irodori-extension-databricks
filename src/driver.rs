use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use reqwest::Client;
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, DatabricksConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct DatabricksConnection {
    client: Client,
    config: DatabricksConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DatabricksConfig {
    base_url: String,
    warehouse_id: String,
    token: String,
    catalog: Option<String>,
    schema: Option<String>,
    redaction_values: Vec<String>,
}

#[derive(Default)]
struct ObjectMeta {
    columns: Vec<Value>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, DatabricksConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match DatabricksConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let connection = DatabricksConnection {
        client: Client::new(),
        config,
    };
    let version = match runtime().and_then(|runtime| runtime.block_on(load_version(&connection))) {
        Ok(version) => version,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };

    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "endpoint".to_string(),
            Value::String(connection.config.base_url.clone()),
        ),
        (
            "warehouseId".to_string(),
            Value::String(connection.config.warehouse_id.clone()),
        ),
        ("serverVersion".to_string(), Value::String(version)),
    ]);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime()
        .and_then(|runtime| runtime.block_on(execute_sql(&connection, sql, abi::max_rows(request))))
    {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

fn connection(connection_id: &str) -> Result<DatabricksConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

impl DatabricksConfig {
    fn from_request(request: &Value) -> Result<Self, String> {
        let connection_string = option_string(request, &["connectionString", "url", "dsn"]);
        let jdbc_params = connection_string
            .as_deref()
            .map(parse_jdbc_params)
            .unwrap_or_default();
        let base_url = connection_string
            .as_deref()
            .and_then(base_url_from_connection_string)
            .or_else(|| option_string(request, &["host", "serverHostname", "workspaceUrl"]))
            .map(|value| normalize_base_url(&value))
            .ok_or_else(|| "Databricks connect requires host, workspaceUrl, or url.".to_string())?;
        let http_path = option_string(request, &["httpPath", "http_path"])
            .or_else(|| jdbc_param(&jdbc_params, &["httpPath", "http_path"]));
        let warehouse_id = option_string(
            request,
            &["warehouseId", "warehouse_id", "sqlWarehouseId", "warehouse"],
        )
        .or_else(|| http_path.as_deref().and_then(warehouse_id_from_http_path))
        .ok_or_else(|| {
            "Databricks connect requires warehouseId or an httpPath containing /warehouses/<id>."
                .to_string()
        })?;
        let token = option_string(
            request,
            &["token", "accessToken", "bearerToken", "pat", "password"],
        )
        .or_else(|| jdbc_param(&jdbc_params, &["PWD", "password", "Token", "token"]))
        .ok_or_else(|| "Databricks connect requires a bearer token.".to_string())?;
        let catalog = option_string(request, &["catalog"])
            .or_else(|| jdbc_param(&jdbc_params, &["ConnCatalog", "catalog"]));
        let schema = option_string(request, &["schema", "database", "db"])
            .or_else(|| jdbc_param(&jdbc_params, &["schema", "database"]));
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, Some(&token));
        collect_url_auth(&base_url, &mut redaction_values);
        if let Some(connection_string) = connection_string.as_deref() {
            collect_jdbc_secrets(connection_string, &mut redaction_values);
        }
        Ok(Self {
            base_url,
            warehouse_id,
            token,
            catalog,
            schema,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values.iter().fold(
            message.replace(&self.base_url, "<databricks-url>"),
            |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            },
        )
    }
}

async fn load_version(connection: &DatabricksConnection) -> Result<String, String> {
    let (_, rows, _) = execute_sql(connection, "select version()", 1).await?;
    Ok(rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_str)
        .map(|version| format!("Databricks SQL {version}"))
        .unwrap_or_else(|| "Databricks SQL".to_string()))
}

async fn execute_sql(
    connection: &DatabricksConnection,
    sql: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let value = execute_statement(connection, sql).await?;
    statement_response_to_output(connection, value, cap).await
}

async fn execute_statement(connection: &DatabricksConnection, sql: &str) -> Result<Value, String> {
    let mut body = json!({
        "statement": sql,
        "warehouse_id": connection.config.warehouse_id,
        "wait_timeout": "30s",
        "on_wait_timeout": "CONTINUE",
        "disposition": "INLINE",
        "format": "JSON_ARRAY"
    });
    if let Some(catalog) = connection.config.catalog.as_deref() {
        body["catalog"] = Value::String(catalog.to_string());
    }
    if let Some(schema) = connection.config.schema.as_deref() {
        body["schema"] = Value::String(schema.to_string());
    }
    let mut value = request_json(connection, "POST", "/api/2.0/sql/statements", Some(body)).await?;

    for _ in 0..60 {
        match statement_state(&value).as_deref() {
            Some("SUCCEEDED") => return Ok(value),
            Some("FAILED") | Some("CANCELED") | Some("CLOSED") => {
                return Err(statement_error(&value));
            }
            Some("PENDING") | Some("RUNNING") | Some("QUEUED") => {}
            _ if value.get("result").is_some() => return Ok(value),
            _ => {}
        }
        let statement_id = value
            .get("statement_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!("Databricks statement response missing statement_id: {value}")
            })?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        value = request_json(
            connection,
            "GET",
            &format!("/api/2.0/sql/statements/{statement_id}"),
            None,
        )
        .await?;
    }
    Err("Databricks statement did not finish before polling timeout.".to_string())
}

async fn statement_response_to_output(
    connection: &DatabricksConnection,
    mut value: Value,
    cap: usize,
) -> Result<QueryOutput, String> {
    let columns = value
        .pointer("/manifest/schema/columns")
        .and_then(Value::as_array)
        .map(|columns| {
            columns
                .iter()
                .map(|column| {
                    column
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("value")
                        .to_string()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut rows = Vec::new();
    let mut truncated = value
        .pointer("/manifest/truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    loop {
        if let Some(data) = value
            .pointer("/result/data_array")
            .and_then(Value::as_array)
        {
            for row in data {
                if rows.len() >= cap {
                    truncated = true;
                    break;
                }
                rows.push(row.as_array().cloned().unwrap_or_else(|| vec![row.clone()]));
            }
        }
        if rows.len() >= cap {
            break;
        }
        let Some(next_link) = value
            .pointer("/result/next_chunk_internal_link")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            break;
        };
        value = request_json(connection, "GET", &next_link, None).await?;
    }
    Ok((columns, rows, truncated))
}

async fn load_metadata(connection: &DatabricksConnection) -> Result<Value, String> {
    let candidates = [
        "select table_catalog, table_schema, table_name, column_name, data_type, ordinal_position, is_nullable \
         from information_schema.columns \
         where table_schema <> 'information_schema' \
         order by table_catalog, table_schema, table_name, ordinal_position \
         limit 10000",
        "select table_catalog, table_schema, table_name, column_name, data_type, ordinal_position, is_nullable \
         from system.information_schema.columns \
         where table_schema <> 'information_schema' \
         order by table_catalog, table_schema, table_name, ordinal_position \
         limit 10000",
    ];
    let mut last_error = None;
    for sql in candidates {
        match execute_sql(connection, sql, 10_000).await {
            Ok((columns, rows, _)) => return Ok(metadata_from_rows(&columns, &rows)),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or_else(|| "metadata query failed".to_string()))
}

fn metadata_from_rows(columns: &[String], rows: &[Vec<Value>]) -> Value {
    let mut schemas: BTreeMap<String, BTreeMap<String, ObjectMeta>> = BTreeMap::new();
    for row in rows {
        let catalog =
            field(columns, row, "table_catalog").unwrap_or_else(|| "hive_metastore".into());
        let schema = field(columns, row, "table_schema").unwrap_or_else(|| "default".into());
        let table = field(columns, row, "table_name").unwrap_or_default();
        if table.is_empty() {
            continue;
        }
        let schema_name = format!("{catalog}.{schema}");
        let object = schemas
            .entry(schema_name)
            .or_default()
            .entry(table)
            .or_default();
        let ordinal = field(columns, row, "ordinal_position")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or((object.columns.len() + 1) as i64);
        let nullable = field(columns, row, "is_nullable")
            .map(|value| value.eq_ignore_ascii_case("YES") || value.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        object.columns.push(json!({
            "name": field(columns, row, "column_name").unwrap_or_default(),
            "dataType": field(columns, row, "data_type").unwrap_or_default(),
            "nullable": nullable,
            "ordinal": ordinal
        }));
    }
    json!({
        "schemas": schemas
            .into_iter()
            .map(|(schema, objects)| json!({
                "name": schema,
                "objects": objects
                    .into_iter()
                    .map(|(name, object)| json!({
                        "schema": schema,
                        "name": name,
                        "kind": "table",
                        "columns": object.columns,
                        "indexes": [],
                        "primaryKey": [],
                        "foreignKeys": []
                    }))
                    .collect::<Vec<_>>()
            }))
            .collect::<Vec<_>>()
    })
}

async fn request_json(
    connection: &DatabricksConnection,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> Result<Value, String> {
    let url = if path.starts_with("http://") || path.starts_with("https://") {
        path.to_string()
    } else {
        format!("{}{}", connection.config.base_url, path)
    };
    let builder = match method {
        "POST" => connection.client.post(url),
        "GET" => connection.client.get(url),
        _ => return Err(format!("unsupported HTTP method: {method}")),
    }
    .bearer_auth(&connection.config.token);
    let builder = if let Some(body) = body {
        builder.json(&body)
    } else {
        builder
    };
    let response = builder
        .send()
        .await
        .map_err(|err| format!("Databricks request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Databricks response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("Databricks returned HTTP {status}: {text}"));
    }
    serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("Databricks JSON response parse failed: {err}: {text}"))
}

fn statement_state(value: &Value) -> Option<String> {
    value
        .pointer("/status/state")
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn statement_error(value: &Value) -> String {
    value
        .pointer("/status/error/message")
        .or_else(|| value.pointer("/status/error"))
        .map(Value::to_string)
        .unwrap_or_else(|| format!("Databricks statement failed: {value}"))
}

fn field(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))
        .and_then(|index| row.get(index))
        .and_then(|value| match value {
            Value::Null => None,
            Value::String(value) => Some(value.clone()),
            other => Some(other.to_string()),
        })
}

fn normalize_base_url(input: &str) -> String {
    let value = input
        .trim()
        .trim_end_matches('/')
        .trim_end_matches("/default")
        .to_string();
    if value.starts_with("http://") || value.starts_with("https://") {
        value
    } else {
        format!("https://{value}")
    }
}

fn base_url_from_connection_string(input: &str) -> Option<String> {
    let stripped = input
        .strip_prefix("jdbc:databricks://")
        .or_else(|| input.strip_prefix("databricks://"))?;
    let host = stripped
        .split(';')
        .next()
        .unwrap_or(stripped)
        .split('/')
        .next()
        .unwrap_or(stripped)
        .trim_end_matches(":443");
    if host.is_empty() {
        None
    } else {
        Some(format!("https://{host}"))
    }
}

fn warehouse_id_from_http_path(path: &str) -> Option<String> {
    let id = path
        .split("/warehouses/")
        .nth(1)?
        .split(['/', ';', '?'])
        .next()
        .unwrap_or_default()
        .trim();
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

fn parse_jdbc_params(input: &str) -> HashMap<String, String> {
    input
        .split(';')
        .skip(1)
        .filter_map(|part| part.split_once('='))
        .map(|(key, value)| (key.trim().to_ascii_lowercase(), value.trim().to_string()))
        .filter(|(_, value)| !value.is_empty())
        .collect()
}

fn jdbc_param(params: &HashMap<String, String>, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| params.get(&field.to_ascii_lowercase()).cloned())
}

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .map(|value| match value {
                        Value::String(value) => value.clone(),
                        Value::Number(value) => value.to_string(),
                        Value::Bool(value) => value.to_string(),
                        _ => String::new(),
                    })
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }
}

fn collect_url_auth(url: &str, values: &mut Vec<String>) {
    let Some(after_scheme) = url.split_once("://").map(|(_, rest)| rest) else {
        return;
    };
    let Some(auth) = after_scheme
        .split('/')
        .next()
        .and_then(|host| host.split('@').next())
    else {
        return;
    };
    if auth.contains(':') {
        for part in auth.split(':') {
            push_sensitive(values, Some(part));
        }
    }
}

fn collect_jdbc_secrets(input: &str, values: &mut Vec<String>) {
    let params = parse_jdbc_params(input);
    for key in ["pwd", "password", "token"] {
        push_sensitive(values, params.get(key).map(String::as_str));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_jdbc_connection_string() {
        let request = json!({
            "url": "jdbc:databricks://dbc.example.cloud.databricks.com:443/default;httpPath=/sql/1.0/warehouses/abc123;PWD=secret",
        });
        let config = DatabricksConfig::from_request(&request).unwrap();
        assert_eq!(config.base_url, "https://dbc.example.cloud.databricks.com");
        assert_eq!(config.warehouse_id, "abc123");
        assert_eq!(config.token, "secret");
    }

    #[test]
    fn extracts_warehouse_id_from_http_path() {
        assert_eq!(
            warehouse_id_from_http_path("/sql/1.0/warehouses/abc123"),
            Some("abc123".to_string())
        );
    }
}
