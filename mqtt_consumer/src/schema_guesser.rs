use std::collections::BTreeMap;
use tokio_postgres::types::Type;

enum TypeMappings {
    String,
    Float,
    Boolean,
    Json,
}

pub fn guess_type(value: &serde_json::Value) -> Type {
    match value {
        serde_json::Value::String(_) => Type::TEXT,
        serde_json::Value::Number(_) => Type::FLOAT8,
        serde_json::Value::Bool(_) => Type::BOOL,
        serde_json::Value::Array(_) => Type::JSON,
        serde_json::Value::Object(_) => Type::JSON,
        serde_json::Value::Null => Type::JSON,
    }
}

pub fn generate_schema_sql(table_name: &String, input: &serde_json::Value) -> Vec<String> {
    if !input.is_object() {
        todo!("handle non-object input")
    }

    // loop over each value in the object and get the type
    let mut schema = BTreeMap::new();
    for (key, value) in input.as_object().unwrap().iter() {
        schema.insert(key.to_string(), guess_type(value));
    }

    // now generate the sql
    let mut sql = String::new();
    for (key, value) in schema.iter() {
        let type_mapping = match value {
            &Type::TEXT => "TEXT",
            &Type::FLOAT8 => "FLOAT",
            &Type::BOOL => "BOOLEAN",
            &Type::JSON => "JSON",
            _ => todo!("handle other types"),
        };

        sql.push_str(&format!("    {key} {type_mapping}, \n"));
    }
    let _ = sql.pop();
    let _ = sql.pop();
    let _ = sql.pop();

    let stmts = vec![
        format!(
            r"CREATE TABLE IF NOT EXISTS {table_name} (
    timestamp TIMESTAMP NOT NULL,
{sql});"
        ),
        format!(
            r"CREATE INDEX IF NOT EXISTS {table_name}_timestamp_idx ON {table_name} (timestamp);"
        ),
    ];

    stmts
}

#[cfg(test)]
mod tests {
    use crate::schema_guesser::generate_schema_sql;

    #[test]
    fn test_basic_schema() {
        let json_obj: serde_json::Value = serde_json::from_str(r#"{"foo": 1, "bar": 2}"#).unwrap();
        let sql = generate_schema_sql(&"test_table".to_string(), &json_obj);

        let expected = r#"CREATE TABLE IF NOT EXISTS test_table (
    timestamp TIMESTAMP NOT NULL,
    bar FLOAT, 
    foo FLOAT, 
);
CREATE INDEX ix_test_table_time ON test_table (timestamp);"#;
        assert_eq!(sql, expected);
    }
}
