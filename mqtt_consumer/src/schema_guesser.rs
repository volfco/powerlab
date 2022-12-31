use std::collections::BTreeMap;

enum TypeMappings {
    String,
    Float,
    Boolean,
    Json,
}

pub fn generate_schema_sql(table_name: &String, input: &serde_json::Value) -> String {
    if !input.is_object() {
        todo!("handle non-object input")
    }

    // loop over each value in the object and get the type
    let mut schema = BTreeMap::new();
    for (key, value) in input.as_object().unwrap().iter() {
        let type_mapping = match value {
            serde_json::Value::String(_) => TypeMappings::String,
            serde_json::Value::Number(_) => TypeMappings::Float,
            serde_json::Value::Bool(_) => TypeMappings::Boolean,
            serde_json::Value::Array(_) => TypeMappings::Json,
            serde_json::Value::Object(_) => TypeMappings::Json,
            serde_json::Value::Null => TypeMappings::Json,
        };

        schema.insert(key.to_string(), type_mapping);
    }

    // now generate the sql
    let mut sql = String::new();
    for (key, value) in schema.iter() {
        let type_mapping = match value {
            TypeMappings::String => "TEXT",
            TypeMappings::Float => "FLOAT",
            TypeMappings::Boolean => "BOOLEAN",
            TypeMappings::Json => "JSON",
        };

        sql.push_str(&format!("    {key} {type_mapping}, \n"));
    }

    let stmt = format!(
        r"CREATE TABLE IF NOT EXISTS {table_name} (
    timestamp TIMESTAMP NOT NULL,
{sql});
CREATE INDEX ix_{table_name}_time ON {table_name} (timestamp);"
    );

    stmt
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
