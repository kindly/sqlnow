use duckdb::Connection;
use eyre::Result;
use libflatterer::{flatten_all, guess_array, Options};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::str;
use tempfile::TempDir;

use csv;

fn read_first_1000_bytes(file_path: &str) -> io::Result<String> {
    let mut file = File::open(file_path)?;
    let mut buffer = vec![0; 10000];
    file.read(&mut buffer)?;

    let mut s = str::from_utf8(&buffer).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    while !s.is_char_boundary(s.len()) {
        s = &s[..s.len() - 1];
    }

    Ok(s.to_string())
}

#[derive(Debug, serde::Deserialize)]
struct Field {
    table_name: String,
    field_name: String,
    field_type: String,
}
struct Table {
    fields: Vec<Field>,
}

fn to_duckdb_type(json_type: &str) -> String {
    match json_type {
        "text" => "TEXT".to_string(),
        "date" => "TIMESTAMP".to_string(),
        "integer" => "BIGINT".to_string(),
        "number" => "NUMERIC".to_string(),
        "boolean" => "BOOLEAN".to_string(),
        _ => "TEXT".to_string(),
    }
}

pub fn load_json(file: &str, name: &str, selected_tables: &Vec<String>, drop: bool, connection: &Connection) -> Result<()> {

    let first_1000_bytes = read_first_1000_bytes(file)?;
    let (file_type, _) = guess_array(&first_1000_bytes)?;
    let stream = if file_type == "stream" {
        true
    } else {
        false
    };

    let path = std::path::PathBuf::from(file);
    let file_stem = path.file_stem().unwrap_or(std::ffi::OsStr::new("")).to_str().unwrap_or("");

    let schema_or_table = if !name.is_empty() {
        name
    } else {
        file_stem
    };

    let options = Options::builder().main_table_name(schema_or_table.to_owned()).json_stream(stream).force(true).build();

    let temp_dir = TempDir::new()?;

    flatten_all(vec![file.to_string()], temp_dir.path().to_string_lossy().to_string(), options)?;

    let mut field_reader = csv::ReaderBuilder::new().from_path(temp_dir.path().join("fields.csv"))?;

    let mut tables: HashMap<String, Table> = std::collections::HashMap::new();

    for row in field_reader.deserialize() {
        let field: Field = row?;
        if selected_tables.len() > 0 && !selected_tables.contains(&field.table_name) {
            continue;
        }

        if tables.contains_key(&field.table_name) {
            tables.get_mut(&field.table_name).unwrap().fields.push(field);
        } else {
            tables.insert(field.table_name.clone(), Table {
                fields: vec![field],
            });
        }
    }

    let single_table = tables.len() == 1;

    if !single_table {
        connection.execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema_or_table}\" "), [])?;
    }

    for (table_name, table) in tables {
        let db_table_name = if single_table {
            format!("\"{schema_or_table}\"")
        } else {
            format!("\"{}\".\"{}\"", schema_or_table, table_name)
        };

        let mut columns = vec![];

        for field in table.fields {
            columns.push(format!("\"{}\" {}", field.field_name, to_duckdb_type(&field.field_type)));
        }

        if drop {
            connection.execute(&format!("DROP TABLE IF EXISTS {};", db_table_name), [])?;
        }

        let create_table = format!("CREATE TABLE {} ({})", db_table_name, columns.join(", "));
        connection.execute(&create_table, [])?;

        let mut csv_path = temp_dir.path().join("csv").join(format!("{}.csv", table_name)).to_string_lossy().to_string();
        csv_path = csv_path.replace("'", "''");

        connection.execute(&format!("COPY {db_table_name} from '{csv_path}' WITH (HEADER true)"), [])?;
    }

    Ok(())
}


