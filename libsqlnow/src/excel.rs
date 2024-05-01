use duckdb::Connection;
use duckdb::appender_params_from_iter;
use calamine::{open_workbook, Reader, Xlsx, Data};
use eyre::Result;

struct Detector {
    db_type: &'static str,
}

impl Detector {
    fn new() -> Self {
        Self {
            db_type: "",
        }
    }

    fn add(&mut self, cell: &Data) {
        if self.db_type == "TEXT" {
            return;
        }
        let db_type = db_type(cell);
        if !self.db_type.is_empty() {
            if self.db_type != db_type {
                self.db_type = "TEXT";
            }
        } else {
            self.db_type = db_type;
        }
    }

    fn detect(&self) -> &str {
        if self.db_type.is_empty() {
            "TEXT"
        } else {
            &self.db_type
        }
    }
}

pub fn load_xlsx(file: &str, name: &str, tables: &Vec<String>, drop: bool, connection: &Connection) -> Result<()> {
    let mut workbook: Xlsx<_> = open_workbook(file)?;
    let sheet_names = workbook.sheet_names().to_owned();

    let path = std::path::PathBuf::from(file);
    let file_stem = path.file_stem().unwrap_or(std::ffi::OsStr::new("")).to_str().unwrap_or("");

    let schema_or_table = if !name.is_empty() {
        name
    } else {
        file_stem
    };

    let single_sheet = sheet_names.iter().len() == 1 || tables.len() == 1;

    if !single_sheet {
        connection.execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema_or_table}\" "), [])?;
    }

    for sheet_name in sheet_names.iter() {

        if !tables.is_empty() && !tables.contains(&sheet_name) {
            continue;
        } 

        let table_name = if single_sheet {
            format!("\"{schema_or_table}\"")
        } else {
            format!("\"{}\".\"{}\"", schema_or_table, sheet_name)
        };

        let range = workbook.worksheet_range(&sheet_name)?;

        let mut columns = vec![];
        let mut rows = vec![];

        let mut detectors = vec![];

        for (num, input_row) in range.rows().enumerate() {

            if num == 0 {
                for cell in input_row.iter() {
                    let value = data_to_string(cell);
                    if value.is_empty() {
                        columns.push(format!("column-{num}"));
                        continue;
                    }
                    columns.push(value);
                    detectors.push(Detector::new());
                }
                continue;
            }

            let mut row = vec![];
            for (col_num, cell) in input_row.iter().enumerate() {
                let value = data_to_string(cell);
                if value.is_empty() {
                    row.push(None);
                } else {
                    row.push(Some(value));
                }
                detectors[col_num].add(cell);
            }
            rows.push(row);
        }

        if drop {
            connection.execute(&format!("DROP TABLE IF EXISTS {};", table_name), [])?;
        }

        let mut create_table = format!("CREATE TABLE {} (", table_name);
        for (num, column) in columns.iter().enumerate() {

            let db_type = detectors[num].detect();
            let col_name = format!("\"{}\"", column);
            
            if num == columns.len() - 1 {
                create_table.push_str(&format!("{col_name} {db_type} NULL"));
            } else {
                create_table.push_str(&format!("{col_name} {db_type} NULL, "));
            }
        }
        create_table.push_str(");");

        connection.execute(&create_table, [])?;

        let mut appender = if single_sheet {
            connection.appender(&schema_or_table)?
        } else {
            connection.appender_to_db(&sheet_name, &schema_or_table)?  
        }; 

        for row in rows {
            appender.append_row(appender_params_from_iter(row))?;
        }
        appender.flush();
    }
    Ok(())
}

fn data_to_string(cell: &Data) -> String {
    let value = match cell {
        Data::Int(value) => {value.to_string()},
        Data::Float(value) => {value.to_string()},
        Data::String(value) => {value.to_string()},
        Data::Bool(value) => {value.to_string()},
        Data::DateTime(value) => {value.to_string()},
        Data::DateTimeIso(value) => {value.to_string()},
        Data::DurationIso(value) => {value.to_string()},
        Data::Error(value) => {value.to_string()},
        Data::Empty => {"".to_string()},
    };
    value
}

fn db_type(cell: &Data) -> &'static str {
    match cell {
        Data::Int(_) => "BIGINT",
        Data::Float(_) => "FLOAT",
        Data::String(_) => "TEXT",
        Data::Bool(_) => "BOOLEAN",
        Data::DateTime(_) => "TEXT",
        Data::DateTimeIso(_) => "TEXT",
        Data::DurationIso(_) => "TEXT",
        Data::Error(_) => "TEXT",
        Data::Empty => ""
    }
}