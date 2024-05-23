use std::{
    fs::{read_to_string, File},
    io::Write,
    path::Path,
};

fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();

    let current_dir = Path::new(".").canonicalize()?;
    write_all_sql(&current_dir)?;

    Ok(())
}

fn write_all_sql(dir: &Path) -> std::io::Result<()> {
    if dir.file_name().unwrap() == "queries" {
        let parent = dir.parent().unwrap().parent().unwrap();
        let const_sql_path = parent.join("const_sql.rs");

        let mut f = File::create(const_sql_path)?;

        for entry in std::fs::read_dir(dir)? {
            let entry_path = entry?.path();

            if entry_path.extension().and_then(|s| s.to_str()) == Some("sql") {
                let sql_string = read_sql(entry_path.to_str().unwrap());

                let const_name = entry_path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_uppercase();
                writeln!(
                    f,
                    "pub const {}: &str = r#\"{}\"#;\n",
                    const_name, sql_string
                )?;
            }
        }
    }

    // If this path is a directory, recurse into it
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                write_all_sql(&path)?;
            }
        }
    }

    Ok(())
}

fn read_sql(file_path: &str) -> String {
    read_to_string(file_path).expect("Failed to read SQL file")
}
