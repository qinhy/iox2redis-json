use std::fs;

const UNKNOWN_ICEORYX2_VERSION: &str = "unknown";

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-changed=Cargo.toml");

    let version = fs::read_to_string("Cargo.lock")
        .ok()
        .and_then(|lock| package_version(&lock, "iceoryx2"))
        .unwrap_or_else(|| UNKNOWN_ICEORYX2_VERSION.to_owned());

    println!("cargo:rustc-env=IOX2REDIS_ICEORYX2_VERSION={version}");
}

fn package_version(lock: &str, package_name: &str) -> Option<String> {
    let mut in_package = false;
    let mut name_matches = false;

    for line in lock.lines() {
        let trimmed = line.trim();
        if trimmed == "[[package]]" {
            in_package = true;
            name_matches = false;
            continue;
        }
        if !in_package {
            continue;
        }
        if let Some(raw_name) = trimmed.strip_prefix("name = ") {
            if let Some(name) = quoted_value(raw_name) {
                name_matches = name == package_name;
            }
            continue;
        }
        if name_matches {
            if let Some(version) = trimmed
                .strip_prefix("version = ")
                .and_then(quoted_value)
            {
                return Some(version.to_owned());
            }
        }
    }

    None
}

fn quoted_value(value: &str) -> Option<&str> {
    value.strip_prefix('"')?.strip_suffix('"')
}
