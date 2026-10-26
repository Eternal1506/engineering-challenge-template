//! [`KalshiCredentials`] loads the Kalshi API key id and private key.

use std::fmt;
use std::path::Path;

use anyhow::Context;

const KEY_ID_FILE: &str = "key_id.txt";
const PRIVATE_KEY_FILE: &str = "private_key.pem";
const KEY_ID_VAR: &str = "KALSHI_API_KEY_ID";
const PRIVATE_KEY_PATH_VAR: &str = "KALSHI_PRIVATE_KEY_PATH";

/// The Kalshi API key id and its PEM-encoded private key.
///
/// By default these are read from `key_id.txt` and `private_key.pem` in the
/// secrets directory. The `KALSHI_API_KEY_ID` environment variable (the key id
/// itself) and `KALSHI_PRIVATE_KEY_PATH` (a path to the PEM file) take
/// precedence over the files.
///
/// [`Debug`](fmt::Debug) output redacts both fields, so credentials can be
/// logged without leaking key material.
pub struct KalshiCredentials {
    key_id: String,
    private_key_pem: String,
}

impl KalshiCredentials {
    /// Load credentials from `secrets_dir`, with overrides from the process
    /// environment. Errors name the missing path or variable, never the
    /// secret contents.
    pub fn load(secrets_dir: &Path) -> anyhow::Result<Self> {
        Self::load_with_env(secrets_dir, |name| std::env::var(name).ok())
    }

    /// The API key id, sent with every signed request.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The PEM-encoded private key that signs requests.
    pub(crate) fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }

    /// Load credentials from `secrets_dir`, looking up overrides with `env`.
    fn load_with_env(
        secrets_dir: &Path,
        env: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            key_id: load_key_id(secrets_dir, &env)?,
            private_key_pem: load_private_key(secrets_dir, &env)?,
        })
    }
}

impl fmt::Debug for KalshiCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KalshiCredentials")
            .field("key_id", &"<redacted>")
            .field("private_key_pem", &"<redacted>")
            .finish()
    }
}

fn load_key_id(secrets_dir: &Path, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<String> {
    if let Some(key_id) = env(KEY_ID_VAR) {
        anyhow::ensure!(!key_id.trim().is_empty(), "{KEY_ID_VAR} is set but empty");
        return Ok(key_id.trim().to_owned());
    }
    let path = secrets_dir.join(KEY_ID_FILE);
    let key_id = read_secret(&path, "Kalshi API key id", &path.display().to_string())?;
    Ok(key_id.trim().to_owned())
}

fn load_private_key(
    secrets_dir: &Path,
    env: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<String> {
    match env(PRIVATE_KEY_PATH_VAR) {
        Some(value) => read_private_key_from_path_var(&value),
        None => {
            let path = secrets_dir.join(PRIVATE_KEY_FILE);
            read_secret(&path, "Kalshi private key", &path.display().to_string())
        }
    }
}

/// Reads the private key from the path held in `KALSHI_PRIVATE_KEY_PATH`.
/// Errors name the variable instead of its value, in case the value is the
/// key itself.
fn read_private_key_from_path_var(value: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        !value.trim().is_empty(),
        "{PRIVATE_KEY_PATH_VAR} is set but empty"
    );
    anyhow::ensure!(
        !value.contains("-----BEGIN") && !value.contains('\n'),
        "{PRIVATE_KEY_PATH_VAR} must be a file path, not the key itself"
    );
    let source = format!("the path in {PRIVATE_KEY_PATH_VAR}");
    read_secret(Path::new(value), "Kalshi private key", &source)
}

/// Reads a non-empty secret from `path`. Errors call the file `source` and
/// never include its contents.
fn read_secret(path: &Path, what: &str, source: &str) -> anyhow::Result<String> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {what} from {source}"))?;
    anyhow::ensure!(!contents.trim().is_empty(), "{what} at {source} is empty");
    Ok(contents)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    const FILE_KEY_ID: &str = "file-key-id";
    const FILE_PEM_BODY: &str = "RmlsZVByaXZhdGVLZXlCb2R5";
    const ENV_PEM: &str = "-----BEGIN PRIVATE KEY-----\nenv-key\n-----END PRIVATE KEY-----\n";

    fn file_pem() -> String {
        format!("-----BEGIN PRIVATE KEY-----\n{FILE_PEM_BODY}\n-----END PRIVATE KEY-----\n")
    }

    fn secrets_dir_with_files() -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(KEY_ID_FILE), format!("  {FILE_KEY_ID}\n")).unwrap();
        fs::write(dir.path().join(PRIVATE_KEY_FILE), file_pem()).unwrap();
        dir
    }

    fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let vars: HashMap<String, String> = pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        move |name| vars.get(name).cloned()
    }

    fn load_error(dir: &TempDir, env: &[(&str, &str)]) -> String {
        let error = KalshiCredentials::load_with_env(dir.path(), env_from(env)).unwrap_err();
        format!("{error:#}")
    }

    #[test]
    fn loads_trimmed_key_id_and_pem_from_secrets_dir() {
        let dir = secrets_dir_with_files();
        let credentials = KalshiCredentials::load_with_env(dir.path(), env_from(&[])).unwrap();
        assert_eq!(
            (credentials.key_id(), credentials.private_key_pem()),
            (FILE_KEY_ID, file_pem().as_str())
        );
    }

    #[test]
    fn environment_overrides_win_over_files() {
        let dir = secrets_dir_with_files();
        let env_key_dir = tempfile::tempdir().unwrap();
        let env_key_path = env_key_dir.path().join("other.pem");
        fs::write(&env_key_path, ENV_PEM).unwrap();
        let env = env_from(&[
            (KEY_ID_VAR, "env-key-id"),
            (PRIVATE_KEY_PATH_VAR, env_key_path.to_str().unwrap()),
        ]);

        let credentials = KalshiCredentials::load_with_env(dir.path(), env).unwrap();

        assert_eq!(
            (credentials.key_id(), credentials.private_key_pem()),
            ("env-key-id", ENV_PEM)
        );
    }

    #[test]
    fn missing_key_id_file_error_names_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let expected = dir.path().join(KEY_ID_FILE);
        assert!(load_error(&dir, &[]).contains(&expected.display().to_string()));
    }

    #[test]
    fn missing_private_key_file_error_names_the_path() {
        let dir = secrets_dir_with_files();
        fs::remove_file(dir.path().join(PRIVATE_KEY_FILE)).unwrap();
        let expected = dir.path().join(PRIVATE_KEY_FILE);
        assert!(load_error(&dir, &[]).contains(&expected.display().to_string()));
    }

    #[test]
    fn missing_path_variable_file_error_names_the_variable_not_the_value() {
        let dir = secrets_dir_with_files();
        let missing = dir.path().join("missing.pem");
        let value = missing.to_str().unwrap();
        let error = load_error(&dir, &[(PRIVATE_KEY_PATH_VAR, value)]);
        assert!(error.contains(PRIVATE_KEY_PATH_VAR) && !error.contains(value));
    }

    #[test]
    fn pem_text_in_path_variable_is_rejected_without_echoing_it() {
        let dir = secrets_dir_with_files();
        let error = load_error(&dir, &[(PRIVATE_KEY_PATH_VAR, ENV_PEM)]);
        assert_eq!(
            error,
            format!("{PRIVATE_KEY_PATH_VAR} must be a file path, not the key itself")
        );
    }

    #[test]
    fn single_line_pem_header_in_path_variable_is_rejected() {
        let dir = secrets_dir_with_files();
        let value = "-----BEGIN PRIVATE KEY----- env-key";
        let error = load_error(&dir, &[(PRIVATE_KEY_PATH_VAR, value)]);
        assert!(!error.contains(value) && error.contains("not the key itself"));
    }

    #[test]
    fn empty_path_variable_is_rejected() {
        let dir = secrets_dir_with_files();
        let error = load_error(&dir, &[(PRIVATE_KEY_PATH_VAR, "  ")]);
        assert_eq!(error, format!("{PRIVATE_KEY_PATH_VAR} is set but empty"));
    }

    #[test]
    fn empty_key_id_variable_is_rejected() {
        let dir = secrets_dir_with_files();
        let error = load_error(&dir, &[(KEY_ID_VAR, "  ")]);
        assert_eq!(error, format!("{KEY_ID_VAR} is set but empty"));
    }

    #[test]
    fn debug_output_hides_the_key_id() {
        let dir = secrets_dir_with_files();
        let credentials = KalshiCredentials::load_with_env(dir.path(), env_from(&[])).unwrap();
        assert!(!format!("{credentials:?}").contains(FILE_KEY_ID));
    }

    #[test]
    fn debug_output_hides_the_private_key() {
        let dir = secrets_dir_with_files();
        let credentials = KalshiCredentials::load_with_env(dir.path(), env_from(&[])).unwrap();
        assert!(!format!("{credentials:?}").contains(FILE_PEM_BODY));
    }
}
