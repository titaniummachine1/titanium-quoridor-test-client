//! Quoridor distributed-testing worker.
//!
//! Usage:
//!   quoridor-test-client --coordinator https://xxx.workers.dev [--worker-id NAME] [--once]
//!
//! Per job: acquires NEW (job commit) and BASE engines — cache → prebuilt
//! download (sha256-verified) → build from source (cargo, target-cpu=native) —
//! then plays `game_count` games alternating colors and reports W/L/D.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

const POLL_INTERVAL: Duration = Duration::from_secs(30);
const MAX_PLIES: usize = 300; // draw cutoff
const KEEP_ENGINES: usize = 10;

#[derive(Debug, Deserialize)]
struct Job {
    job_id: Option<String>,
    #[serde(default)]
    commit_sha: String,
    #[serde(default)]
    repo: String,
    #[serde(default)]
    prebuilt_url: Option<String>,
    #[serde(default)]
    prebuilt_sha256: Option<String>,
    #[serde(default)]
    game_count: u32,
    #[serde(default = "default_movetime")]
    movetime_ms: u64,
    #[serde(default)]
    base_commit: String,
}

fn default_movetime() -> u64 {
    1000
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let coordinator = flag(&args, "--coordinator")
        .ok_or_else(|| anyhow!("--coordinator URL required"))?;
    let worker_id = flag(&args, "--worker-id").unwrap_or_else(|| {
        format!("worker-{}", std::process::id())
    });
    let once = args.iter().any(|a| a == "--once");
    let cache = cache_dir()?;
    eprintln!("[client] coordinator={coordinator} worker={worker_id} cache={}", cache.display());

    loop {
        match claim(&coordinator, &worker_id) {
            Ok(Some(job)) => {
                let id = job.job_id.clone().unwrap_or_default();
                eprintln!("[client] claimed {id} ({} games @ {}ms)", job.game_count, job.movetime_ms);
                match run_job(&cache, &job) {
                    Ok((w, l, d)) => {
                        eprintln!("[client] {id}: W{w} L{l} D{d} — submitting");
                        if let Err(e) = submit(&coordinator, &id, &worker_id, w, l, d) {
                            save_retry(&cache, &id, &worker_id, w, l, d)?;
                            eprintln!("[client] submit failed ({e}); saved for retry");
                        }
                    }
                    Err(e) => eprintln!("[client] job {id} failed: {e:#}"),
                }
            }
            Ok(None) => {
                if once {
                    eprintln!("[client] queue empty, exiting (--once)");
                    return Ok(());
                }
                std::thread::sleep(POLL_INTERVAL);
            }
            Err(e) => {
                eprintln!("[client] poll error: {e}; backing off");
                std::thread::sleep(POLL_INTERVAL);
            }
        }
        flush_retries(&cache, &coordinator);
        if once {
            return Ok(());
        }
    }
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn cache_dir() -> Result<PathBuf> {
    let base = dirs::cache_dir().unwrap_or_else(|| PathBuf::from("."));
    let dir = base.join("quoridor-fishtest");
    fs::create_dir_all(dir.join("engines"))?;
    fs::create_dir_all(dir.join("retry"))?;
    Ok(dir)
}

// ---------- coordinator I/O ----------

fn claim(coordinator: &str, worker_id: &str) -> Result<Option<Job>> {
    let url = format!("{coordinator}/api/job?worker={worker_id}");
    let resp = ureq::get(&url).timeout(Duration::from_secs(30)).call();
    match resp {
        Ok(r) if r.status() == 204 => Ok(None),
        Ok(r) => {
            let job: Job = r.into_json()?;
            Ok(if job.job_id.is_some() { Some(job) } else { None })
        }
        Err(ureq::Error::Status(204, _)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn submit(coordinator: &str, job_id: &str, worker: &str, w: u32, l: u32, d: u32) -> Result<()> {
    ureq::post(&format!("{coordinator}/api/result"))
        .timeout(Duration::from_secs(30))
        .send_json(serde_json::json!({
            "job_id": job_id, "wins": w, "losses": l, "draws": d, "worker": worker
        }))?;
    Ok(())
}

fn save_retry(cache: &Path, job_id: &str, worker: &str, w: u32, l: u32, d: u32) -> Result<()> {
    let path = cache.join("retry").join(format!("{job_id}.json"));
    fs::write(path, serde_json::json!({
        "job_id": job_id, "worker": worker, "w": w, "l": l, "d": d
    }).to_string())?;
    Ok(())
}

fn flush_retries(cache: &Path, coordinator: &str) {
    let Ok(entries) = fs::read_dir(cache.join("retry")) else { return };
    for e in entries.flatten() {
        let Ok(text) = fs::read_to_string(e.path()) else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
        let ok = submit(
            coordinator,
            v["job_id"].as_str().unwrap_or(""),
            v["worker"].as_str().unwrap_or(""),
            v["w"].as_u64().unwrap_or(0) as u32,
            v["l"].as_u64().unwrap_or(0) as u32,
            v["d"].as_u64().unwrap_or(0) as u32,
        )
        .is_ok();
        if ok {
            let _ = fs::remove_file(e.path());
        }
    }
}

// ---------- engine acquisition ----------

fn engine_binary_name() -> &'static str {
    if cfg!(windows) { "titanium.exe" } else { "titanium" }
}

/// cache hit → prebuilt download → build from source.
fn acquire_engine(cache: &Path, repo: &str, sha: &str, prebuilt: Option<(&str, Option<&str>)>) -> Result<PathBuf> {
    let short = &sha[..sha.len().min(8)];
    let dir = cache.join("engines").join(short);
    let bin = dir.join(engine_binary_name());
    if bin.exists() {
        return Ok(bin);
    }
    fs::create_dir_all(&dir)?;

    if let Some((url, want_hash)) = prebuilt {
        eprintln!("[client] downloading prebuilt for {short}");
        match download(url, &bin, want_hash) {
            Ok(()) => return Ok(bin),
            Err(e) => eprintln!("[client] prebuilt failed ({e}); falling back to source build"),
        }
    }

    eprintln!("[client] building {short} from source (~90s once, then cached)");
    build_from_source(cache, repo, sha, &bin)?;
    evict_old_engines(cache);
    Ok(bin)
}

fn download(url: &str, dest: &Path, want_hash: Option<&str>) -> Result<()> {
    let resp = ureq::get(url).timeout(Duration::from_secs(120)).call()?;
    let mut bytes = Vec::new();
    resp.into_reader().read_to_end(&mut bytes)?;
    if let Some(want) = want_hash {
        use sha2::{Digest, Sha256};
        let got = format!("{:x}", Sha256::digest(&bytes));
        if got != want.to_lowercase() {
            bail!("sha256 mismatch: got {got}, want {want}");
        }
    }
    fs::write(dest, &bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dest, fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

fn build_from_source(cache: &Path, repo: &str, sha: &str, bin_out: &Path) -> Result<()> {
    let src = cache.join("src-checkout");
    if !src.join(".git").exists() {
        run(Command::new("git").args(["clone", "--filter=blob:none", repo]).arg(&src))?;
    }
    run(Command::new("git").current_dir(&src).args(["fetch", "origin"]))?;
    run(Command::new("git").current_dir(&src).args(["checkout", "--force", sha]))?;

    let mut cmd = Command::new("cargo");
    cmd.current_dir(&src)
        .args(["build", "--release", "--bin", "titanium"])
        .env("RUSTFLAGS", "-C target-cpu=native");
    run(&mut cmd)?;

    let built = src.join("target/release").join(engine_binary_name());
    fs::copy(&built, bin_out).context("copy built binary into cache")?;
    Ok(())
}

fn run(cmd: &mut Command) -> Result<()> {
    let status = cmd.status().with_context(|| format!("spawn {cmd:?}"))?;
    if !status.success() {
        bail!("command failed ({status}): {cmd:?}");
    }
    Ok(())
}

fn evict_old_engines(cache: &Path) {
    let Ok(entries) = fs::read_dir(cache.join("engines")) else { return };
    let mut dirs: Vec<_> = entries.flatten().filter_map(|e| {
        let m = e.metadata().ok()?;
        Some((e.path(), m.modified().ok()?))
    }).collect();
    if dirs.len() <= KEEP_ENGINES { return; }
    dirs.sort_by_key(|(_, t)| *t);
    for (path, _) in dirs.into_iter().rev().skip(KEEP_ENGINES) {
        let _ = fs::remove_dir_all(path);
    }
}

// ---------- match runner ----------

struct UciEngine {
    child: Child,
    reader: BufReader<std::process::ChildStdout>,
}

impl UciEngine {
    fn start(bin: &Path) -> Result<Self> {
        let mut child = Command::new(bin)
            .arg("uci")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("spawn {}", bin.display()))?;
        let stdout = child.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;
        let mut e = UciEngine { child, reader: BufReader::new(stdout) };
        e.send("uci")?;
        e.wait_for("uciok")?;
        Ok(e)
    }

    fn send(&mut self, line: &str) -> Result<()> {
        let stdin = self.child.stdin.as_mut().ok_or_else(|| anyhow!("no stdin"))?;
        writeln!(stdin, "{line}")?;
        stdin.flush()?;
        Ok(())
    }

    fn wait_for(&mut self, prefix: &str) -> Result<String> {
        loop {
            let mut line = String::new();
            if self.reader.read_line(&mut line)? == 0 {
                bail!("engine closed stdout");
            }
            let t = line.trim();
            if t.starts_with(prefix) {
                return Ok(t.to_string());
            }
        }
    }

    /// Returns bestmove or None when the engine reports a terminal position.
    fn bestmove(&mut self, moves: &[String], movetime_ms: u64) -> Result<Option<String>> {
        self.send("ucinewgame")?;
        let pos = if moves.is_empty() {
            "position startpos".to_string()
        } else {
            format!("position startpos moves {}", moves.join(" "))
        };
        self.send(&pos)?;
        self.send(&format!("go movetime {movetime_ms}"))?;
        let line = self.wait_for("bestmove")?;
        let mv = line.split_whitespace().nth(1).unwrap_or("(none)").to_string();
        Ok(if mv == "(none)" { None } else { Some(mv) })
    }
}

impl Drop for UciEngine {
    fn drop(&mut self) {
        let _ = self.send("quit");
        let _ = self.child.wait();
    }
}

/// Plays one game. Returns +1 if NEW wins, -1 if BASE wins, 0 draw (ply cutoff).
fn play_game(new_bin: &Path, base_bin: &Path, new_first: bool, movetime_ms: u64) -> Result<i32> {
    let mut new_eng = UciEngine::start(new_bin)?;
    let mut base_eng = UciEngine::start(base_bin)?;
    let mut moves: Vec<String> = Vec::new();

    for ply in 0..MAX_PLIES {
        let new_to_move = (ply % 2 == 0) == new_first;
        let eng = if new_to_move { &mut new_eng } else { &mut base_eng };
        match eng.bestmove(&moves, movetime_ms)? {
            Some(mv) => moves.push(mv),
            // terminal before this side could move → the previous mover won
            None => {
                let prev_was_new = (ply % 2 == 1) == new_first;
                return Ok(if prev_was_new { 1 } else { -1 });
            }
        }
    }
    Ok(0)
}

fn run_job(cache: &Path, job: &Job) -> Result<(u32, u32, u32)> {
    let new_bin = acquire_engine(
        cache,
        &job.repo,
        &job.commit_sha,
        job.prebuilt_url.as_deref().map(|u| (u, job.prebuilt_sha256.as_deref())),
    )?;
    let base_sha = if job.base_commit.is_empty() { "main".to_string() } else { job.base_commit.clone() };
    let base_bin = acquire_engine(cache, &job.repo, &base_sha, None)?;

    let (mut w, mut l, mut d) = (0, 0, 0);
    for game in 0..job.game_count {
        let new_first = game % 2 == 0;
        match play_game(&new_bin, &base_bin, new_first, job.movetime_ms)? {
            1 => w += 1,
            -1 => l += 1,
            _ => d += 1,
        }
        eprintln!("[client]   game {}/{}: W{w} L{l} D{d}", game + 1, job.game_count);
    }
    Ok((w, l, d))
}
