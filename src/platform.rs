//! Clock and randomness for platform-neutral code.
//!
//! Shared modules read the time and draw random bytes through these traits,
//! never through `SystemTime`, `Instant`, `OsRng` or `Uuid::new_v4` directly.
//! On wasm32-unknown-unknown `SystemTime::now` and `Instant::now` compile but
//! panic, and other builds may want to substitute a deterministic
//! source. The process-wide default ([`SystemClock`], [`SystemRandom`]) works
//! on both builds. [`install`] replaces it once at start-up, and [`scoped`]
//! overrides it on the current thread, which is how tests pin the time.
use rand::{CryptoRng, RngCore};
use std::{
    cell::RefCell,
    sync::{Arc, OnceLock},
};
use uuid::Uuid;

/// A source of the current time.
pub trait Clock: Send + Sync {
    /// Wall-clock milliseconds since the Unix epoch.
    fn now_ms(&self) -> i64;
    /// Milliseconds from an arbitrary fixed origin that never decrease, for
    /// measuring intervals. Not comparable across processes.
    fn monotonic_ms(&self) -> u64;
}

/// A source of cryptographically secure random bytes.
pub trait Random: Send + Sync {
    fn fill_bytes(&self, dest: &mut [u8]);
}

/// The clock and random source in effect.
#[derive(Clone)]
pub struct Platform {
    pub clock: Arc<dyn Clock>,
    pub random: Arc<dyn Random>,
}

impl Default for Platform {
    fn default() -> Self {
        Self {
            clock: Arc::new(SystemClock),
            random: Arc::new(SystemRandom),
        }
    }
}

/// The operating system's clock. Natively the monotonic reading is an
/// `Instant` since first use. Workers have no `Instant`, so it is the wall
/// clock (`Date.now()`) held at its highest value so far.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        // `time` reads `Date.now()` on wasm32 (the `time/wasm-bindgen` feature).
        (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn monotonic_ms(&self) -> u64 {
        static ORIGIN: OnceLock<std::time::Instant> = OnceLock::new();
        ORIGIN
            .get_or_init(std::time::Instant::now)
            .elapsed()
            .as_millis() as u64
    }

    #[cfg(target_arch = "wasm32")]
    fn monotonic_ms(&self) -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static HIGHEST: AtomicU64 = AtomicU64::new(0);
        let now = self.now_ms().max(0) as u64;
        HIGHEST.fetch_max(now, Ordering::Relaxed).max(now)
    }
}

/// `getrandom`: the OS source natively, `crypto.getRandomValues` in Workers.
pub struct SystemRandom;

impl Random for SystemRandom {
    fn fill_bytes(&self, dest: &mut [u8]) {
        getrandom::getrandom(dest).expect("the platform random source failed");
    }
}

static INSTALLED: OnceLock<Platform> = OnceLock::new();

thread_local! {
    static SCOPED: RefCell<Option<Platform>> = const { RefCell::new(None) };
}

/// Install the process-wide platform. Only the first call takes effect; it
/// returns the platform back if one was already installed or read.
pub fn install(platform: Platform) -> Result<(), Platform> {
    INSTALLED.set(platform)
}

/// Run `f` with `platform` in effect on this thread, restoring the previous
/// one afterwards (also on panic).
pub fn scoped<R>(platform: Platform, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<Platform>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = self.0.take();
            SCOPED.with(|slot| *slot.borrow_mut() = previous);
        }
    }
    let _restore = Restore(SCOPED.with(|slot| slot.borrow_mut().replace(platform)));
    f()
}

/// The platform in effect on this thread.
pub fn current() -> Platform {
    SCOPED
        .with(|slot| slot.borrow().clone())
        .unwrap_or_else(|| INSTALLED.get_or_init(Platform::default).clone())
}

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    current().clock.now_ms()
}

/// Wall-clock whole seconds since the Unix epoch, never negative.
pub fn now_secs() -> u64 {
    (now_ms().max(0) / 1000) as u64
}

/// Monotonic milliseconds; see [`Clock::monotonic_ms`].
pub fn monotonic_ms() -> u64 {
    current().clock.monotonic_ms()
}

/// Fill `dest` from the platform random source.
pub fn fill_bytes(dest: &mut [u8]) {
    current().random.fill_bytes(dest)
}

/// `N` random bytes.
pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0; N];
    fill_bytes(&mut bytes);
    bytes
}

/// A random (version 4) UUID.
pub fn uuid_v4() -> Uuid {
    uuid::Builder::from_random_bytes(random_bytes()).into_uuid()
}

/// A `rand` generator over the platform random source, for APIs that take
/// an `RngCore` (key generation, `gen_range`).
pub fn rng() -> PlatformRng {
    PlatformRng(current().random)
}

pub struct PlatformRng(Arc<dyn Random>);

impl RngCore for PlatformRng {
    fn next_u32(&mut self) -> u32 {
        let mut bytes = [0; 4];
        self.0.fill_bytes(&mut bytes);
        u32::from_le_bytes(bytes)
    }

    fn next_u64(&mut self) -> u64 {
        let mut bytes = [0; 8];
        self.0.fill_bytes(&mut bytes);
        u64::from_le_bytes(bytes)
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.0.fill_bytes(dest)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.0.fill_bytes(dest);
        Ok(())
    }
}

impl CryptoRng for PlatformRng {}

/// A clock whose time only moves when told to, for tests and replay.
#[derive(Default)]
pub struct ManualClock {
    ms: std::sync::atomic::AtomicI64,
}

impl ManualClock {
    pub fn at(ms: i64) -> Self {
        Self { ms: ms.into() }
    }

    pub fn advance(&self, ms: i64) {
        self.ms.fetch_add(ms, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.ms.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn monotonic_ms(&self) -> u64 {
        self.now_ms().max(0) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bytes from a counter, so draws are reproducible.
    struct CountingRandom(std::sync::atomic::AtomicU8);
    impl Random for CountingRandom {
        fn fill_bytes(&self, dest: &mut [u8]) {
            for byte in dest {
                *byte = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn system_clock_reads_the_wall_clock() {
        let ms = SystemClock.now_ms();
        let system = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!((system - ms).abs() < 5_000, "{system} vs {ms}");
        let first = SystemClock.monotonic_ms();
        assert!(SystemClock.monotonic_ms() >= first);
    }

    #[test]
    fn scoped_platform_overrides_and_restores() {
        let clock = Arc::new(ManualClock::at(1_700_000_000_123));
        let platform = Platform {
            clock: clock.clone(),
            random: Arc::new(CountingRandom(0.into())),
        };
        scoped(platform, || {
            assert_eq!(now_ms(), 1_700_000_000_123);
            assert_eq!(now_secs(), 1_700_000_000);
            clock.advance(2_000);
            assert_eq!(now_secs(), 1_700_000_002);
            assert_eq!(random_bytes::<4>(), [0, 1, 2, 3]);
            let id = uuid_v4();
            assert_eq!(id.get_version_num(), 4);
            assert_eq!(id.to_string(), "04050607-0809-4a0b-8c0d-0e0f10111213");
            assert_eq!(rng().next_u32(), u32::from_le_bytes([20, 21, 22, 23]));
        });
        assert!(now_ms() > 1_700_000_002_123);
    }

    #[test]
    fn scoped_platform_is_restored_after_a_panic() {
        let platform = Platform {
            clock: Arc::new(ManualClock::at(5)),
            random: Arc::new(SystemRandom),
        };
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            scoped(platform, || panic!("inside"))
        }));
        assert!(caught.is_err());
        assert!(now_ms() > 5);
    }

    #[test]
    fn shared_time_helpers_follow_the_platform_clock() {
        let platform = Platform {
            clock: Arc::new(ManualClock::at(1_700_000_000_123)),
            random: Arc::new(SystemRandom),
        };
        scoped(platform, || {
            assert_eq!(crate::store::now(), 1_700_000_000);
            assert_eq!(crate::auth_service::now(), 1_700_000_000_123);
            assert_eq!(
                crate::auth_service::iso(crate::auth_service::now()),
                "2023-11-14T22:13:20.123Z"
            );
        });
    }
}
