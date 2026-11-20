//! The runtime the RPC layer runs on: background tasks, short waits, one
//! wake-up alarm, and the open client connections.
//!
//! Dispatch and subscriptions reach the event loop only through [`Runtime`],
//! never through `tokio::spawn`, `tokio::time` or `Instant`, so the server
//! core can run on an event loop that is not tokio's (a single-threaded
//! WebAssembly host has no tokio timers and panics on `Instant::now`). Time
//! itself comes from [`crate::platform`].
//!
//! Work that must outlive the current request without holding a future
//! (retrying later, expiring an entry) sets the alarm; the host then calls
//! back into its alarm handler at or after that time. A future passed to
//! [`Runtime::spawn`] or [`Runtime::sleep`] is lost if the process stops
//! or is suspended, so neither suits work that must happen eventually.
use futures_util::future::BoxFuture;
use serde_json::Value;
use std::time::Duration;

/// An open client connection, unique for the life of the process.
pub type ConnId = u64;

/// A task handed to the runtime. Natively tasks run on a multi-threaded
/// executor and must be `Send`; a single-threaded host's need not be.
#[cfg(feature = "native")]
pub type Task = BoxFuture<'static, ()>;
#[cfg(not(feature = "native"))]
pub type Task = futures_util::future::LocalBoxFuture<'static, ()>;

/// A wait the runtime resolves.
#[cfg(feature = "native")]
pub type Wait = BoxFuture<'static, ()>;
#[cfg(not(feature = "native"))]
pub type Wait = futures_util::future::LocalBoxFuture<'static, ()>;

pub trait Runtime: crate::workspace::MaybeSendSync {
    /// Wall-clock milliseconds since the Unix epoch.
    fn now_ms(&self) -> i64 {
        crate::platform::now_ms()
    }
    /// Run `task` in the background.
    fn spawn(&self, task: Task);
    /// Resolve after `duration`.
    fn sleep(&self, duration: Duration) -> Wait;
    /// Let other tasks run before continuing.
    fn yield_now(&self) -> Wait;
    /// Ask for one alarm at `at_ms` (wall-clock milliseconds), replacing any
    /// earlier request. The host calls its alarm handler once it is due.
    fn set_alarm(&self, at_ms: i64);
    /// Queue `frames` (RPC frames, before framing) on connection `conn`.
    /// Frames for a connection that has closed are dropped.
    fn push(&self, conn: ConnId, frames: Vec<Value>);
    /// The connections open now.
    fn connections(&self) -> Vec<ConnId>;
}

#[cfg(feature = "native")]
pub use native::TokioRuntime;

#[cfg(feature = "native")]
mod native {
    use super::*;
    use futures_util::FutureExt;
    use std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, OnceLock,
        },
    };
    use tokio::sync::mpsc;

    type AlarmHandler = Arc<dyn Fn() + Send + Sync>;

    /// The native runtime: tokio tasks and timers, and connections that are
    /// channels to each socket's writer.
    #[derive(Default)]
    pub struct TokioRuntime {
        next_conn: AtomicU64,
        connections: Mutex<BTreeMap<ConnId, mpsc::UnboundedSender<Vec<Value>>>>,
        alarm: Mutex<Option<tokio::task::JoinHandle<()>>>,
        alarm_handler: OnceLock<AlarmHandler>,
    }

    impl TokioRuntime {
        /// Where alarms go. Only the first call takes effect.
        pub fn on_alarm(&self, handler: impl Fn() + Send + Sync + 'static) {
            let _ = self.alarm_handler.set(Arc::new(handler));
        }

        /// Register a connection; frames pushed to it arrive on the receiver
        /// until [`close`](Self::close).
        pub fn open(&self) -> (ConnId, mpsc::UnboundedReceiver<Vec<Value>>) {
            let id = self.next_conn.fetch_add(1, Ordering::Relaxed) + 1;
            let (tx, rx) = mpsc::unbounded_channel();
            self.connections.lock().unwrap().insert(id, tx);
            (id, rx)
        }

        pub fn close(&self, conn: ConnId) {
            self.connections.lock().unwrap().remove(&conn);
        }
    }

    impl Runtime for TokioRuntime {
        fn spawn(&self, task: Task) {
            tokio::spawn(task);
        }

        fn sleep(&self, duration: Duration) -> Wait {
            tokio::time::sleep(duration).boxed()
        }

        fn yield_now(&self) -> Wait {
            tokio::task::yield_now().boxed()
        }

        fn set_alarm(&self, at_ms: i64) {
            let delay = Duration::from_millis((at_ms - self.now_ms()).max(0) as u64);
            let handler = self.alarm_handler.get().cloned();
            let task = tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                if let Some(handler) = handler {
                    handler();
                }
            });
            if let Some(previous) = self.alarm.lock().unwrap().replace(task) {
                previous.abort();
            }
        }

        fn push(&self, conn: ConnId, frames: Vec<Value>) {
            if let Some(tx) = self.connections.lock().unwrap().get(&conn) {
                let _ = tx.send(frames);
            }
        }

        fn connections(&self) -> Vec<ConnId> {
            self.connections.lock().unwrap().keys().copied().collect()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde_json::json;

        #[tokio::test]
        async fn pushes_reach_open_connections_only() {
            let runtime = TokioRuntime::default();
            let (one, mut frames) = runtime.open();
            let (two, _) = runtime.open();
            assert_eq!(runtime.connections(), vec![one, two]);
            runtime.push(one, vec![json!({"_tag":"Pong"})]);
            assert_eq!(frames.recv().await, Some(vec![json!({"_tag":"Pong"})]));
            runtime.close(one);
            runtime.push(one, vec![json!(1)]);
            assert_eq!(runtime.connections(), vec![two]);
            assert_eq!(frames.recv().await, None);
        }

        #[tokio::test]
        async fn a_later_alarm_replaces_an_earlier_one() {
            let runtime = TokioRuntime::default();
            let (tx, mut fired) = mpsc::unbounded_channel();
            runtime.on_alarm(move || {
                let _ = tx.send(());
            });
            runtime.set_alarm(runtime.now_ms() + 60_000);
            runtime.set_alarm(runtime.now_ms() + 10);
            tokio::time::timeout(Duration::from_secs(5), fired.recv())
                .await
                .expect("the alarm fires")
                .unwrap();
            runtime.sleep(Duration::from_millis(50)).await;
            assert!(fired.try_recv().is_err(), "only the latest alarm fires");
        }
    }
}
