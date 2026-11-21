//! The control plane's runtime in the WebAssembly build: tasks on the
//! JavaScript event loop, timers from the runtime, and one storage alarm.
use std::{rc::Rc, time::Duration};
use t3code_server::runtime::{ConnId, Runtime, Task, Wait};
use worker::{wasm_bindgen_futures::spawn_local, Delay, Storage};

pub struct WorkersRuntime {
    storage: Rc<Storage>,
}

impl WorkersRuntime {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Rc::new(storage),
        }
    }
}

impl Runtime for WorkersRuntime {
    fn spawn(&self, task: Task) {
        spawn_local(task);
    }

    fn sleep(&self, duration: Duration) -> Wait {
        Box::pin(Delay::from(duration))
    }

    fn yield_now(&self) -> Wait {
        Box::pin(Delay::from(Duration::ZERO))
    }

    fn set_alarm(&self, at_ms: i64) {
        let storage = self.storage.clone();
        let delay = (at_ms - self.now_ms()).max(0);
        spawn_local(async move {
            if let Err(error) = storage.set_alarm(delay).await {
                worker::console_error!("could not set the alarm: {error}");
            }
        });
    }

    // Sockets are the entry point's: it delivers frames itself after each
    // message and alarm.
    fn push(&self, _conn: ConnId, _frames: Vec<serde_json::Value>) {}

    fn connections(&self) -> Vec<ConnId> {
        Vec::new()
    }
}
