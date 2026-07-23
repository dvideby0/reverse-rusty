//! Reference external-broker publication seam for ADR-114.
//!
//! Reverse Rusty does not choose or embed a broker. A server integration
//! implements [`BrokerPublisher`] for Kafka, Pub/Sub, SQS, JetStream, or its
//! local equivalent and uses [`publish_at_least_once`] to retry the exact same
//! keyed frame. Consumer-side idempotency commits only a terminal completion.

use std::time::Duration;
use std::{error::Error, fmt};

/// Minimal keyed-frame interface implemented by an external broker adapter.
pub trait BrokerPublisher {
    type Error;

    fn publish(&mut self, idempotency_key: &str, payload: &[u8]) -> Result<(), Self::Error>;
}

/// A failed at-least-once publish.
#[derive(Debug)]
pub enum BrokerPublishError<E> {
    Cancelled,
    Exhausted { attempts: usize, source: E },
}

impl<E: fmt::Display> fmt::Display for BrokerPublishError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("broker publish cancelled"),
            Self::Exhausted { attempts, source } => {
                write!(
                    formatter,
                    "broker publish failed after {attempts} attempts: {source}"
                )
            }
        }
    }
}

impl<E: Error + 'static> Error for BrokerPublishError<E> {}

/// Retry one immutable keyed frame. Every attempt receives byte-identical key
/// and payload; this provides at-least-once transport, never a claim of
/// broker-side exactly-once effects.
pub fn publish_at_least_once<P, C>(
    publisher: &mut P,
    idempotency_key: &str,
    payload: &[u8],
    max_attempts: usize,
    retry_delay: Duration,
    mut cancelled: C,
) -> Result<usize, BrokerPublishError<P::Error>>
where
    P: BrokerPublisher,
    C: FnMut() -> bool,
{
    let attempts = max_attempts.max(1);
    for attempt in 1..=attempts {
        if cancelled() {
            return Err(BrokerPublishError::Cancelled);
        }
        match publisher.publish(idempotency_key, payload) {
            Ok(()) => return Ok(attempt),
            Err(source) if attempt == attempts => {
                return Err(BrokerPublishError::Exhausted {
                    attempts: attempt,
                    source,
                });
            }
            Err(_) => std::thread::sleep(retry_delay),
        }
    }
    unreachable!("max(1) guarantees one publish attempt")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Flaky {
        calls: Vec<(String, Vec<u8>)>,
        fail: usize,
    }

    impl BrokerPublisher for Flaky {
        type Error = &'static str;

        fn publish(&mut self, key: &str, payload: &[u8]) -> Result<(), Self::Error> {
            self.calls.push((key.to_string(), payload.to_vec()));
            if self.calls.len() <= self.fail {
                Err("transient")
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn retries_the_identical_keyed_frame() {
        let mut publisher = Flaky {
            calls: Vec::new(),
            fail: 2,
        };
        let attempts = publish_at_least_once(
            &mut publisher,
            "event:view:id",
            b"frame",
            3,
            Duration::ZERO,
            || false,
        )
        .expect("third attempt succeeds");
        assert_eq!(attempts, 3);
        assert_eq!(
            publisher.calls,
            vec![
                ("event:view:id".into(), b"frame".to_vec()),
                ("event:view:id".into(), b"frame".to_vec()),
                ("event:view:id".into(), b"frame".to_vec()),
            ]
        );
    }
}
