use super::*;

/// The sanctioned per-mode divergence: a missing winner source is our own
/// inconsistency locally (500) but an upstream failure in cluster mode
/// (502). Pinned here because both handlers now share every other arm.
#[test]
fn unavailable_status_divergence_is_pinned() {
    assert_eq!(
        <RankedMatchError as RankedBackendError>::UNAVAILABLE_STATUS,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        <ClusterRankedError as RankedBackendError>::UNAVAILABLE_STATUS,
        StatusCode::BAD_GATEWAY
    );
}
