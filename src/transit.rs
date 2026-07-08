use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

// Mutex guard that recovers a poisoned lock via into_inner.
// A panic under the lock must not brick the ride.
fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct Ride<P> {
    inner: Arc<RideInner<P>>,
}

impl<P> Clone for Ride<P> {
    fn clone(&self) -> Self {
        Ride {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct RideInner<P> {
    state: Mutex<RideState<P>>,
}

struct RideState<P> {
    seats: BTreeMap<usize, P>,
    next_id: usize,
}

// A seat's coordinate, shared between its Pass and the ride.
// Weak to the ride: the ride owns seats, never the reverse (no Arc cycle).
struct SeatCell<P> {
    loc: Mutex<Option<Loc<P>>>,
}

struct Loc<P> {
    ride: Weak<RideInner<P>>,
    id: usize,
}

#[must_use = "dropping the Pass leaves the ride"]
pub struct Pass {
    seat: Arc<dyn Leave + Send + Sync>,
}

impl Drop for Pass {
    fn drop(&mut self) {
        self.seat.leave();
    }
}

// Type erasure so Pass carries no P.
trait Leave {
    fn leave(&self);
}

impl<P: Send> Leave for SeatCell<P> {
    fn leave(&self) {
        loop {
            let (weak, id) = {
                let loc = locked(&self.loc);
                match &*loc {
                    Some(l) => (l.ride.clone(), l.id),
                    None => return,
                }
            };
            let Some(ride) = weak.upgrade() else { return };
            // Bind the removed cargo so the ride lock releases before P drops:
            // a P that owns another Pass must not re-enter leave under our lock.
            let removed = locked(&ride.state).seats.remove(&id);
            match removed {
                // Found it: loc pinned this exact (ride, id), so it is our seat.
                Some(_cargo) => {
                    *locked(&self.loc) = None;
                    return;
                }
                // Gone: the seat relocated after we read loc. Retry (hand-over-hand).
                None => continue,
            }
        }
    }
}

impl<P: Send + 'static> Default for Ride<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: Send + 'static> Ride<P> {
    pub fn new() -> Self {
        Ride {
            inner: Arc::new(RideInner {
                state: Mutex::new(RideState {
                    seats: BTreeMap::new(),
                    next_id: 0,
                }),
            }),
        }
    }

    pub fn board(&self, cargo: P) -> Pass {
        let mut state = locked(&self.inner.state);
        let id = state.next_id;
        state.next_id += 1;
        let cell = Arc::new(SeatCell {
            loc: Mutex::new(Some(Loc {
                ride: Arc::downgrade(&self.inner),
                id,
            })),
        });
        state.seats.insert(id, cargo);
        drop(state);
        Pass { seat: cell }
    }

    pub fn len(&self) -> usize {
        locked(&self.inner.state).seats.len()
    }

    pub fn is_empty(&self) -> bool {
        locked(&self.inner.state).seats.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boarding_accumulates_seats() {
        let ride: Ride<u32> = Ride::new();
        let _p1 = ride.board(1);
        let _p2 = ride.board(2);
        let _p3 = ride.board(3);
        assert_eq!(ride.len(), 3);
    }

    #[test]
    fn dropping_a_pass_removes_its_seat() {
        let ride: Ride<u32> = Ride::new();
        let p1 = ride.board(1);
        let p2 = ride.board(2);
        let p3 = ride.board(3);
        drop(p2); // drop from the middle, not an end
        assert_eq!(ride.len(), 2);
        drop(p1);
        drop(p3);
        assert_eq!(ride.len(), 0);
    }
}
