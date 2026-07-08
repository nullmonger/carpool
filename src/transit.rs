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
    seats: BTreeMap<usize, SeatEntry<P>>,
    next_id: usize,
}

// The ride keeps each seat's cell so take() can readdress a moved seat's loc.
struct SeatEntry<P> {
    cargo: P,
    cell: Arc<SeatCell<P>>,
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
            // Bind the removed entry so the ride lock releases before its payload drops:
            // a P that owns another Pass must not re-enter leave under our lock.
            let removed = locked(&ride.state).seats.remove(&id);
            match removed {
                // Found it: loc pinned this exact (ride, id), so it is our seat.
                Some(SeatEntry { cargo, .. }) => {
                    *locked(&self.loc) = None;
                    drop(cargo);
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
        state.seats.insert(
            id,
            SeatEntry {
                cargo,
                cell: Arc::clone(&cell),
            },
        );
        drop(state);
        Pass { seat: cell }
    }

    // Move the first n seats (boarding order) to a fresh ride; the tail stays.
    // No new Pass: each holder's guard is readdressed to the taken ride.
    pub fn take(&self, n: usize) -> Ride<P> {
        let taken = Ride::new();
        let mut src = locked(&self.inner.state);
        let front = if n >= src.seats.len() {
            std::mem::take(&mut src.seats)
        } else {
            let split_key = *src.seats.keys().nth(n).expect("n < len in this branch");
            let tail = src.seats.split_off(&split_key);
            std::mem::replace(&mut src.seats, tail)
        };
        {
            let mut dst = locked(&taken.inner.state);
            for (id, entry) in front {
                *locked(&entry.cell.loc) = Some(Loc {
                    ride: Arc::downgrade(&taken.inner),
                    id,
                });
                dst.seats.insert(id, entry);
            }
            // Carry the counter so a later board on taken cannot reuse a moved seat's id.
            dst.next_id = src.next_id;
        }
        drop(src);
        taken
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

    #[test]
    fn take_edges_move_none_or_all() {
        let ride: Ride<u32> = Ride::new();
        let _passes: Vec<Pass> = (0..3).map(|i| ride.board(i)).collect();

        let none = ride.take(0);
        assert_eq!((none.len(), ride.len()), (0, 3));

        let all = ride.take(5); // n > len moves everything
        assert_eq!((all.len(), ride.len()), (3, 0));
    }

    #[test]
    fn a_moved_pass_leaves_from_its_new_ride() {
        let ride: Ride<u32> = Ride::new();
        let p0 = ride.board(0);
        let p1 = ride.board(1);
        let _p2 = ride.board(2);
        let taken = ride.take(2); // p0, p1 relocate to taken; p2 stays
        assert_eq!((taken.len(), ride.len()), (2, 1));
        drop(p0);
        assert_eq!((taken.len(), ride.len()), (1, 1));
        drop(p1);
        assert_eq!((taken.len(), ride.len()), (0, 1));
    }
}
