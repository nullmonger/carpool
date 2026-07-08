use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tokio::sync::Notify;

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
    boarded: Notify,
    abandoned: Notify,
}

struct RideState<P> {
    seats: BTreeMap<usize, SeatEntry<P>>,
    next_id: usize,
    departed: bool,
    abandoned: bool,
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

// Outcome of one leave attempt against the ride the seat currently points to.
enum Leaving<P> {
    Removed { entry: SeatEntry<P>, emptied: bool },
    Relocated, // seat moved; retry with fresh loc
    Departed,  // ride left; seat stays, drop is a no-op
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
            // Decide removal and the abandon latch under one lock;
            // act off-lock (drop payload, notify) so no user code runs under it.
            let outcome = {
                let mut state = locked(&ride.state);
                if state.departed {
                    Leaving::Departed
                } else if let Some(entry) = state.seats.remove(&id) {
                    let emptied = state.seats.is_empty() && !state.abandoned;
                    if emptied {
                        state.abandoned = true;
                    }
                    Leaving::Removed { entry, emptied }
                } else {
                    Leaving::Relocated
                }
            };
            match outcome {
                // Found it: loc pinned this exact (ride, id), so it is our seat.
                Leaving::Removed {
                    entry: SeatEntry { cargo, .. },
                    emptied,
                } => {
                    *locked(&self.loc) = None;
                    // Signal before the cargo drop:
                    // a panicking P::drop must not swallow the one-shot abandon wakeup.
                    if emptied {
                        ride.abandoned.notify_waiters();
                    }
                    drop(cargo);
                    return;
                }
                Leaving::Departed => return,
                // Gone: the seat relocated after we read loc. Retry (hand-over-hand).
                Leaving::Relocated => continue,
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
                    departed: false,
                    abandoned: false,
                }),
                boarded: Notify::new(),
                abandoned: Notify::new(),
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
        self.inner.boarded.notify_waiters();
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

    // Seal the ride: a Pass dropped afterwards no-ops (see Leaving::Departed).
    pub fn depart(&self) {
        locked(&self.inner.state).departed = true;
    }

    // Resolve once every seat has left before departure (the abandon latch).
    // The latch is irreversible, so this stays ready afterwards.
    pub async fn abandoned(&self) {
        loop {
            let mut signal = std::pin::pin!(self.inner.abandoned.notified());
            signal.as_mut().enable();
            let latched = locked(&self.inner.state).abandoned;
            if latched {
                return;
            }
            signal.await;
        }
    }

    // Resolve once the ride holds a seat.
    // Enable before the check so a boarding in the gap is not lost.
    pub async fn boarded(&self) {
        loop {
            let mut signal = std::pin::pin!(self.inner.boarded.notified());
            signal.as_mut().enable();
            if !self.is_empty() {
                return;
            }
            signal.await;
        }
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

    #[test]
    fn after_depart_dropping_a_pass_is_a_noop() {
        let ride: Ride<u32> = Ride::new();
        let p = ride.board(1);
        let _p2 = ride.board(2);
        ride.depart();
        drop(p); // no-op after departure: the seat stays for delivery
        assert_eq!(ride.len(), 2);
    }

    #[tokio::test]
    async fn abandoned_latches_when_the_last_seat_leaves() {
        let ride: Ride<u32> = Ride::new();
        let p1 = ride.board(1);
        let p2 = ride.board(2);
        drop(p1);
        drop(p2); // last to leave sets the latch synchronously
        ride.abandoned().await;
    }

    #[tokio::test]
    async fn boarded_resolves_once_a_seat_is_present() {
        let ride: Ride<u32> = Ride::new();
        let _p = ride.board(1);
        ride.boarded().await;
    }
}
