use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
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
    drained: bool,
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
                    drained: false,
                }),
                boarded: Notify::new(),
                abandoned: Notify::new(),
            }),
        }
    }

    pub fn board(&self, cargo: P) -> Pass {
        let pass = {
            let mut state = locked(&self.inner.state);
            self.seat(&mut state, cargo)
        };
        self.inner.boarded.notify_waiters();
        pass
    }

    // Board only into a still-open ride (not abandoned, departed, or drained);
    // otherwise hand the cargo back so the caller (Logue) founds a fresh ride.
    fn try_board(&self, cargo: P) -> Result<Pass, P> {
        let pass = {
            let mut state = locked(&self.inner.state);
            if state.abandoned || state.departed || state.drained {
                return Err(cargo);
            }
            self.seat(&mut state, cargo)
        };
        self.inner.boarded.notify_waiters();
        Ok(pass)
    }

    // Insert a seat under the held lock and return its Pass; the caller notifies.
    fn seat(&self, state: &mut RideState<P>, cargo: P) -> Pass {
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

impl<P> IntoIterator for Ride<P> {
    type Item = P;
    type IntoIter = std::vec::IntoIter<P>;

    // Drain: extract seats under the lock, then hand out cargo off-lock in order.
    fn into_iter(self) -> Self::IntoIter {
        let seats = {
            let mut state = locked(&self.inner.state);
            // Clear locs under the lock so a mid-drain Pass drop no-ops, not spins.
            for entry in state.seats.values() {
                *locked(&entry.cell.loc) = None;
            }
            state.drained = true;
            std::mem::take(&mut state.seats)
        };
        let cargo: Vec<P> = seats.into_values().map(|entry| entry.cargo).collect();
        cargo.into_iter()
    }
}

// Result of book(): either joined an existing ride,
// or founded a new one whose handle comes back (to drive it and check out by identity).
pub enum Booking<P> {
    Joined(Pass),
    Founded(Pass, Ride<P>),
}

pub struct Logue<K, P> {
    rides: Mutex<HashMap<K, Ride<P>>>,
}

impl<K, P> Default for Logue<K, P> {
    fn default() -> Self {
        Logue {
            rides: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, P> Logue<K, P> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K: Hash + Eq + Clone, P: Send + 'static> Logue<K, P> {
    // Join the key's live ride, or found a new one.
    // An abandoned ride under the key is not joined: it is replaced by the freshly founded one.
    pub fn book(&self, key: K, cargo: P) -> Booking<P> {
        let mut rides = locked(&self.rides);
        let cargo = if let Some(ride) = rides.get(&key) {
            match ride.try_board(cargo) {
                Ok(pass) => return Booking::Joined(pass),
                Err(cargo) => cargo, // abandoned: reuse cargo to found a new ride
            }
        } else {
            cargo
        };
        let ride = Ride::new();
        let pass = ride.board(cargo);
        rides.insert(key, ride.clone());
        Booking::Founded(pass, ride)
    }

    // Remove the key's ride only if it is this exact ride (by identity),
    // so a stale checkout cannot evict a fresh same-key ride.
    pub fn checkout(&self, key: &K, ride: &Ride<P>) -> bool {
        let mut rides = locked(&self.rides);
        let matches = rides
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(&current.inner, &ride.inner));
        if matches {
            rides.remove(key);
        }
        matches
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

    #[test]
    fn draining_empties_the_shared_instance() {
        let ride: Ride<u32> = Ride::new();
        let _passes: Vec<Pass> = (0..3).map(|i| ride.board(i)).collect();
        let other = ride.clone();
        let drained: Vec<u32> = ride.into_iter().collect();
        assert_eq!(drained, vec![0, 1, 2]); // boarding order preserved
        assert_eq!(other.len(), 0); // shared instance emptied for other holders
    }

    #[test]
    fn a_pass_dropped_after_drain_is_a_noop() {
        let ride: Ride<u32> = Ride::new();
        let p = ride.board(1);
        let other = ride.clone();
        let _drained: Vec<u32> = ride.into_iter().collect();
        drop(p); // loc cleared by drain, so this no-ops
        assert_eq!(other.len(), 0);
    }

    #[test]
    fn book_founds_then_joins_the_same_key() {
        let logue: Logue<u32, u32> = Logue::new();
        let first = logue.book(1, 100);
        assert!(matches!(&first, Booking::Founded(..)));
        // first stays aboard, so booking the same key joins the live ride
        let second = logue.book(1, 101);
        assert!(matches!(&second, Booking::Joined(_)));
    }

    #[test]
    fn checkout_only_removes_the_identical_ride() {
        let logue: Logue<u32, u32> = Logue::new();
        let Booking::Founded(p1, ride1) = logue.book(1, 100) else {
            panic!("first book founds");
        };
        drop(p1); // ride1 empties and latches abandoned
        let Booking::Founded(_p2, ride2) = logue.book(1, 101) else {
            panic!("an abandoned ride is replaced, so this founds again");
        };
        assert!(!logue.checkout(&1, &ride1)); // stale handle evicts nothing
        assert!(logue.checkout(&1, &ride2)); // the current ride checks out
    }

    #[test]
    fn book_founds_again_after_the_ride_is_drained() {
        let logue: Logue<u32, u32> = Logue::new();
        let Booking::Founded(_p1, ride1) = logue.book(1, 100) else {
            panic!("first book founds");
        };
        let _drained: Vec<u32> = ride1.into_iter().collect();
        // the drained ride is terminal, so booking the same key founds a new one
        let second = logue.book(1, 101);
        assert!(matches!(&second, Booking::Founded(..)));
    }
}
