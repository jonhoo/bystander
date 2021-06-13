use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};

const CONTENTION_THRESHOLD: usize = 2;
const RETRY_THRESHOLD: usize = 2;

pub struct Contention;

// in bystander
pub struct ContentionMeasure(usize);
impl ContentionMeasure {
    pub fn detected(&mut self) -> Result<(), Contention> {
        self.0 += 1;
        if self.0 < CONTENTION_THRESHOLD {
            Ok(())
        } else {
            Err(Contention)
        }
    }

    pub fn use_slow_path(&self) -> bool {
        self.0 > CONTENTION_THRESHOLD
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CasState {
    Success,
    Failure,
    Pending,
}

struct CasByRcu<T> {
    version: u64,

    /// The value that will actually be CASed.
    value: T,
}

pub struct Atomic<T>(AtomicPtr<CasByRcu<T>>);

pub trait VersionedCas {
    fn execute(&self, contention: &mut ContentionMeasure) -> Result<bool, Contention>;
    fn has_modified_bit(&self) -> bool;
    fn clear_bit(&self) -> bool;
    fn state(&self) -> CasState;
    fn set_state(&self, new: CasState);
}

impl<T> Atomic<T>
where
    T: PartialEq + Eq,
{
    pub fn new(initial: T) -> Self {
        Self(AtomicPtr::new(Box::into_raw(Box::new(CasByRcu {
            version: 0,
            value: initial,
        }))))
    }

    fn get(&self) -> *mut CasByRcu<T> {
        self.0.load(Ordering::SeqCst)
    }

    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T, u64) -> R,
    {
        // Safety: this is safe because we never deallocate.
        let this = unsafe { &*self.get() };
        f(&this.value, this.version)
    }

    pub fn set(&self, value: T) {
        let this_ptr = self.get();
        // Safety: this is safe because we never deallocate.
        let this = unsafe { &*this_ptr };
        if this.value != value {
            self.0.store(
                Box::into_raw(Box::new(CasByRcu {
                    version: this.version + 1,
                    value,
                })),
                Ordering::SeqCst,
            );
        }
    }

    pub fn compare_and_set(
        &self,
        expected: &T,
        value: T,
        contention: &mut ContentionMeasure,
        version: Option<u64>,
    ) -> Result<bool, Contention> {
        let this_ptr = self.get();
        // Safety: this is safe because we never deallocate.
        let this = unsafe { &*this_ptr };
        if &this.value == expected {
            if let Some(v) = version {
                if v != this.version {
                    contention.detected()?;
                    return Ok(false);
                }
            }

            if expected == &value {
                Ok(true)
            } else {
                let new_ptr = Box::into_raw(Box::new(CasByRcu {
                    version: this.version + 1,
                    value,
                }));
                match self.0.compare_exchange(
                    this_ptr,
                    new_ptr,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => Ok(true),
                    Err(_current) => {
                        // Safety: the Box was never shared.
                        let _ = unsafe { Box::from_raw(new_ptr) };
                        contention.detected()?;
                        Ok(false)
                    }
                }
            }
        } else {
            Ok(false)
        }
    }
}

pub trait NormalizedLockFree {
    type Input: Clone;
    type Output: Clone;
    type CommitDescriptor: Clone;

    fn generator(
        &self,
        op: &Self::Input,
        contention: &mut ContentionMeasure,
    ) -> Result<Self::CommitDescriptor, Contention>;
    fn wrap_up(
        &self,
        executed: Result<(), usize>,
        performed: &Self::CommitDescriptor,
        contention: &mut ContentionMeasure,
    ) -> Result<Option<Self::Output>, Contention>;

    fn fast_path(
        &self,
        op: &Self::Input,
        contention: &mut ContentionMeasure,
    ) -> Result<Self::Output, Contention>;
}

struct OperationRecordBox<LF: NormalizedLockFree> {
    val: AtomicPtr<OperationRecord<LF>>,
}

enum OperationState<LF: NormalizedLockFree> {
    PreCas,
    ExecuteCas(LF::CommitDescriptor),
    PostCas(LF::CommitDescriptor, Result<(), usize>),
    Completed(LF::Output),
}

struct OperationRecord<LF: NormalizedLockFree> {
    owner: std::thread::ThreadId,
    input: LF::Input,
    state: OperationState<LF>,
}

mod help_queue;
use help_queue::HelpQueue;

struct Shared<LF: NormalizedLockFree, const N: usize> {
    algorithm: LF,
    help: HelpQueue<LF, N>,
    free_ids: Mutex<Vec<usize>>,
}

pub struct WaitFreeSimulator<LF: NormalizedLockFree, const N: usize> {
    shared: Arc<Shared<LF, N>>,
    id: usize,
}

pub struct TooManyHandles;
impl<LF: NormalizedLockFree, const N: usize> WaitFreeSimulator<LF, N> {
    pub fn new(algorithm: LF) -> Self {
        assert_ne!(N, 0);
        Self {
            shared: Arc::new(Shared {
                algorithm,
                help: HelpQueue::new(),
                // NOTE: The Self we return has already claimed 0, therefore 1..
                free_ids: Mutex::new((1..N).collect()),
            }),
            id: 0,
        }
    }

    pub fn fork(&self) -> Result<Self, TooManyHandles> {
        if let Some(id) = self.shared.free_ids.lock().unwrap().pop() {
            Ok(Self {
                shared: Arc::clone(&self.shared),
                id,
            })
        } else {
            return Err(TooManyHandles);
        }
    }
}

impl<LF: NormalizedLockFree, const N: usize> Drop for WaitFreeSimulator<LF, N> {
    fn drop(&mut self) {
        self.shared.free_ids.lock().unwrap().push(self.id);
    }
}

enum CasExecuteFailure {
    CasFailed(usize),
    Contention,
}

impl From<Contention> for CasExecuteFailure {
    fn from(_: Contention) -> Self {
        Self::Contention
    }
}

impl<LF: NormalizedLockFree, const N: usize> WaitFreeSimulator<LF, N>
where
    for<'a> &'a LF::CommitDescriptor: IntoIterator<Item = &'a dyn VersionedCas>,
{
    fn cas_execute(
        &self,
        descriptors: &LF::CommitDescriptor,
        contention: &mut ContentionMeasure,
    ) -> Result<(), CasExecuteFailure> {
        for (i, cas) in descriptors.into_iter().enumerate() {
            match cas.state() {
                CasState::Success => {
                    cas.clear_bit();
                }
                CasState::Failure => {
                    return Err(CasExecuteFailure::CasFailed(i));
                }
                CasState::Pending => {
                    cas.execute(contention)?;
                    if cas.has_modified_bit() {
                        // XXX: Paper and code diverge here.
                        cas.set_state(CasState::Success);
                        cas.clear_bit();
                    }
                    if cas.state() != CasState::Success {
                        cas.set_state(CasState::Failure);
                        return Err(CasExecuteFailure::CasFailed(i));
                    }
                }
            }
        }
        Ok(())
    }

    // Guarantees that on return, orb is no longer in help queue.
    fn help_op(&self, orb: &OperationRecordBox<LF>) {
        loop {
            let or = unsafe { &*orb.val.load(Ordering::SeqCst) };
            let updated_or = match &or.state {
                OperationState::Completed(..) => {
                    let _ = self.shared.help.try_remove_front(orb);
                    return;
                }
                OperationState::PreCas => {
                    let cas_list = match self
                        .shared
                        .algorithm
                        .generator(&or.input, &mut ContentionMeasure(0))
                    {
                        Ok(cas_list) => cas_list,
                        Err(Contention) => continue,
                    };
                    Box::new(OperationRecord {
                        owner: or.owner.clone(),
                        input: or.input.clone(),
                        state: OperationState::ExecuteCas(cas_list),
                    })
                }
                OperationState::ExecuteCas(cas_list) => {
                    let outcome = match self.cas_execute(cas_list, &mut ContentionMeasure(0)) {
                        Ok(outcome) => Ok(outcome),
                        Err(CasExecuteFailure::CasFailed(i)) => Err(i),
                        Err(CasExecuteFailure::Contention) => continue,
                    };
                    Box::new(OperationRecord {
                        owner: or.owner.clone(),
                        input: or.input.clone(),
                        state: OperationState::PostCas(cas_list.clone(), outcome),
                    })
                }
                OperationState::PostCas(cas_list, outcome) => {
                    match self.shared.algorithm.wrap_up(
                        *outcome,
                        cas_list,
                        &mut ContentionMeasure(0),
                    ) {
                        Ok(Some(result)) => Box::new(OperationRecord {
                            owner: or.owner.clone(),
                            input: or.input.clone(),
                            state: OperationState::Completed(result),
                        }),
                        Ok(None) => {
                            // We need to re-start from the generator.
                            Box::new(OperationRecord {
                                owner: or.owner.clone(),
                                input: or.input.clone(),
                                state: OperationState::PreCas,
                            })
                        }
                        Err(Contention) => {
                            // Not up to us to re-start.
                            continue;
                        }
                    }
                }
            };
            let updated_or = Box::into_raw(updated_or);

            if orb
                .val
                .compare_exchange_weak(
                    or as *const OperationRecord<_> as *mut OperationRecord<_>,
                    updated_or,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                // Never got shared, so safe to drop.
                let _ = unsafe { Box::from_raw(updated_or) };
            }
        }
    }

    fn help_first(&self) {
        if let Some(help) = self.shared.help.peek() {
            self.help_op(unsafe { &*help });
        }
    }

    pub fn run(&self, op: LF::Input) -> LF::Output {
        let help = /* once in a while */ true;
        if help {
            self.help_first();
        }

        // fast path
        for retry in 0.. {
            let mut contention = ContentionMeasure(0);
            match self.shared.algorithm.fast_path(&op, &mut contention) {
                Ok(result) => return result,
                Err(Contention) => {}
            }

            if retry > RETRY_THRESHOLD {
                break;
            }
        }

        // slow path: ask for help.
        let orb = OperationRecordBox {
            val: AtomicPtr::new(Box::into_raw(Box::new(OperationRecord {
                owner: std::thread::current().id(),
                input: op,
                state: OperationState::PreCas,
            }))),
        };
        self.shared.help.enqueue(self.id, &orb);
        loop {
            let or = unsafe { &*orb.val.load(Ordering::SeqCst) };
            if let OperationState::Completed(t) = &or.state {
                break t.clone();
            } else {
                self.help_first();
            }
        }
    }
}

/*
// in a consuming crate (wait-free-linked-list crate)
pub struct WaitFreeLinkedList<T> {
    simulator: WaitFreeSimulator<LockFreeLinkedList<T>>,
}

struct LockFreeLinkedList<T> {
    t: T,
}

// impl<T> NormalizedLockFree for LockFreeLinkedList<T> {}

impl<T> WaitFreeLinkedList<T> {
    pub fn push_front(&self, t: T) {
        // self.simulator.run(Insert(t))
    }
}
*/
