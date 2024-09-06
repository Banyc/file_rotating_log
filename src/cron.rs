use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct Cron {
    slot_matcher: SlotMatcher<i16>,
}
impl Cron {
    pub fn new(
        minute: AllowedSet2<i16>,
        hour: AllowedSet2<i16>,
        day_of_month: AllowedSet2<i16>,
        month: AllowedSet2<i16>,
        day_of_week: AllowedSet2<i16>,
    ) -> Self {
        let slot_matcher = SlotMatcher::new(&[minute, hour, day_of_month, month, day_of_week]);
        Self { slot_matcher }
    }

    pub fn edge_triggered_poll(&mut self, now: jiff::Zoned) -> bool {
        let values = [
            now.minute().into(),
            now.hour().into(),
            now.days_in_month().into(),
            now.month().into(),
            (now.weekday() as u8).into(),
            now.year(),
        ];
        self.slot_matcher.edge_triggered_poll(&values)
    }
}

#[derive(Debug, Clone)]
pub struct SlotMatcher<T> {
    cells: Vec<Cell<T>>,
}
impl<T> SlotMatcher<T>
where
    T: Copy + Ord,
{
    pub fn new(allowed: &[AllowedSet2<T>]) -> Self {
        let extra = Cell::new(AllowedSet2::Any);
        let cells = allowed
            .iter()
            .cloned()
            .map(Cell::new)
            .chain([extra])
            .collect();
        Self { cells }
    }

    pub fn edge_triggered_poll(&mut self, values: &[T]) -> bool {
        assert_eq!(self.cells.len(), values.len());
        let is_all_allowed = self
            .cells
            .iter()
            .zip(values.iter())
            .all(|(cell, &value)| cell.is_allowed(value));
        if !is_all_allowed {
            return false;
        }
        let has_all_set = self
            .cells
            .iter()
            .zip(values.iter())
            .all(|(cell, &value)| cell.has_set(value));
        self.cells
            .iter_mut()
            .zip(values.iter())
            .for_each(|(cell, &value)| cell.set(value));
        if has_all_set {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone)]
struct Cell<T> {
    instr: AllowedSet2<T>,
    prev: Option<T>,
}
impl<T> Cell<T>
where
    T: Copy + Ord,
{
    pub fn new(instr: AllowedSet2<T>) -> Self {
        Self { instr, prev: None }
    }

    pub fn has_set(&self, value: T) -> bool {
        self.prev == Some(value)
    }
    pub fn set(&mut self, value: T) {
        self.prev = Some(value);
    }

    pub fn is_allowed(&self, value: T) -> bool {
        self.instr.is_allowed(value)
    }
}

#[derive(Debug, Clone)]
pub enum AllowedSet2<T> {
    Any,
    #[allow(dead_code)]
    Selected(AllowedSet<T>),
}
impl<T> AllowedSet2<T>
where
    T: Copy + Ord,
{
    pub fn is_allowed(&self, value: T) -> bool {
        match self {
            AllowedSet2::Any => true,
            AllowedSet2::Selected(allowed) => allowed.is_allowed(value),
        }
    }

    pub fn from_iter(values: impl Iterator<Item = T>) -> Option<Self> {
        let allowed = BTreeSet::from_iter(values);
        let set = AllowedSet::new(allowed)?;
        Some(Self::Selected(set))
    }
}

#[derive(Debug, Clone)]
pub struct AllowedSet<T> {
    allowed: Vec<T>,
}
impl<T> AllowedSet<T>
where
    T: Copy + Ord,
{
    pub fn new(allowed: BTreeSet<T>) -> Option<Self> {
        if allowed.is_empty() {
            return None;
        }
        let allowed = allowed.iter().copied().collect::<Vec<T>>();
        Some(Self { allowed })
    }

    pub fn is_allowed(&self, value: T) -> bool {
        self.allowed.binary_search(&value).is_ok()
    }

    pub fn next(&self, value: T) -> T {
        for allowed in self.allowed.windows(2) {
            if allowed[0] != value {
                continue;
            }
            return allowed[1];
        }
        self.allowed[0]
    }
}
