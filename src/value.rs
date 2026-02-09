//! Unified value types for the Diamond Types Extended API.
//!
//! This module provides the public value types:
//! - [`Value`] — read values (primitives + CRDT references)
//! - [`PrimitiveValue`] — write values (primitives only, used in set/add/remove)
//! - [`MaterializedValue`] — fully resolved document tree from checkout()
//! - [`CrdtId`] — opaque handle to nested CRDTs
//! - [`Conflicted`] — wrapper for values with concurrent conflicts

use std::collections::BTreeMap;
use std::fmt;

use crate::{CRDTKind, CreateValue, DTValue, LV, Primitive, RegisterValue};
use smartstring::alias::String as SmartString;

/// Opaque handle to a nested CRDT within a Document.
///
/// This handle can be used to navigate to nested CRDTs (maps, text, sets, registers)
/// without exposing internal implementation details.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct CrdtId(pub(crate) LV);

impl CrdtId {
    /// Get the internal LV (for debugging/testing).
    #[doc(hidden)]
    pub fn as_lv(&self) -> LV {
        self.0
    }
}

/// Unified value type for reading from CRDTs.
///
/// This enum represents all values that can be read from CRDTs.
/// Primitive variants contain data directly; CRDT variants contain
/// opaque [`CrdtId`] handles for navigation.
///
/// For writing primitive values, use [`PrimitiveValue`] instead —
/// it prevents accidentally passing CRDT references to `set()`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Value {
    /// Nil/null value.
    Nil,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// String value (UTF-8).
    Str(String),
    /// Reference to a nested Map CRDT.
    Map(CrdtId),
    /// Reference to a nested Text CRDT.
    Text(CrdtId),
    /// Reference to a nested Set CRDT.
    Set(CrdtId),
    /// Reference to a nested Register CRDT.
    Register(CrdtId),
}

impl Value {
    /// Check if this value is nil.
    pub fn is_nil(&self) -> bool {
        matches!(self, Value::Nil)
    }

    /// Check if this value is a primitive (not a CRDT reference).
    pub fn is_primitive(&self) -> bool {
        matches!(self, Value::Nil | Value::Bool(_) | Value::Int(_) | Value::Str(_))
    }

    /// Check if this value is a CRDT reference.
    pub fn is_crdt(&self) -> bool {
        matches!(self, Value::Map(_) | Value::Text(_) | Value::Set(_) | Value::Register(_))
    }

    /// Get as bool if this is a boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Get as i64 if this is an integer.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// Get as str if this is a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Get the CRDT ID if this is a CRDT reference.
    pub fn crdt_id(&self) -> Option<CrdtId> {
        match self {
            Value::Map(id) | Value::Text(id) | Value::Set(id) | Value::Register(id) => Some(*id),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Nil => write!(f, "nil"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(n) => write!(f, "{}", n),
            Value::Str(s) => write!(f, "{:?}", s),
            Value::Map(id) => write!(f, "Map({:?})", id),
            Value::Text(id) => write!(f, "Text({:?})", id),
            Value::Set(id) => write!(f, "Set({:?})", id),
            Value::Register(id) => write!(f, "Register({:?})", id),
        }
    }
}

// ============ PrimitiveValue — type-safe write input ============

/// Primitive value type for write operations.
///
/// This is the input type for `MapMut::set()`, `SetMut::add()`, and
/// `SetMut::remove()`. Unlike [`Value`], it cannot contain CRDT references,
/// so the type system prevents passing a Map/Text/Set/Register handle
/// where a primitive is expected.
///
/// Use the `create_map()`, `create_text()`, etc. methods to create
/// nested CRDTs instead.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PrimitiveValue {
    /// Nil/null value.
    Nil,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// String value (UTF-8).
    Str(String),
}

impl From<bool> for PrimitiveValue {
    fn from(b: bool) -> Self { PrimitiveValue::Bool(b) }
}

impl From<i64> for PrimitiveValue {
    fn from(n: i64) -> Self { PrimitiveValue::Int(n) }
}

impl From<i32> for PrimitiveValue {
    fn from(n: i32) -> Self { PrimitiveValue::Int(n as i64) }
}

impl From<String> for PrimitiveValue {
    fn from(s: String) -> Self { PrimitiveValue::Str(s) }
}

impl From<&str> for PrimitiveValue {
    fn from(s: &str) -> Self { PrimitiveValue::Str(s.to_string()) }
}

impl From<()> for PrimitiveValue {
    fn from(_: ()) -> Self { PrimitiveValue::Nil }
}

impl From<PrimitiveValue> for Primitive {
    fn from(v: PrimitiveValue) -> Self {
        match v {
            PrimitiveValue::Nil => Primitive::Nil,
            PrimitiveValue::Bool(b) => Primitive::Bool(b),
            PrimitiveValue::Int(n) => Primitive::I64(n),
            PrimitiveValue::Str(s) => Primitive::Str(s.into()),
        }
    }
}

impl From<PrimitiveValue> for CreateValue {
    fn from(v: PrimitiveValue) -> Self {
        CreateValue::Primitive(v.into())
    }
}

impl From<PrimitiveValue> for Value {
    fn from(v: PrimitiveValue) -> Self {
        match v {
            PrimitiveValue::Nil => Value::Nil,
            PrimitiveValue::Bool(b) => Value::Bool(b),
            PrimitiveValue::Int(n) => Value::Int(n),
            PrimitiveValue::Str(s) => Value::Str(s),
        }
    }
}

// ============ MaterializedValue — checkout output ============

/// Fully resolved document tree value.
///
/// Returned by `Document::checkout()` to represent the entire document state
/// without exposing internal types. Unlike [`Value`], this enum contains
/// the actual nested data rather than CRDT handles.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MaterializedValue {
    /// Nil/null value.
    Nil,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// String value (UTF-8).
    Str(String),
    /// Text CRDT content.
    Text(String),
    /// Nested map with string keys.
    Map(BTreeMap<String, MaterializedValue>),
    /// OR-Set containing primitive values.
    Set(Vec<PrimitiveValue>),
}

impl From<DTValue> for MaterializedValue {
    fn from(dt: DTValue) -> Self {
        match dt {
            DTValue::Primitive(p) => match p {
                Primitive::Nil | Primitive::InvalidUninitialized => MaterializedValue::Nil,
                Primitive::Bool(b) => MaterializedValue::Bool(b),
                Primitive::I64(n) => MaterializedValue::Int(n),
                Primitive::Str(s) => MaterializedValue::Str(s.to_string()),
            },
            DTValue::Register(inner) => (*inner).into(),
            DTValue::Text(s) => MaterializedValue::Text(s),
            DTValue::Map(entries) => MaterializedValue::Map(
                entries.into_iter()
                    .map(|(k, v)| (k.to_string(), (*v).into()))
                    .collect()
            ),
            DTValue::Set(members) => MaterializedValue::Set(
                members.into_iter()
                    .map(|p| match p {
                        Primitive::Nil | Primitive::InvalidUninitialized => PrimitiveValue::Nil,
                        Primitive::Bool(b) => PrimitiveValue::Bool(b),
                        Primitive::I64(n) => PrimitiveValue::Int(n),
                        Primitive::Str(s) => PrimitiveValue::Str(s.to_string()),
                    })
                    .collect()
            ),
        }
    }
}

pub(crate) fn checkout_to_materialized(
    raw: BTreeMap<SmartString, Box<DTValue>>
) -> BTreeMap<String, MaterializedValue> {
    raw.into_iter()
        .map(|(k, v)| (k.to_string(), (*v).into()))
        .collect()
}

// ============ Conversions from Rust types to Value (read type) ============

impl From<bool> for Value {
    fn from(b: bool) -> Self { Value::Bool(b) }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self { Value::Int(n) }
}

impl From<i32> for Value {
    fn from(n: i32) -> Self { Value::Int(n as i64) }
}

impl From<String> for Value {
    fn from(s: String) -> Self { Value::Str(s) }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self { Value::Str(s.to_string()) }
}

impl From<()> for Value {
    fn from(_: ()) -> Self { Value::Nil }
}

// ============ Internal conversions ============

impl From<Primitive> for Value {
    fn from(p: Primitive) -> Self {
        match p {
            Primitive::Nil => Value::Nil,
            Primitive::Bool(b) => Value::Bool(b),
            Primitive::I64(n) => Value::Int(n),
            Primitive::Str(s) => Value::Str(s.to_string()),
            Primitive::InvalidUninitialized => Value::Nil,
        }
    }
}

impl From<RegisterValue> for Value {
    fn from(rv: RegisterValue) -> Self {
        match rv {
            RegisterValue::Primitive(p) => p.into(),
            RegisterValue::OwnedCRDT(kind, lv) => {
                let id = CrdtId(lv);
                match kind {
                    CRDTKind::Map => Value::Map(id),
                    CRDTKind::Text => Value::Text(id),
                    CRDTKind::Set => Value::Set(id),
                    CRDTKind::Register => Value::Register(id),
                    CRDTKind::Collection => Value::Map(id),
                }
            }
        }
    }
}

impl From<Value> for Primitive {
    fn from(v: Value) -> Self {
        match v {
            Value::Nil => Primitive::Nil,
            Value::Bool(b) => Primitive::Bool(b),
            Value::Int(n) => Primitive::I64(n),
            Value::Str(s) => Primitive::Str(s.into()),
            _ => Primitive::Nil,
        }
    }
}

impl From<Value> for CreateValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Nil => CreateValue::Primitive(Primitive::Nil),
            Value::Bool(b) => CreateValue::Primitive(Primitive::Bool(b)),
            Value::Int(n) => CreateValue::Primitive(Primitive::I64(n)),
            Value::Str(s) => CreateValue::Primitive(Primitive::Str(s.into())),
            Value::Map(_) => CreateValue::NewCRDT(CRDTKind::Map),
            Value::Text(_) => CreateValue::NewCRDT(CRDTKind::Text),
            Value::Set(_) => CreateValue::NewCRDT(CRDTKind::Set),
            Value::Register(_) => CreateValue::NewCRDT(CRDTKind::Register),
        }
    }
}

// ============ Conflicted<T> ============

/// Wrapper for values that may have concurrent conflicts.
///
/// In LWW (Last-Writer-Wins) semantics, concurrent writes result in one
/// value "winning" deterministically. The losing values are preserved
/// in `conflicts` for applications that want custom merge strategies.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Conflicted<T> {
    /// The "winning" value according to LWW semantics.
    pub value: T,
    /// Concurrent values that lost the tie-break.
    pub conflicts: Vec<T>,
}

impl<T> Conflicted<T> {
    /// Create a new Conflicted with no conflicts.
    pub fn new(value: T) -> Self {
        Self {
            value,
            conflicts: Vec::new(),
        }
    }

    /// Create a new Conflicted with conflicts.
    pub fn with_conflicts(value: T, conflicts: Vec<T>) -> Self {
        Self { value, conflicts }
    }

    /// Check if there are any conflicts.
    pub fn has_conflicts(&self) -> bool {
        !self.conflicts.is_empty()
    }

    /// Get all values (winning + conflicts) as an iterator.
    pub fn all_values(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&self.value).chain(self.conflicts.iter())
    }

    /// Map the value and conflicts through a function.
    pub fn map<U, F: Fn(&T) -> U>(&self, f: F) -> Conflicted<U> {
        Conflicted {
            value: f(&self.value),
            conflicts: self.conflicts.iter().map(f).collect(),
        }
    }

    /// Extract the winning value, discarding conflicts.
    pub fn into_value(self) -> T {
        self.value
    }

    /// Extract all values (winning + conflicts).
    pub fn into_all_values(self) -> Vec<T> {
        let mut all = vec![self.value];
        all.extend(self.conflicts);
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_primitives() {
        assert!(Value::Nil.is_nil());
        assert!(Value::Nil.is_primitive());
        assert!(!Value::Nil.is_crdt());

        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Int(42).as_int(), Some(42));
        assert_eq!(Value::Str("hello".into()).as_str(), Some("hello"));
    }

    #[test]
    fn test_value_conversions() {
        let v: Value = true.into();
        assert_eq!(v, Value::Bool(true));

        let v: Value = 42i64.into();
        assert_eq!(v, Value::Int(42));

        let v: Value = "hello".into();
        assert_eq!(v, Value::Str("hello".into()));
    }

    #[test]
    fn test_primitive_value_conversions() {
        let v: PrimitiveValue = true.into();
        assert_eq!(v, PrimitiveValue::Bool(true));

        let v: PrimitiveValue = 42i64.into();
        assert_eq!(v, PrimitiveValue::Int(42));

        let v: PrimitiveValue = "hello".into();
        assert_eq!(v, PrimitiveValue::Str("hello".into()));

        let v: PrimitiveValue = ().into();
        assert_eq!(v, PrimitiveValue::Nil);
    }

    #[test]
    fn test_conflicted() {
        let c = Conflicted::new(42);
        assert!(!c.has_conflicts());
        assert_eq!(c.all_values().count(), 1);

        let c = Conflicted::with_conflicts(42, vec![43, 44]);
        assert!(c.has_conflicts());
        assert_eq!(c.all_values().collect::<Vec<_>>(), vec![&42, &43, &44]);
    }

    #[test]
    fn test_conflicted_into_value_no_clone() {
        // Verify into_value works without Clone bound
        let c = Conflicted::new(String::from("hello"));
        let val = c.into_value();
        assert_eq!(val, "hello");
    }
}
