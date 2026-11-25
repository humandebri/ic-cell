//! Caller allowlist persisted in a dedicated stable-memory region.
//!
//! The allowlist deliberately lives *outside* the SQLite image: LTX
//! replication overwrites raw SQLite pages, so anything stored in SQLite could
//! be replaced by a replicated snapshot.
//!
//! Policy: controllers are always authorized; any other principal must be in
//! the allowlist. An empty allowlist authorizes nobody but controllers.

use candid::Principal;
use std::cell::RefCell;
use std::collections::BTreeSet;

const MAGIC: [u8; 4] = *b"ICAU";
const MAX_PRINCIPAL_LEN: usize = 29;
pub const MAX_AUTHORIZED_CALLERS: usize = 1024;

thread_local! {
    static AUTHORIZED_CALLERS: RefCell<BTreeSet<Principal>> = const { RefCell::new(BTreeSet::new()) };
}

pub fn contains(principal: &Principal) -> bool {
    AUTHORIZED_CALLERS.with(|set| set.borrow().contains(principal))
}

pub fn list() -> Vec<Principal> {
    AUTHORIZED_CALLERS.with(|set| set.borrow().iter().copied().collect())
}

/// Returns `true` if the principal was newly inserted.
pub fn insert(principal: Principal) -> Result<bool, String> {
    AUTHORIZED_CALLERS.with(|set| {
        let mut set = set.borrow_mut();
        if set.contains(&principal) {
            return Ok(false);
        }
        if set.len() >= MAX_AUTHORIZED_CALLERS {
            return Err(format!(
                "allowlist is full ({MAX_AUTHORIZED_CALLERS} principals)"
            ));
        }
        Ok(set.insert(principal))
    })
}

/// Returns `true` if the principal was present.
pub fn remove(principal: &Principal) -> bool {
    AUTHORIZED_CALLERS.with(|set| set.borrow_mut().remove(principal))
}

pub fn clear_for_tests() {
    AUTHORIZED_CALLERS.with(|set| set.borrow_mut().clear());
}

/// Layout: `MAGIC (4) | count u32 LE | { len u8 | bytes[len] }*`.
pub fn encode() -> Vec<u8> {
    AUTHORIZED_CALLERS.with(|set| {
        let set = set.borrow();
        let mut buf = Vec::with_capacity(8 + set.len() * (1 + MAX_PRINCIPAL_LEN));
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&(set.len() as u32).to_le_bytes());
        for p in set.iter() {
            let bytes = p.as_slice();
            buf.push(bytes.len() as u8);
            buf.extend_from_slice(bytes);
        }
        buf
    })
}

/// Restores the allowlist. Returns `false` (leaving the set untouched) when the
/// buffer does not hold a valid encoding, e.g. a fresh memory region.
pub fn decode_and_restore(buf: &[u8]) -> bool {
    let Some(set) = decode(buf) else {
        return false;
    };
    AUTHORIZED_CALLERS.with(|s| *s.borrow_mut() = set);
    true
}

fn decode(buf: &[u8]) -> Option<BTreeSet<Principal>> {
    if buf.len() < 8 || buf[0..4] != MAGIC {
        return None;
    }
    let count = u32::from_le_bytes(buf[4..8].try_into().ok()?) as usize;
    if count > MAX_AUTHORIZED_CALLERS {
        return None;
    }
    let mut set = BTreeSet::new();
    let mut pos = 8;
    for _ in 0..count {
        let len = *buf.get(pos)? as usize;
        pos += 1;
        if len > MAX_PRINCIPAL_LEN {
            return None;
        }
        let bytes = buf.get(pos..pos + len)?;
        pos += len;
        set.insert(Principal::try_from_slice(bytes).ok()?);
    }
    Some(set)
}

/// Upper bound of the encoded size, used to size the stable-memory region.
pub fn max_encoded_len() -> usize {
    8 + MAX_AUTHORIZED_CALLERS * (1 + MAX_PRINCIPAL_LEN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_reject_garbage() {
        clear_for_tests();
        let a = Principal::from_slice(&[1, 2, 3]);
        let b = Principal::management_canister();
        assert!(insert(a).unwrap());
        assert!(!insert(a).unwrap());
        assert!(insert(b).unwrap());
        let encoded = encode();

        clear_for_tests();
        assert!(!decode_and_restore(&[0u8; 64]));
        assert!(list().is_empty());
        assert!(decode_and_restore(&encoded));
        assert!(contains(&a) && contains(&b));

        // Truncated buffers are rejected rather than partially applied.
        clear_for_tests();
        assert!(!decode_and_restore(&encoded[..encoded.len() - 1]));
        assert!(list().is_empty());
    }
}
