//! celld-cdc core: turn celld's LTX segments into row-level change events.

pub mod bucket;
pub mod catalog;
pub mod cursor;
pub mod diff;
pub mod event;
pub mod image;
pub mod key;
pub mod lane;
pub mod paged;
pub mod pagediff;
pub mod segment;
pub mod v8value;

pub use celld_ltx;
pub use event::{ChangeEvent, Op};
