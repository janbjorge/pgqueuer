use pyo3::prelude::*;

mod core;
mod entrypoint_queue;
mod types;

use core::InMemoryCore;

/// PyO3 module initialization
#[pymodule(name = "core_rs")]
fn core_rs(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<InMemoryCore>()?;
    Ok(())
}
