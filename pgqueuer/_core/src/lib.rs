use pyo3::prelude::*;

mod core;
mod entrypoint_queue;
mod types;

use core::InMemoryCore;

/// PyO3 module initialization
#[pymodule]
fn _core(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<InMemoryCore>()?;
    Ok(())
}
