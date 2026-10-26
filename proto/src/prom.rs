//! Prometheus' text format: every program's /metrics uses this.

use std::fmt::Write;

/// One metric: help line, type line, value.
pub fn write_metric(out: &mut String, name: &str, kind: &str, help: &str, value: f64) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}");
}
