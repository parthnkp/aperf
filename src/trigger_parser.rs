use anyhow::{bail, Result};

/// Simple CPU trigger for POC - only supports "cpu > value" format
#[derive(Debug, Clone)]
pub struct CpuTrigger {
    pub threshold: f64,
}

/// Parse simple CPU trigger expression like "cpu > 80"
pub fn parse_cpu_trigger(expression: &str) -> Result<CpuTrigger> {
    let expr = expression.trim().to_lowercase();
    
    // Simple parsing for "cpu > number" format
    if expr.starts_with("cpu") {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() == 3 && parts[0] == "cpu" && parts[1] == ">" {
            let threshold = parts[2].parse::<f64>()
                .map_err(|_| anyhow::anyhow!("Invalid threshold value: {}", parts[2]))?;
            
            if threshold < 0.0 || threshold > 100.0 {
                bail!("CPU threshold must be between 0 and 100, got: {}", threshold);
            }
            
            return Ok(CpuTrigger { threshold });
        }
    }
    
    bail!("Invalid trigger format. Expected 'cpu > <number>' (e.g., 'cpu > 80')");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_condition() {
        let result = parse_trigger_expression("cpu > 80").unwrap();
        match result {
            TriggerCondition::Simple { metric, operator, value } => {
                assert_eq!(metric, "cpu");
                assert_eq!(operator, ComparisonOp::Greater);
                assert_eq!(value, 80.0);
            }
            _ => panic!("Expected simple condition"),
        }
    }

    #[test]
    fn test_and_condition() {
        let result = parse_trigger_expression("cpu > 80 && mem > 50").unwrap();
        match result {
            TriggerCondition::And(left, right) => {
                // Verify left side
                if let TriggerCondition::Simple { metric, operator, value } = left.as_ref() {
                    assert_eq!(metric, "cpu");
                    assert_eq!(*operator, ComparisonOp::Greater);
                    assert_eq!(*value, 80.0);
                } else {
                    panic!("Expected simple condition on left");
                }
                
                // Verify right side
                if let TriggerCondition::Simple { metric, operator, value } = right.as_ref() {
                    assert_eq!(metric, "mem");
                    assert_eq!(*operator, ComparisonOp::Greater);
                    assert_eq!(*value, 50.0);
                } else {
                    panic!("Expected simple condition on right");
                }
            }
            _ => panic!("Expected AND condition"),
        }
    }

    #[test]
    fn test_complex_condition() {
        let result = parse_trigger_expression("(cpu > 80 && mem > 50) || net > 30").unwrap();
        // Just verify it parses without error for now
        println!("Parsed: {}", result);
    }

    #[test]
    fn test_not_condition() {
        let result = parse_trigger_expression("!(swap < 1)").unwrap();
        match result {
            TriggerCondition::Not(inner) => {
                if let TriggerCondition::Simple { metric, operator, value } = inner.as_ref() {
                    assert_eq!(metric, "swap");
                    assert_eq!(*operator, ComparisonOp::Less);
                    assert_eq!(*value, 1.0);
                } else {
                    panic!("Expected simple condition inside NOT");
                }
            }
            _ => panic!("Expected NOT condition"),
        }
    }
}
