"""
Configuration Validation Script

Run this script to validate your config.yaml changes BEFORE running the pipeline.
Checks for syntax errors, invalid values, and provides warnings.

Usage:
    python validate_config.py
"""

import yaml
import sys
from pathlib import Path
from typing import Dict, List, Tuple


def validate_config() -> Tuple[bool, List[str], List[str]]:
    """
    Validate config.yaml file.
    
    Returns:
        Tuple of (is_valid, errors, warnings)
    """
    errors = []
    warnings = []
    
    config_path = Path(__file__).parent / "config" / "config.yaml"
    
    # Check file exists
    if not config_path.exists():
        errors.append(f"Config file not found: {config_path}")
        return False, errors, warnings
    
    # Load YAML
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        errors.append(f"YAML syntax error: {str(e)}")
        return False, errors, warnings
    except Exception as e:
        errors.append(f"Failed to read config: {str(e)}")
        return False, errors, warnings
    
    print("✓ YAML syntax is valid")
    
    # Validate PM Timing
    if 'pm_timing' in config:
        pm_timing = config['pm_timing']
        
        early = pm_timing.get('early_threshold')
        on_time_min = pm_timing.get('on_time_min')
        on_time_max = pm_timing.get('on_time_max')
        late = pm_timing.get('late_threshold')
        overdue = pm_timing.get('overdue_threshold')
        
        # Check all values present
        if None in [early, on_time_min, on_time_max, late, overdue]:
            errors.append("PM timing thresholds: Missing required values")
        else:
            # Check early_threshold matches on_time_min
            if early != on_time_min:
                warnings.append(
                    f"PM timing: early_threshold ({early}) should match on_time_min ({on_time_min})"
                )
            
            # Check on_time_max matches late_threshold
            if on_time_max != late:
                warnings.append(
                    f"PM timing: on_time_max ({on_time_max}) should match late_threshold ({late})"
                )
            
            # Check logical order
            if not (early < on_time_min <= on_time_max < late < overdue):
                errors.append(
                    f"PM timing thresholds not in logical order: "
                    f"early ({early}) < on_time_min ({on_time_min}) <= on_time_max ({on_time_max}) "
                    f"< late ({late}) < overdue ({overdue})"
                )
            else:
                print(f"✓ PM timing thresholds are valid: {early}% / {on_time_max}% / {late}% / {overdue}%")
    else:
        errors.append("Missing pm_timing section")
    
    # Validate Chronic Tools
    if 'chronic_tools' in config:
        chronic = config['chronic_tools']
        
        # Check threshold values
        if 'chronic_tool_threshold' in chronic:
            threshold = chronic['chronic_tool_threshold']
            
            unscheduled_rate = threshold.get('unscheduled_pm_rate')
            variance = threshold.get('pm_life_variance')
            min_events = threshold.get('min_pm_events')
            
            # Check unscheduled_pm_rate
            if unscheduled_rate is not None:
                if not (0 <= unscheduled_rate <= 1):
                    errors.append(
                        f"Chronic tools unscheduled_pm_rate must be 0-1, got {unscheduled_rate}"
                    )
                elif unscheduled_rate > 0.5:
                    warnings.append(
                        f"Chronic tools unscheduled_pm_rate is very high ({unscheduled_rate*100:.0f}%). "
                        f"This may flag too few tools as chronic."
                    )
                elif unscheduled_rate < 0.2:
                    warnings.append(
                        f"Chronic tools unscheduled_pm_rate is very low ({unscheduled_rate*100:.0f}%). "
                        f"This may flag too many tools as chronic."
                    )
                else:
                    print(f"✓ Chronic unscheduled_pm_rate: {unscheduled_rate*100:.0f}%")
            
            # Check pm_life_variance
            if variance is not None:
                if not (0 <= variance <= 2):
                    errors.append(
                        f"Chronic tools pm_life_variance should be 0-2, got {variance}"
                    )
                elif variance > 1:
                    warnings.append(
                        f"Chronic tools pm_life_variance is very high ({variance*100:.0f}%). "
                        f"This may flag too few tools as chronic."
                    )
                elif variance < 0.2:
                    warnings.append(
                        f"Chronic tools pm_life_variance is very low ({variance*100:.0f}%). "
                        f"This may flag too many tools as chronic."
                    )
                else:
                    print(f"✓ Chronic pm_life_variance: {variance*100:.0f}%")
            
            # Check min_pm_events
            if min_events is not None:
                if min_events < 1:
                    errors.append(f"Chronic tools min_pm_events must be >= 1, got {min_events}")
                elif min_events < 3:
                    warnings.append(
                        f"Chronic tools min_pm_events is very low ({min_events}). "
                        f"Consider at least 5 for statistical reliability."
                    )
                elif min_events > 20:
                    warnings.append(
                        f"Chronic tools min_pm_events is very high ({min_events}). "
                        f"Many tools may be excluded from analysis."
                    )
                else:
                    print(f"✓ Chronic min_pm_events: {min_events}")
        
        # Check score weights
        if 'score_weights' in chronic:
            weights = chronic['score_weights']
            
            unscheduled_w = weights.get('unscheduled_pm_rate', 0)
            variance_w = weights.get('pm_life_variance', 0)
            downtime_w = weights.get('downtime_hours', 0)
            reclean_w = weights.get('reclean_rate', 0)
            sympathy_w = weights.get('sympathy_pm_rate', 0)
            
            total_weight = unscheduled_w + variance_w + downtime_w + reclean_w + sympathy_w
            
            if abs(total_weight - 1.0) > 0.01:
                errors.append(
                    f"Chronic score weights must total 1.0, got {total_weight:.2f}"
                )
            else:
                print(f"✓ Chronic score weights total: {total_weight:.2f}")
        
        # Check severity thresholds
        if 'severity_thresholds' in chronic:
            severity = chronic['severity_thresholds']
            
            low = severity.get('low')
            medium = severity.get('medium')
            high = severity.get('high')
            critical = severity.get('critical')
            
            if None not in [low, medium, high, critical]:
                if not (0 < low < medium < high < critical <= 100):
                    errors.append(
                        f"Chronic severity thresholds not in order: "
                        f"0 < low ({low}) < medium ({medium}) < high ({high}) "
                        f"< critical ({critical}) <= 100"
                    )
                else:
                    print(f"✓ Chronic severity thresholds: {low} / {medium} / {high} / {critical}")
    else:
        errors.append("Missing chronic_tools section")
    
    # Validate Retention
    if 'retention' in config:
        retention = config['retention']
        
        copper = retention.get('copper_weeks')
        silver = retention.get('silver_weeks')
        gold = retention.get('gold_weeks')
        
        if None not in [copper, silver, gold]:
            if copper < 1 or silver < 1 or gold < 1:
                errors.append("Retention weeks must be >= 1")
            elif copper > silver or silver > gold:
                warnings.append(
                    f"Retention: Gold ({gold}) should be >= Silver ({silver}) >= Copper ({copper})"
                )
            else:
                print(f"✓ Retention periods: Copper={copper}w, Silver={silver}w, Gold={gold}w")
    
    # Validate Data Quality
    if 'data_quality' in config:
        dq = config['data_quality']
        
        max_null = dq.get('max_null_pct')
        tolerance = dq.get('row_count_tolerance')
        
        if max_null is not None:
            if not (0 <= max_null <= 100):
                errors.append(f"max_null_pct must be 0-100, got {max_null}")
            else:
                print(f"✓ Data quality max_null_pct: {max_null}%")
        
        if tolerance is not None:
            if not (0 <= tolerance <= 1):
                errors.append(f"row_count_tolerance must be 0-1, got {tolerance}")
            else:
                print(f"✓ Data quality row_count_tolerance: {tolerance*100:.0f}%")
    
    # Summary
    is_valid = len(errors) == 0
    
    return is_valid, errors, warnings


def main():
    """Run validation and print results."""
    print("=" * 60)
    print("PM Flex Configuration Validation")
    print("=" * 60)
    print()
    
    is_valid, errors, warnings = validate_config()
    
    print()
    print("-" * 60)
    
    # Print warnings
    if warnings:
        print(f"\n⚠️  WARNINGS ({len(warnings)}):")
        for i, warning in enumerate(warnings, 1):
            print(f"  {i}. {warning}")
    
    # Print errors
    if errors:
        print(f"\n❌ ERRORS ({len(errors)}):")
        for i, error in enumerate(errors, 1):
            print(f"  {i}. {error}")
        print("\nPlease fix errors before running the pipeline.")
        sys.exit(1)
    
    # Success
    if not warnings:
        print("\n✅ Configuration is valid - no errors or warnings!")
    else:
        print(f"\n✅ Configuration is valid - {len(warnings)} warning(s)")
    
    print("\nYou can now run the pipeline:")
    print("  python run_etl_pipeline.py")
    print()
    
    sys.exit(0)


if __name__ == "__main__":
    main()
