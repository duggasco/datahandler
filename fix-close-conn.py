#!/usr/bin/env python3
"""
Simple fix: Move close_conn to the beginning of load_to_database
"""

# Read the file
with open('fund_etl_pipeline.py', 'r') as f:
    lines = f.readlines()

# Process line by line
new_lines = []
i = 0
fixed = False

while i < len(lines):
    line = lines[i]
    
    # Look for load_to_database method
    if 'def load_to_database' in line and 'conn=None' in line:
        # Found the method, add it
        new_lines.append(line)
        i += 1
        
        # Skip to the end of docstring
        # First line after def should be the docstring
        if i < len(lines) and '"""' in lines[i]:
            new_lines.append(lines[i])  # Opening """
            i += 1
            
            # Copy until closing """
            while i < len(lines):
                new_lines.append(lines[i])
                if '"""' in lines[i]:
                    i += 1
                    break
                i += 1
        
        # Now add close_conn RIGHT HERE
        new_lines.append('        close_conn = False  # Initialize first to prevent NameError\n')
        new_lines.append('\n')
        fixed = True
        
        # Continue with rest of method, skipping any existing close_conn = False
        while i < len(lines):
            if 'close_conn = False' in lines[i]:
                i += 1
                continue  # Skip it
            
            # Check if we've reached next method
            if lines[i].strip().startswith('def ') and 'load_to_database' not in lines[i]:
                break
                
            new_lines.append(lines[i])
            i += 1
    else:
        new_lines.append(line)
        i += 1

# Write the fixed file
with open('fund_etl_pipeline.py', 'w') as f:
    f.writelines(new_lines)

if fixed:
    print("✓ Fixed: close_conn is now at the beginning of load_to_database")
else:
    print("✗ Could not find load_to_database method to fix")

# Verify syntax
try:
    compile(open('fund_etl_pipeline.py').read(), 'fund_etl_pipeline.py', 'exec')
    print("✓ Syntax check passed")
    print("\nNow run:")
    print("  docker compose down")
    print("  docker compose build fund-etl")
    print("  docker compose up -d")
    print("  ./run-etl.sh validate")
except SyntaxError as e:
    print(f"✗ Syntax error at line {e.lineno}: {e.msg}")
