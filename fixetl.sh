#!/usr/bin/env python3
"""
Simple fix for 'conn is not defined' in validate_against_lookback
"""

print("Fixing 'conn is not defined' error...")

# Read the file
with open('fund_etl_pipeline.py', 'r') as f:
    content = f.read()

# Find the validate_against_lookback method and add conn = None at the beginning
import re

# Pattern to find the method
pattern = r'(def validate_against_lookback\(self.*?\).*?:\s*\n\s*""".*?"""\s*\n)(.*?)(try:)'

def fix_method(match):
    method_start = match.group(1)  # Method definition and docstring
    between = match.group(2)        # Code between docstring and try
    try_block = match.group(3)      # The try: line
    
    # Add conn = None at the beginning
    fixed = method_start
    fixed += '        conn = None  # Initialize to avoid NameError\n'
    fixed += between
    fixed += try_block
    
    return fixed

# Apply the fix
new_content = re.sub(pattern, fix_method, content, flags=re.DOTALL)

# If the regex didn't work, try a simpler approach
if new_content == content:
    print("Regex didn't match, trying line-by-line approach...")
    
    lines = content.split('\n')
    new_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        new_lines.append(line)
        
        # Look for validate_against_lookback method
        if 'def validate_against_lookback' in line:
            # Skip to end of docstring
            i += 1
            while i < len(lines):
                new_lines.append(lines[i])
                if '"""' in lines[i] and i > 0 and 'def validate_against_lookback' not in lines[i-1]:
                    # This is the closing docstring
                    # Add conn = None on the next line
                    new_lines.append('        conn = None  # Initialize to avoid NameError')
                    print("✓ Added conn = None after docstring")
                    break
                i += 1
        
        i += 1
    
    new_content = '\n'.join(new_lines)

# Write the fixed file
with open('fund_etl_pipeline.py', 'w') as f:
    f.write(new_content)

# Verify the fix
if 'conn = None  # Initialize to avoid NameError' in new_content:
    print("✓ Successfully added conn initialization")
else:
    print("✗ Failed to add conn initialization")
    print("You may need to add it manually")

# Test syntax
try:
    compile(new_content, 'fund_etl_pipeline.py', 'exec')
    print("✓ Syntax check passed")
except SyntaxError as e:
    print(f"✗ Syntax error: {e}")
    exit(1)

print("\nDone! Now rebuild and run:")
print("  docker compose down")
print("  docker compose build fund-etl")
print("  docker compose up -d")
print("  ./run-etl.sh validate")
