#!/usr/bin/env python3
"""
Test database connections and dynamic masking settings
Use this to debug why masked_user sees unmasked data in Python
"""

import psycopg2
import sys

DB_CONFIGS = {
    'postgres': {'user': 'postgres', 'password': 'postgres'},
    'masked_user': {'user': 'masked_user', 'password': 'masked'}
}

def test_connection(role):
    """Test connection and check masking settings"""
    print(f"\n{'='*60}")
    print(f"Testing connection as: {role}")
    print(f"{'='*60}")
    
    db_config = DB_CONFIGS[role].copy()
    db_config.update({
        'host': 'localhost',
        'port': 5432,
        'database': 'benchmark'
    })
    
    try:
        # Connect
        conn = psycopg2.connect(**db_config)
        print("✓ Connection successful")
        
        cur = conn.cursor()
        
        # Check 1: PostgreSQL version
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"\nPostgreSQL version: {version[:50]}...")
        
        # Check 2: Extension loaded?
        cur.execute("SELECT extname, extversion FROM pg_extension WHERE extname = 'anon';")
        ext = cur.fetchone()
        if ext:
            print(f"✓ anon extension: {ext[1]}")
        else:
            print("✗ anon extension NOT loaded!")
            return
        
        # Check 3: Dynamic masking setting (session level)
        cur.execute("SHOW anon.transparent_dynamic_masking;")
        masking_setting = cur.fetchone()[0]
        print(f"\nSession setting - anon.transparent_dynamic_masking: {masking_setting}")
        
        if masking_setting != 'on' and role == 'masked_user':
            print("⚠ WARNING: Dynamic masking is OFF!")
            print("  Attempting to enable it...")
            cur.execute("SET anon.transparent_dynamic_masking TO on;")
            conn.commit()
            
            cur.execute("SHOW anon.transparent_dynamic_masking;")
            new_setting = cur.fetchone()[0]
            print(f"  After SET command: {new_setting}")
        
        # Check 4: Role security label
        cur.execute("""
            SELECT rolname,
                   (SELECT label FROM pg_seclabel 
                    WHERE objoid = r.oid 
                    AND provider = 'anon') as security_label
            FROM pg_roles r
            WHERE rolname = %s
        """, (role,))
        role_info = cur.fetchone()
        print(f"\nRole security label: {role_info[1] if role_info else 'None'}")
        
        # Check 5: Can access anon schema?
        cur.execute("SELECT COUNT(*) FROM anon.company LIMIT 1;")
        count = cur.fetchone()[0]
        print(f"✓ Can access anon.company table ({count} rows)")
        
        # Check 6: Test actual query
        print("\n" + "-"*60)
        print("Testing actual query on healthcare_raw_100000:")
        print("-"*60)
        
        cur.execute("SELECT name, age, doctor FROM healthcare_raw_100000 LIMIT 2;")
        rows = cur.fetchall()
        
        for i, row in enumerate(rows, 1):
            print(f"Row {i}: name={row[0]}, age={row[1]}, doctor={row[2]}")
        
        # Check 7: Check masking rules
        print("\n" + "-"*60)
        print("Checking masking rules on healthcare_raw_100000:")
        print("-"*60)
        
        cur.execute("""
            SELECT attname as column_name,
                   (SELECT label FROM pg_seclabel 
                    WHERE objoid = c.oid 
                    AND provider = 'anon' 
                    AND objsubid = a.attnum) as masking_rule
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            WHERE c.relname = 'healthcare_raw_100000'
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND (SELECT label FROM pg_seclabel 
                   WHERE objoid = c.oid 
                   AND provider = 'anon' 
                   AND objsubid = a.attnum) IS NOT NULL;
        """)
        
        rules = cur.fetchall()
        if rules:
            for rule in rules:
                print(f"  {rule[0]}: {rule[1]}")
        else:
            print("  ✗ NO masking rules found on healthcare_raw_100000!")
        
        cur.close()
        conn.close()
        
        print("\n" + "="*60)
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    print("="*60)
    print("PostgreSQL Dynamic Masking Connection Test")
    print("="*60)
    
    # Test postgres user first
    test_connection('postgres')
    
    # Then test masked_user
    test_connection('masked_user')
    
    print("\n" + "="*60)
    print("Test complete!")
    print("="*60)
    print("\nExpected behavior:")
    print("  - postgres: Should see raw data")
    print("  - masked_user: Should see masked data (different names/ages)")
    print("\nIf masked_user sees raw data, dynamic masking is NOT working.")

if __name__ == '__main__':
    main()