import zipfile
import os
import sqlite3

def extract_zip_file(zip_path, extract_dir=None):
    """Extract the zip file and return the path to the database"""
    if extract_dir is None:
        extract_dir = os.path.dirname(zip_path)
    
    print(f"Extracting {zip_path} to {extract_dir}")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
        # Find the SQLite database file
        db_file = None
        for file in zip_ref.namelist():
            if file.endswith('.db'):
                db_file = os.path.join(extract_dir, file)
                break
        
        return db_file

def setup_database(db_path):
    """Set up the database tables"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create attribution_customer_journey table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS attribution_customer_journey (
        conv_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ihc REAL NOT NULL,
        PRIMARY KEY(conv_id, session_id)
    )
    ''')
    
    # Create channel_reporting table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS channel_reporting (
        channel_name TEXT NOT NULL,
        date TEXT NOT NULL,
        cost REAL NOT NULL,
        ihc REAL NOT NULL,
        ihc_revenue REAL NOT NULL,
        PRIMARY KEY(channel_name, date)
    )
    ''')
    
    conn.commit()
    conn.close()
    
    print(f"Database setup complete at {db_path}")

if __name__ == "__main__":
    # Path to the zip file
    zip_path = os.path.join('data', 'challenge.zip')
    
    # Extract the zip file
    db_path = extract_zip_file(zip_path, os.path.join('data'))
    
    if db_path and os.path.exists(db_path):
        print(f"Found database at {db_path}")
        setup_database(db_path)
    else:
        print("Could not find a database file in the zip.")