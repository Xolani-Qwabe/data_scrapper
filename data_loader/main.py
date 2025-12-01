# main.py (Use Original Schema)
from database_setup import create_database
from nba_database import create_tables  # Use original nba_database
from nba_data_loader import NBADataLoader  # Use original data loader

def main():
    # Step 1: Create database if it doesn't exist
    print("Step 1: Checking database...")
    create_database()
    
    # Step 2: Create database tables
    print("\nStep 2: Creating database tables...")
    create_tables()
    
    # Step 3: Load data
    print("\nStep 3: Loading data into database...")
    loader = NBADataLoader("./nba_data")
    
    try:
        loader.load_all_data()
    except Exception as e:
        print(f"❌ Error loading data: {e}")
    finally:
        loader.close()
    
    print("🎉 NBA Database setup completed!")

if __name__ == "__main__":
    main()