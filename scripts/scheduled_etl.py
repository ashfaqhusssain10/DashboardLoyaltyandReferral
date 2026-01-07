"""
Scheduled ETL Runner for Loyalty Data Mart

This script can be run via:
1. Windows Task Scheduler
2. Linux cron
3. GitHub Actions
4. AWS Lambda

Usage:
    python scheduled_etl.py
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'etl_run_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)

def run_etl():
    """Run the full loyalty ETL pipeline."""
    
    # Add Data_Attributes to path
    script_dir = Path(__file__).parent
    data_attr_path = script_dir.parent / 'Data_Attributes'
    sys.path.insert(0, str(data_attr_path))
    
    # Load environment variables from .env
    env_path = script_dir.parent / '.env'
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
        logger.info(f"Loaded environment from {env_path}")
    
    try:
        # Import and run ETL
        from loyalty_etl import LoyaltyETL
        
        logger.info("=" * 60)
        logger.info("SCHEDULED ETL RUN STARTED")
        logger.info(f"Time: {datetime.now().isoformat()}")
        logger.info("=" * 60)
        
        etl = LoyaltyETL()
        etl.run_full_pipeline()
        
        logger.info("=" * 60)
        logger.info("SCHEDULED ETL RUN COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"ETL FAILED: {str(e)}", exc_info=True)
        return False


def send_notification(success: bool, message: str = ""):
    """
    Optional: Send notification on completion/failure.
    Implement your preferred notification method here.
    """
    # Example: Slack webhook, email, SNS, etc.
    pass


if __name__ == "__main__":
    success = run_etl()
    
    if success:
        logger.info("ETL completed successfully")
        sys.exit(0)
    else:
        logger.error("ETL failed")
        sys.exit(1)
