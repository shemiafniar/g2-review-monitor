import json
import os
import requests
import time
from datetime import datetime

# Configuration from environment variables
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
BRIGHT_DATA_API_KEY = os.environ.get('BRIGHT_DATA_API_KEY')
BRIGHT_DATA_ENDPOINT = os.environ.get('BRIGHT_DATA_ENDPOINT')
STATE_FILE = 'last_review.json'

def load_last_review_id():
    """Load the last review ID from file"""
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('last_review_id', 0)
    except FileNotFoundError:
        return 0

def save_last_review_id(review_id):
    """Save the last review ID to file"""
    with open(STATE_FILE, 'w') as f:
        json.dump({
            'last_review_id': review_id,
            'last_checked': datetime.now().isoformat()
        }, f, indent=2)

def scrape_g2_reviews():
    """Scrape G2 reviews using Bright Data Datasets API - 3 step process"""
    headers = {
        'Authorization': f'Bearer {BRIGHT_DATA_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = [
        {
            "url": "https://www.g2.com/products/bright-data/reviews",
            "sort_filter": "Most Recent",
            "pages": 1
        }
    ]
    
    try:
        # STEP 1: Trigger the collection
        print(f"🌐 Step 1: Triggering Bright Data collection...")
        response = requests.post(BRIGHT_DATA_ENDPOINT, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        trigger_result = response.json()
        
        print(f"✅ Collection triggered: {trigger_result}")
        
        # Get snapshot_id from response
        if isinstance(trigger_result, list) and len(trigger_result) > 0:
            snapshot_id = trigger_result[0].get('snapshot_id')
        else:
            snapshot_id = trigger_result.get('snapshot_id')
        
        if not snapshot_id:
            print("❌ No snapshot_id in response")
            return None
        
        print(f"📸 Snapshot ID: {snapshot_id}")
        
        # STEP 2: Monitor progress until ready
        max_wait = 180  # 3 minutes max
        wait_interval = 10  # Check every 10 seconds
        elapsed = 0
        
        progress_url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
        
        while elapsed < max_wait:
            print(f"⏳ Step 2: Checking progress... ({elapsed}s)")
            time.sleep(wait_interval)
            elapsed += wait_interval
            
            try:
                progress_response = requests.get(progress_url, headers=headers, timeout=30)
                progress_response.raise_for_status()
                progress_data = progress_response.json()
                
                status = progress_data.get('status', 'unknown')
                print(f"   Status: {status}")
                
                if status == 'ready':
                    print(f"✅ Data is ready!")
                    break
                elif status == 'running':
                    print(f"   Still gathering data...")
                    continue
                elif status == 'failed':
                    print(f"❌ Collection failed: {progress_data}")
                    return None
                    
            except Exception as e:
                print(f"⚠️ Error checking progress: {e}")
                continue
        
        if elapsed >= max_wait:
            print("❌ Timeout waiting for data")
            return None
        
        # STEP 3: Download the data
        print(f"📥 Step 3: Downloading data...")
        download_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format=json"
        
        download_response = requests.get(download_url, headers=headers, timeout=30)
        download_response.raise_for_status()
        data = download_response.json()
        
        if data and len(data) > 0:
            print(f"✅ Successfully received {len(data)} reviews")
            return data
        else:
            print("⚠️ No reviews in response")
            return None
        
    except Exception as e:
        print(f"❌ Error in scraping process: {e}")
        import traceback
        traceback.print_exc()
        return None

def send_slack_notification(review):
    """Send notification to Slack via Workflow Webhook"""
    try:
        if not SLACK_WEBHOOK_URL:
            print("❌ SLACK_WEBHOOK_URL not configured")
            return False
        
        # Prepare review text
        first_text = review['text'][0] if review.get('text') and len(review['text']) > 0 else "No review text available"
        
        # Clean up text
        if "Answer:" in first_text:
            first_text = first_text.split("Answer:")[1].strip()
        
        # Truncate if too long
        if len(first_text) > 500:
            first_text = first_text[:500] + "..."
        
        stars_emoji = "⭐" * int(float(review['stars']))
        
        # Prepare payload for Slack workflow
        payload = {
            "review_title": review['title'],
            "review_author": review['author'],
            "review_rating": f"{review['stars']}/5 {stars_emoji}",
            "review_date": review['date'],
            "review_url": review['review_url'],
            "review_text": first_text
        }
        
        print(f"📤 Sending to Slack webhook...")
        print(f"   Review: {review['title']}")
        
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        
        print("✅ Notification sent to Slack successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error sending to Slack: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("=" * 50)
    print("🔍 G2 Review Monitor - Starting Check")
    print("=" * 50)
    
    reviews_data = scrape_g2_reviews()
    
    if not reviews_data or len(reviews_data) == 0:
        print("⚠️ No data retrieved from scraper")
        return
    
    latest_review = reviews_data[0]
    latest_review_id = latest_review['review_id']
    
    print(f"\n📊 Latest review ID: {latest_review_id}")
    print(f"   Title: {latest_review['title']}")
    print(f"   Author: {latest_review['author']}")
    print(f"   Rating: {latest_review['stars']}/5")
    print(f"   Date: {latest_review['date']}")
    
    last_stored_id = load_last_review_id()
    print(f"\n💾 Last stored review ID: {last_stored_id}")
    
    if latest_review_id != last_stored_id:
        print(f"\n🆕 NEW REVIEW DETECTED!")
        
        if send_slack_notification(latest_review):
            save_last_review_id(latest_review_id)
            print("✅ State updated successfully")
        else:
            print("⚠️ Slack notification failed - state not updated")
    else:
        print("\n✨ No new reviews - all caught up!")
    
    print("\n" + "=" * 50)
    print("✅ Check complete")
    print("=" * 50)

if __name__ == '__main__':
    main()
