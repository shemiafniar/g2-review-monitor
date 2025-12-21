import json
import os
import requests
import time
from datetime import datetime, timedelta

# Configuration from environment variables
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
BRIGHT_DATA_API_KEY = os.environ.get('BRIGHT_DATA_API_KEY')
BRIGHT_DATA_ENDPOINT = os.environ.get('BRIGHT_DATA_ENDPOINT')
STATE_FILE = 'last_review.json'

def validate_secrets():
    """Validate all required secrets are present"""
    missing = []
    
    if not SLACK_WEBHOOK_URL:
        missing.append("SLACK_WEBHOOK_URL")
    if not BRIGHT_DATA_API_KEY:
        missing.append("BRIGHT_DATA_API_KEY")
    if not BRIGHT_DATA_ENDPOINT:
        missing.append("BRIGHT_DATA_ENDPOINT")
    
    if missing:
        error_msg = f"Missing required secrets: {', '.join(missing)}"
        print(f"❌ {error_msg}")
        return False
    
    print("✅ All secrets validated")
    return True

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
            'last_checked': datetime.utcnow().isoformat(),
            'last_notification_sent': datetime.utcnow().isoformat()
        }, f, indent=2)

def get_last_notification_time():
    """Get the last time a notification was sent"""
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            last_notif = data.get('last_notification_sent')
            if last_notif:
                return datetime.fromisoformat(last_notif)
    except:
        pass
    return None

def should_run_check():
    """Check if enough time has passed since last check"""
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            last_checked = data.get('last_checked')
            if last_checked:
                last_time = datetime.fromisoformat(last_checked)
                now = datetime.utcnow()
                hours_since = (now - last_time).total_seconds() / 3600
                
                if hours_since < 0.5:  # Less than 30 minutes since last check
                    print(f"⏭️ Skipping - last checked {hours_since:.1f} hours ago (too recent)")
                    return False
    except:
        pass
    
    return True

def is_review_recent(review, days=30):
    """Check if review is from the last X days"""
    try:
        review_date = datetime.strptime(review['date'], '%Y-%m-%d')
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        return review_date >= cutoff_date
    except:
        return True  # If can't parse date, assume it's valid

def scrape_g2_reviews():
    """Scrape G2 reviews using Bright Data Datasets API"""
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
        print(f"🌐 Step 1: Triggering Bright Data collection...")
        response = requests.post(BRIGHT_DATA_ENDPOINT, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        trigger_result = response.json()
        
        print(f"✅ Collection triggered successfully")
        
        if isinstance(trigger_result, list) and len(trigger_result) > 0:
            snapshot_id = trigger_result[0].get('snapshot_id')
        else:
            snapshot_id = trigger_result.get('snapshot_id')
        
        if not snapshot_id:
            print("❌ No snapshot_id in response")
            return None
        
        print(f"📸 Snapshot ID: {snapshot_id}")
        
        max_wait = 180
        wait_interval = 10
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
                if elapsed >= max_wait - wait_interval:
                    return None
                continue
        
        if elapsed >= max_wait:
            print("❌ Timeout waiting for data")
            return None
        
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
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error in scraping process: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error in scraping process: {e}")
        import traceback
        traceback.print_exc()
        return None

def send_slack_notification(review, max_retries=3):
    """Send notification to Slack with retry logic"""
    for attempt in range(1, max_retries + 1):
        try:
            if not SLACK_WEBHOOK_URL:
                print("❌ SLACK_WEBHOOK_URL not configured")
                return False
            
            first_text = review['text'][0] if review.get('text') and len(review['text']) > 0 else "No review text available"
            
            if "Answer:" in first_text:
                first_text = first_text.split("Answer:")[1].strip()
            
            if len(first_text) > 500:
                first_text = first_text[:500] + "..."
            
            stars_emoji = "⭐" * int(float(review['stars']))
            
            payload = {
                "review_title": review['title'],
                "review_author": review['author'],
                "review_rating": f"{review['stars']}/5 {stars_emoji}",
                "review_date": review['date'],
                "review_url": review['review_url'],
                "review_text": first_text
            }
            
            print(f"📤 Sending to Slack: {review['title']} (Attempt {attempt}/{max_retries})")
            
            response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
            response.raise_for_status()
            
            print("✅ Notification sent successfully")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error on attempt {attempt}: {e}")
            if attempt < max_retries:
                wait_time = attempt * 5
                print(f"   Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed after {max_retries} attempts")
                return False
        except Exception as e:
            print(f"❌ Unexpected error sending to Slack: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    return False

def send_error_notification(error_message):
    """Send error notification to Slack"""
    try:
        if not SLACK_WEBHOOK_URL:
            return False
        
        payload = {
            "review_title": "⚠️ G2 Review Monitor - Error Alert",
            "review_author": "Automation System",
            "review_rating": "⚠️ Error",
            "review_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
            "review_url": "https://github.com/shemiafniar/g2-review-monitor/actions",
            "review_text": f"The G2 review monitoring script encountered an error:\n\n{error_message}\n\nPlease check the GitHub Actions logs for details."
        }
        
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except:
        return False

def send_health_check():
    """Send weekly health check if no notifications sent recently"""
    last_notif_time = get_last_notification_time()
    
    if last_notif_time:
        days_since = (datetime.utcnow() - last_notif_time).total_seconds() / 86400
        
        if days_since >= 7:
            try:
                payload = {
                    "review_title": "✅ G2 Review Monitor - Weekly Health Check",
                    "review_author": "Automation System",
                    "review_rating": "✅ System Active",
                    "review_date": datetime.utcnow().strftime("%Y-%m-%d"),
                    "review_url": "https://www.g2.com/products/bright-data/reviews",
                    "review_text": f"No new reviews detected in the past {int(days_since)} days.\n\nThe monitoring system is running normally and checking every 6 hours.\n\nLast notification sent: {last_notif_time.strftime('%Y-%m-%d %H:%M UTC')}"
                }
                
                response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
                response.raise_for_status()
                print(f"✅ Weekly health check sent ({int(days_since)} days since last review)")
            except Exception as e:
                print(f"⚠️ Failed to send health check: {e}")

def main():
    print("=" * 60)
    print("🔍 G2 Review Monitor - Starting Check")
    print(f"⏰ Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 60)
    
    # Validate secrets
    if not validate_secrets():
        print("\n❌ Cannot proceed without required secrets")
        return
    
    # Check if we should run (avoid duplicates)
    if not should_run_check():
        print("\n✅ Check skipped to avoid duplicate runs")
        return
    
    try:
        # Scrape reviews
        reviews_data = scrape_g2_reviews()
        
        if not reviews_data or len(reviews_data) == 0:
            error_msg = "Failed to retrieve data from Bright Data API. The API may be down or rate-limited."
            print(f"\n⚠️ {error_msg}")
            send_error_notification(error_msg)
            return
        
        last_stored_id = load_last_review_id()
        print(f"\n💾 Last stored review ID: {last_stored_id}")
        print(f"📊 Total reviews fetched: {len(reviews_data)}")
        
        # Find all new reviews
        new_reviews = []
        for review in reviews_data:
            if review['review_id'] > last_stored_id and is_review_recent(review):
                new_reviews.append(review)
        
        if len(new_reviews) == 0:
            print("\n✨ No new reviews - all caught up!")
            
            # Send weekly health check if needed
            send_health_check()
            
            # Update last checked time
            if reviews_data:
                latest_review_id = max(review['review_id'] for review in reviews_data)
                save_last_review_id(latest_review_id)
            
            print("\n" + "=" * 60)
            print("✅ Check complete")
            print("=" * 60)
            return
        
        # Sort new reviews by ID (oldest first)
        new_reviews.sort(key=lambda x: x['review_id'])
        
        print(f"\n🆕 FOUND {len(new_reviews)} NEW REVIEW(S)!")
        print("=" * 60)
        
        # Send notification for each new review
        successful_notifications = 0
        failed_reviews = []
        
        for i, review in enumerate(new_reviews, 1):
            print(f"\n[{i}/{len(new_reviews)}] Processing Review ID: {review['review_id']}")
            print(f"  📝 Title: {review['title']}")
            print(f"  👤 Author: {review['author']}")
            print(f"  ⭐ Rating: {review['stars']}/5")
            print(f"  📅 Date: {review['date']}")
            
            if send_slack_notification(review):
                successful_notifications += 1
                # Small delay between notifications
                if i < len(new_reviews):
                    time.sleep(2)
            else:
                failed_reviews.append(review['review_id'])
                print(f"  ❌ Failed to send notification")
        
        print(f"\n{'=' * 60}")
        print(f"✅ Successfully sent {successful_notifications}/{len(new_reviews)} notifications")
        
        if failed_reviews:
            print(f"⚠️ Failed review IDs: {failed_reviews}")
            error_msg = f"Failed to send {len(failed_reviews)} notification(s) for review IDs: {failed_reviews}"
            send_error_notification(error_msg)
        
        # Update stored ID to the most recent review
        latest_review_id = max(review['review_id'] for review in reviews_data)
        save_last_review_id(latest_review_id)
        print(f"💾 State updated - Latest review ID: {latest_review_id}")
        
        print("\n" + "=" * 60)
        print("✅ Check complete")
        print("=" * 60)
        
    except Exception as e:
        error_msg = f"Unexpected error in main execution: {str(e)}"
        print(f"\n❌ {error_msg}")
        send_error_notification(error_msg)
        import traceback
        traceback.print_exc()
        print("\n" + "=" * 60)
        print("❌ Check failed")
        print("=" * 60)

if __name__ == '__main__':
    main()
